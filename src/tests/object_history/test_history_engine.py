from __future__ import annotations

import threading
import uuid

import pytest
from django.contrib.contenttypes.models import ContentType
from django.db import IntegrityError
from django.db.models.query import QuerySet

from core.models import TenantSettings
from crm_records.models import Record
from object_history.engine import HistoryEngine, set_manual_context, clear_request_context
from object_history.models import ObjectHistory
from tests.factories import RecordFactory, SupabaseAuthUserFactory, TenantFactory, UserFactory


@pytest.fixture
def model_instance(db):
    tenant = TenantFactory()
    record = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data={"name": "initial", "phone": "9999999999", "email": "initial@test.com"},
    )
    record_ct = ContentType.objects.get_for_model(Record)
    ObjectHistory.objects.filter(content_type=record_ct, object_id=str(record.pk)).delete()
    return record


@pytest.fixture(autouse=True)
def _clear_history_context():
    clear_request_context()
    yield
    clear_request_context()


@pytest.mark.django_db(transaction=True)
def test_create_first_history(model_instance):
    HistoryEngine.capture_before(model_instance)
    model_instance.data["name"] = "new"
    HistoryEngine.capture_after(model_instance)

    history = ObjectHistory.objects.filter(object_id=str(model_instance.pk)).order_by("version")
    assert history.count() == 1
    assert history.first().version == 1


@pytest.mark.django_db(transaction=True)
def test_version_increments(model_instance):
    for i in range(3):
        HistoryEngine.capture_before(model_instance)
        model_instance.data["name"] = f"v{i}"
        HistoryEngine.capture_after(model_instance)

    versions = list(
        ObjectHistory.objects.filter(object_id=str(model_instance.pk))
        .order_by("version")
        .values_list("version", flat=True)
    )
    assert versions == [1, 2, 3]


@pytest.mark.django_db(transaction=True)
def test_duplicate_event_skipped(model_instance):
    HistoryEngine.capture_before(model_instance)
    model_instance.data["name"] = "same-value"
    HistoryEngine.capture_after(model_instance)

    HistoryEngine.capture_before(model_instance)
    model_instance.data["name"] = "same-value"
    HistoryEngine.capture_after(model_instance)

    assert ObjectHistory.objects.filter(object_id=str(model_instance.pk)).count() == 1


@pytest.mark.django_db(transaction=True)
def test_retry_on_conflict(monkeypatch, model_instance):
    original_create = QuerySet.create
    calls = {"count": 0}

    def flaky_create(self, **kwargs):
        if self.model is ObjectHistory and calls["count"] == 0:
            calls["count"] += 1
            raise IntegrityError("object_hist_unique_version")
        return original_create(self, **kwargs)

    monkeypatch.setattr(QuerySet, "create", flaky_create)

    HistoryEngine.capture_before(model_instance)
    model_instance.data["name"] = "retry"
    HistoryEngine.capture_after(model_instance)

    histories = ObjectHistory.objects.filter(object_id=str(model_instance.pk)).order_by("version")
    assert histories.count() == 1
    assert histories.first().version == 1


@pytest.mark.django_db(transaction=True)
def test_concurrent_writes(model_instance):
    errors = []
    start_gate = threading.Barrier(5)

    def worker(i):
        try:
            obj = Record.objects.get(pk=model_instance.pk)
            start_gate.wait()
            HistoryEngine.capture_before(obj)
            obj.data["name"] = f"worker-{i}"
            HistoryEngine.capture_after(obj)
        except Exception as exc:  # pragma: no cover - test collects full thread failures
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    versions = list(
        ObjectHistory.objects.filter(object_id=str(model_instance.pk))
        .order_by("version")
        .values_list("version", flat=True)
    )
    assert versions == [1, 2, 3, 4, 5]


@pytest.mark.django_db(transaction=True)
def test_parallel_first_insert(model_instance):
    errors = []
    start_gate = threading.Barrier(2)

    def worker(i):
        try:
            obj = Record.objects.get(pk=model_instance.pk)
            start_gate.wait()
            HistoryEngine.capture_before(obj)
            obj.data["name"] = f"first-{i}"
            HistoryEngine.capture_after(obj)
        except Exception as exc:  # pragma: no cover - test collects full thread failures
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    versions = list(
        ObjectHistory.objects.filter(object_id=str(model_instance.pk))
        .order_by("version")
        .values_list("version", flat=True)
    )
    assert versions == [1, 2]


@pytest.mark.django_db(transaction=True)
def test_atomic_rollback(monkeypatch, model_instance):
    original_create = QuerySet.create

    def fail_create(self, **kwargs):
        if self.model is ObjectHistory:
            raise Exception("fail")
        return original_create(self, **kwargs)

    monkeypatch.setattr(QuerySet, "create", fail_create)

    HistoryEngine.capture_before(model_instance)
    model_instance.data["name"] = "fail"
    with pytest.raises(Exception, match="fail"):
        HistoryEngine.capture_after(model_instance)

    assert ObjectHistory.objects.filter(object_id=str(model_instance.pk)).count() == 0


@pytest.mark.django_db(transaction=True)
def test_snapshot_correctness(model_instance):
    old_name = model_instance.data["name"]
    HistoryEngine.capture_before(model_instance)
    model_instance.data["name"] = "new"
    HistoryEngine.capture_after(model_instance)

    hist = ObjectHistory.objects.filter(object_id=str(model_instance.pk)).first()
    assert hist.before_state["name"] == old_name
    assert hist.after_state["name"] == "new"


@pytest.mark.django_db(transaction=True)
def test_actor_metadata(model_instance):
    tenant = model_instance.tenant
    user = UserFactory(tenant_id=str(tenant.id))
    SupabaseAuthUserFactory(id=uuid.UUID(user.supabase_uid), email=user.email)

    set_manual_context(
        actor_user=user,
        actor_label="test-user",
        metadata={"source": "unit-test", "operation": "history-write"},
    )

    HistoryEngine.capture_before(model_instance)
    model_instance.data["name"] = "new"
    HistoryEngine.capture_after(model_instance)

    hist = ObjectHistory.objects.filter(object_id=str(model_instance.pk)).first()
    assert str(hist.actor_user_id) == user.supabase_uid
    assert hist.actor_label == "test-user"
    assert hist.metadata["source"] == "unit-test"
    assert hist.metadata["operation"] == "history-write"


@pytest.mark.django_db(transaction=True)
def test_persistent_history_when_tenant_settings_enabled(model_instance):
    tenant = model_instance.tenant
    TenantSettings.objects.create(tenant=tenant, persistent_object_history=True)

    HistoryEngine.capture_before(model_instance)
    model_instance.data["name"] = "persistent-flag-test"
    HistoryEngine.capture_after(model_instance)

    hist = (
        ObjectHistory.objects.filter(object_id=str(model_instance.pk))
        .order_by("-version")
        .first()
    )
    assert hist is not None
    assert hist.persistent_history is True
