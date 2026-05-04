"""
Tests for GET /crm-records/records/<pk>/history/ (RecordHistoryView).
"""

import uuid
from typing import Optional

from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from authz import service as authz_service
from authz.models import Role, TenantMembership
from django.contrib.auth import get_user_model
from django.core.cache import cache
from rest_framework.test import APIClient

from core.models import Tenant
from crm_records.models import Record
from object_history.models import ObjectHistory

User = get_user_model()


def _history_url(record_id: int) -> str:
    return f"/crm-records/records/{record_id}/history/"


def _create_object_history(
    *,
    record: Record,
    version: int,
    action: str = "updated",
    changes: Optional[dict] = None,
    actor_label: str | None = "tester@example.com",
) -> ObjectHistory:
    ct = ContentType.objects.get_for_model(Record)
    return ObjectHistory.objects.create(
        tenant=record.tenant,
        content_type=ct,
        object_id=str(record.id),
        object_repr=f"Record {record.id}",
        action=action,
        actor_label=actor_label,
        version=version,
        changes=changes if changes is not None else {"status": {"from": "OLD", "to": "NEW"}},
    )


class RecordHistoryApiTests(TestCase):
    def setUp(self):
        authz_service._CACHE.clear()

        self.tenant_a = Tenant.objects.create(
            id=uuid.uuid4(), name="Tenant A", slug=f"tenant-a-{uuid.uuid4().hex[:8]}"
        )
        self.tenant_b = Tenant.objects.create(
            id=uuid.uuid4(), name="Tenant B", slug=f"tenant-b-{uuid.uuid4().hex[:8]}"
        )

        for t in (self.tenant_a, self.tenant_b):
            cache.delete(f"tenant:slug:{t.slug}")
            cache.delete(f"tenant:id:{t.id}")

        self.user_a = User.objects.create_user(
            email="hist-a@x.com", password="pass1234", supabase_uid=str(uuid.uuid4())
        )
        self.user_b = User.objects.create_user(
            email="hist-b@x.com", password="pass1234", supabase_uid=str(uuid.uuid4())
        )

        role_a = Role.objects.create(tenant=self.tenant_a, key="AGENT", name="Agent")
        TenantMembership.objects.create(
            tenant=self.tenant_a,
            user_id=self.user_a.supabase_uid,
            email=self.user_a.email,
            role=role_a,
            is_active=True,
        )

        role_b = Role.objects.create(tenant=self.tenant_b, key="AGENT", name="Agent")
        TenantMembership.objects.create(
            tenant=self.tenant_b,
            user_id=self.user_b.supabase_uid,
            email=self.user_b.email,
            role=role_b,
            is_active=True,
        )

        self.client = APIClient()

        self.rec_a = Record.objects.create(
            tenant=self.tenant_a,
            entity_type="inventory_request",
            data={"name": "Item", "status": "NEW"},
        )
        self.rec_b = Record.objects.create(
            tenant=self.tenant_b,
            entity_type="lead",
            data={"name": "Other tenant lead"},
        )

        # Record uses HistoryTrackedModel — create() writes v1 object_history.
        # Remove it so each test controls history rows and versions explicitly.
        ct = ContentType.objects.get_for_model(Record)
        ObjectHistory.objects.filter(
            content_type=ct,
            object_id__in=[str(self.rec_a.id), str(self.rec_b.id)],
        ).delete()

    def _headers(self, tenant: Tenant):
        return {"HTTP_X_TENANT_SLUG": tenant.slug}

    def test_history_returns_shape_and_order(self):
        _create_object_history(
            record=self.rec_a,
            version=1,
            action="created",
            changes={"data": {"from": None, "to": {"status": "NEW"}}},
        )
        _create_object_history(
            record=self.rec_a,
            version=2,
            action="updated",
            changes={"status": {"from": "NEW", "to": "DONE"}},
        )

        self.client.force_login(self.user_a)
        response = self.client.get(_history_url(self.rec_a.id), **self._headers(self.tenant_a))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["record_id"], self.rec_a.id)
        self.assertIn("limit", response.data)
        self.assertEqual(response.data["limit"], 100)

        history = response.data["history"]
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["version"], 2)
        self.assertEqual(history[0]["action"], "updated")
        self.assertEqual(history[1]["version"], 1)
        self.assertEqual(history[1]["action"], "created")

        for row in history:
            self.assertIn("id", row)
            self.assertIn("created_at", row)
            self.assertIn("changes", row)
            self.assertIn("actor", row)
            actor = row["actor"]
            self.assertIn("email", actor)
            self.assertIn("name", actor)
            self.assertIn("label", actor)
            self.assertNotIn("before_state", row)
            self.assertNotIn("after_state", row)
            self.assertNotIn("metadata", row)

    def test_history_empty_when_no_rows(self):
        self.client.force_login(self.user_a)
        response = self.client.get(_history_url(self.rec_a.id), **self._headers(self.tenant_a))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["history"], [])

    def test_history_limit_query(self):
        for v in range(1, 6):
            _create_object_history(record=self.rec_a, version=v, changes={"v": {"from": None, "to": v}})

        self.client.force_login(self.user_a)
        response = self.client.get(
            _history_url(self.rec_a.id),
            {"limit": "2"},
            **self._headers(self.tenant_a),
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["limit"], 2)
        versions = [h["version"] for h in response.data["history"]]
        self.assertEqual(versions, [5, 4])

    def test_history_invalid_limit_falls_back_to_default(self):
        for v in range(1, 4):
            _create_object_history(record=self.rec_a, version=v)

        self.client.force_login(self.user_a)
        response = self.client.get(
            _history_url(self.rec_a.id),
            {"limit": "not-a-number"},
            **self._headers(self.tenant_a),
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["limit"], 100)
        self.assertEqual(len(response.data["history"]), 3)

    def test_history_max_limit_clamped(self):
        self.client.force_login(self.user_a)
        response = self.client.get(
            _history_url(self.rec_a.id),
            {"limit": "9999"},
            **self._headers(self.tenant_a),
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["limit"], 200)

    def test_history_404_other_tenant_record(self):
        _create_object_history(record=self.rec_a, version=1)

        self.client.force_login(self.user_b)
        response = self.client.get(_history_url(self.rec_a.id), **self._headers(self.tenant_b))
        self.assertEqual(response.status_code, 404)
        self.assertIn("error", response.data)

    def test_history_404_unknown_id(self):
        self.client.force_login(self.user_a)
        response = self.client.get(_history_url(999999999), **self._headers(self.tenant_a))
        self.assertEqual(response.status_code, 404)

    def test_history_does_not_leak_other_record_versions(self):
        _create_object_history(record=self.rec_a, version=1, changes={"k": {"from": None, "to": "a"}})
        _create_object_history(record=self.rec_b, version=1, changes={"k": {"from": None, "to": "b"}})

        self.client.force_login(self.user_a)
        response = self.client.get(_history_url(self.rec_a.id), **self._headers(self.tenant_a))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data["history"]), 1)
        self.assertEqual(response.data["history"][0]["changes"]["k"]["to"], "a")
