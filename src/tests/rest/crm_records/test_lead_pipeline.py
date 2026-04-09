"""
Tests for the lead pipeline engine (crm_records.lead_pipeline).

- **Unit (no DB):** CandidateSelector.is_due_for_call
- **Unit (DB):** DailyLimitChecker, PullStrategyApplier ordering
- **Integration:** LeadPipeline.get_next with tenant buckets + UserSettings

Business semantics (aligned with production data + code):
- ``UserSettings.value`` (LEAD_TYPE_ASSIGNMENT) → party **affiliated_party**, not ``data.lead_type``.
- ``lead_sources`` / ``lead_statuses`` filter ``data.lead_source`` / ``data.lead_status`` (real CRM
  values use e.g. `PREMIUM_REFERRAL` for source and ``SALES LEAD`` for status).
- **Bucket priority** (tenant-wide): follow-up (snoozed) → **fresh** → not-connected retry.
  So when both a fresh **and** a due NOT_CONNECTED retry exist, **fresh is tried first** and wins.
- **Pull tiebreaker:** ``tiebreaker`` is ``asc`` or ``desc`` on ``tiebreaker_field`` (default ``desc`` if omitted).
  Legacy ``lifo`` / ``fifo`` are accepted as aliases for ``desc`` / ``asc``. ``tiebreaker_field``: ``created_at`` or ``updated_at`` (default ``created_at``).
  Secondary sort is the other timestamp descending, then ``id``.

Run (venv activated):

  cd pyro-backend && pytest src/tests/rest/crm_records/test_lead_pipeline.py -v

From repo root ``Pyro/``:

  pytest pyro-backend/src/tests/rest/crm_records/test_lead_pipeline.py -v
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone as std_utc
from types import SimpleNamespace
import pytest
from django.core.cache import cache
from django.utils import timezone

from crm_records.lead_pipeline.candidate_selector import CandidateSelector
from crm_records.lead_pipeline.daily_limit import DailyLimitChecker
from crm_records.lead_pipeline.pipeline import LeadPipeline
from crm_records.lead_pipeline.pull_strategy import PullStrategyApplier
from crm_records.models import Bucket, Record, UserBucketAssignment
from user_settings.models import RoutingRule, UserSettings

from tests.factories import (
    RecordFactory,
    RoleFactory,
    SupabaseAuthUserFactory,
    TenantFactory,
    TenantMembershipFactory,
    UserFactory,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _local_start_of_day(now):
    if timezone.is_aware(now):
        return timezone.localtime(now).replace(hour=0, minute=0, second=0, microsecond=0)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def _make_rm_user(
    tenant,
    *,
    eligible_parties: list | None = None,
    lead_sources: list | None = None,
    lead_statuses: list | None = None,
    daily_limit: int | None = None,
):
    """RM user + membership + LEAD_TYPE_ASSIGNMENT (realistic field names)."""
    uid = str(uuid.uuid4())
    user = UserFactory(
        supabase_uid=uid,
        email=f"{uid[:8]}@pipeline.test",
        tenant_id=str(tenant.id),
    )
    SupabaseAuthUserFactory(id=uuid.UUID(uid), email=user.email)
    role = RoleFactory(tenant=tenant, key="pyro_admin", name="Pyro Admin")
    membership = TenantMembershipFactory(
        tenant=tenant,
        user_id=uid,
        email=user.email,
        role=role,
    )
    UserSettings.objects.create(
        tenant=tenant,
        tenant_membership=membership,
        key="LEAD_TYPE_ASSIGNMENT",
        value=eligible_parties if eligible_parties is not None else [],
        lead_sources=lead_sources if lead_sources is not None else [],
        lead_statuses=lead_statuses if lead_statuses is not None else ["SALES LEAD"],
        daily_limit=daily_limit,
    )
    return user, membership, uid


def _seed_tenant_buckets(tenant) -> dict:
    """
    Production-like order: follow-up → fresh → not-connected retry.

    ``call_attempts`` on NOT_CONNECTED uses **lt: 6** (not lte: 6) so attempts == 6 are excluded.
    """
    cache.clear()

    followup = Bucket.objects.create(
        tenant=tenant,
        name="Followup Callback",
        slug="followup_callback",
        filter_conditions={
            "lead_stage": ["SNOOZED", "IN_QUEUE"],
            "call_attempts": {"lt": 6},
            "next_call_due": True,
            "assigned_scope": "me",
            "apply_routing_rule": True,
            "fallback_assigned_scope": "unassigned",
        },
    )
    fresh = Bucket.objects.create(
        tenant=tenant,
        name="Fresh Leads",
        slug="fresh_leads",
        filter_conditions={
            "lead_stage": ["FRESH", "IN_QUEUE"],
            "call_attempts": {"lte": 0},
            "next_call_due": False,
            "assigned_scope": "unassigned",
            "apply_routing_rule": True,
            "daily_limit_applies": True,
        },
    )
    not_connected = Bucket.objects.create(
        tenant=tenant,
        name="Not Connected Retry",
        slug="not_connected_retry",
        filter_conditions={
            "lead_stage": ["NOT_CONNECTED", "IN_QUEUE"],
            "call_attempts": {"lt": 6, "gte": 1},
            "next_call_due": True,
            "assigned_scope": "me",
            "apply_routing_rule": True,
            "fallback_assigned_scope": "unassigned",
        },
    )

    strategy_snoozed = {
        "order_by": "score_desc",
        "tiebreaker": "desc",
        "tiebreaker_field": "created_at",
        "include_snoozed_due": True,
        "ignore_score_for_sources": [],
    }
    strategy_plain = {
        "order_by": "score_desc",
        "tiebreaker": "desc",
        "tiebreaker_field": "created_at",
        "include_snoozed_due": False,
        "ignore_score_for_sources": [],
    }

    UserBucketAssignment.objects.create(
        tenant=tenant,
        user=None,
        bucket=followup,
        priority=1,
        pull_strategy=strategy_snoozed,
    )
    UserBucketAssignment.objects.create(
        tenant=tenant,
        user=None,
        bucket=fresh,
        priority=2,
        pull_strategy=strategy_snoozed,
    )
    UserBucketAssignment.objects.create(
        tenant=tenant,
        user=None,
        bucket=not_connected,
        priority=3,
        pull_strategy=strategy_plain,
    )
    return {"followup": followup, "fresh": fresh, "not_connected": not_connected}


def _sales_lead_row(**kwargs):
    """Typical SALES LEAD row shape."""
    base = {
        "lead_status": "SALES LEAD",
        "lead_source": "PREMIUM_REFERRAL",
        "affiliated_party": "Telugu Desam Party",
        "call_attempts": 0,
    }
    base.update(kwargs)
    return base


# ===================================================================
# UNIT — CandidateSelector (no DB)
# ===================================================================


@pytest.mark.parametrize(
    "data,expected_due",
    [
        ({"call_attempts": 0}, True),
        ({}, True),
        (None, True),
        ({"call_attempts": 2, "next_call_at": None}, False),
        ({"call_attempts": 1, "next_call_at": ""}, False),
        ({"call_attempts": 1, "next_call_at": "null"}, False),
    ],
)
def test_candidate_selector_is_due_for_call(data, expected_due):
    selector = CandidateSelector()
    now = datetime(2026, 3, 22, 12, 0, 0, tzinfo=std_utc.utc)
    assert selector.is_due_for_call(data, now) is expected_due


def test_candidate_selector_retry_past_due():
    selector = CandidateSelector()
    now = datetime(2026, 3, 22, 12, 0, 0, tzinfo=std_utc.utc)
    past = (now - timedelta(hours=1)).isoformat()
    assert selector.is_due_for_call({"call_attempts": 2, "next_call_at": past}, now) is True


def test_candidate_selector_retry_future_not_due():
    selector = CandidateSelector()
    now = datetime(2026, 3, 22, 12, 0, 0, tzinfo=std_utc.utc)
    future = (now + timedelta(hours=2)).isoformat()
    assert selector.is_due_for_call({"call_attempts": 2, "next_call_at": future}, now) is False


def test_candidate_selector_retry_exactly_now_due():
    selector = CandidateSelector()
    now = datetime(2026, 3, 22, 12, 0, 0, tzinfo=std_utc.utc)
    assert selector.is_due_for_call({"call_attempts": 1, "next_call_at": now.isoformat()}, now) is True


def test_candidate_selector_garbage_next_call_at_not_due():
    selector = CandidateSelector()
    now = datetime(2026, 3, 22, 12, 0, 0, tzinfo=std_utc.utc)
    assert selector.is_due_for_call({"call_attempts": 1, "next_call_at": "not-a-date"}, now) is False


# ===================================================================
# UNIT — DailyLimitChecker (DB)
# ===================================================================


@pytest.mark.django_db
def test_daily_limit_checker_none_limit_never_reached():
    tenant = TenantFactory()
    checker = DailyLimitChecker()
    status = checker.check(
        tenant=tenant,
        user_identifier="u1",
        daily_limit=None,
        now=timezone.now(),
        debug=False,
    )
    assert status.is_reached is False
    assert status.assigned_today == 0


@pytest.mark.django_db
def test_daily_limit_checker_negative_limit_not_reached():
    tenant = TenantFactory()
    checker = DailyLimitChecker()
    status = checker.check(
        tenant=tenant,
        user_identifier="u1",
        daily_limit=-1,
        now=timezone.now(),
        debug=False,
    )
    assert status.is_reached is False


@pytest.mark.django_db
def test_daily_limit_checker_zero_reaches_immediately():
    """daily_limit=0 → is_reached True (no fresh pulls allowed when bucket is gated)."""
    tenant = TenantFactory()
    checker = DailyLimitChecker()
    status = checker.check(
        tenant=tenant,
        user_identifier="u1",
        daily_limit=0,
        now=timezone.now(),
        debug=False,
    )
    assert status.is_reached is True


@pytest.mark.django_db
def test_daily_limit_checker_counts_first_assigned_today():
    tenant = TenantFactory()
    uid = "user-1"
    now = timezone.now()
    sod = _local_start_of_day(now)
    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data={
            "first_assigned_to": uid,
            "first_assigned_at": now.isoformat(),
            "lead_stage": "ASSIGNED",
            "lead_status": "SALES LEAD",
        },
    )
    checker = DailyLimitChecker()
    status = checker.check(
        tenant=tenant,
        user_identifier=uid,
        daily_limit=1,
        now=now,
        debug=False,
    )
    assert status.assigned_today >= 1
    assert status.is_reached is True


@pytest.mark.django_db
def test_daily_limit_debug_mode_never_reached():
    tenant = TenantFactory()
    checker = DailyLimitChecker()
    status = checker.check(
        tenant=tenant,
        user_identifier="u1",
        daily_limit=0,
        now=timezone.now(),
        debug=True,
    )
    assert status.is_reached is False


# ===================================================================
# UNIT — PullStrategyApplier ordering (DB)
# ===================================================================


@pytest.mark.django_db
def test_pull_strategy_expired_snoozed_before_fresh_in_sort_order():
    """With include_snoozed_due, due SNOOZED rows sort before non-snoozed (is_expired_snoozed=0 first)."""
    tenant = TenantFactory()
    now = timezone.now()
    past = (now - timedelta(hours=1)).isoformat()

    fresh = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Fresh",
            lead_stage="IN_QUEUE",
            call_attempts=0,
            lead_score=999,
        ),
    )
    snoozed = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Snoozed due",
            lead_stage="SNOOZED",
            call_attempts=1,
            next_call_at=past,
            lead_score=1,
        ),
    )

    qs = Record.objects.filter(tenant=tenant, entity_type="lead", id__in=[fresh.id, snoozed.id])
    applier = PullStrategyApplier()
    ordered = applier.apply(
        qs=qs,
        strategy={
            "order_by": "score_desc",
            "include_snoozed_due": True,
            "ignore_score_for_sources": [],
            "tiebreaker": "desc",
        },
        now_iso=now.isoformat(),
    )
    first = ordered.first()
    assert first is not None
    assert first.id == snoozed.id


@pytest.mark.django_db
def test_pull_strategy_asc_tiebreaker_older_created_at_wins():
    """Same score and attempts: asc tiebreaker prefers smaller created_at (oldest created first)."""
    tenant = TenantFactory()
    now = timezone.now()

    older = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Older", lead_stage="IN_QUEUE", lead_score=50),
    )
    newer = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Newer", lead_stage="IN_QUEUE", lead_score=50),
    )
    Record.objects.filter(pk=older.pk).update(created_at=now - timedelta(hours=2))
    Record.objects.filter(pk=newer.pk).update(created_at=now)

    qs = Record.objects.filter(tenant=tenant, entity_type="lead", id__in=[older.id, newer.id])
    applier = PullStrategyApplier()
    ordered = applier.apply(
        qs=qs,
        strategy={
            "order_by": "score_desc",
            "include_snoozed_due": False,
            "ignore_score_for_sources": [],
            "tiebreaker": "asc",
        },
        now_iso=now.isoformat(),
    )
    first = ordered.first()
    assert first is not None
    assert first.id == older.id


@pytest.mark.django_db
def test_pull_strategy_desc_tiebreaker_newer_created_at_wins():
    """Same score and attempts: desc tiebreaker prefers larger created_at (newest created first)."""
    tenant = TenantFactory()
    now = timezone.now()

    older = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Older", lead_stage="IN_QUEUE", lead_score=50),
    )
    newer = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Newer", lead_stage="IN_QUEUE", lead_score=50),
    )
    Record.objects.filter(pk=older.pk).update(created_at=now - timedelta(hours=2))
    Record.objects.filter(pk=newer.pk).update(created_at=now)

    qs = Record.objects.filter(tenant=tenant, entity_type="lead", id__in=[older.id, newer.id])
    applier = PullStrategyApplier()
    ordered = applier.apply(
        qs=qs,
        strategy={
            "order_by": "score_desc",
            "include_snoozed_due": False,
            "ignore_score_for_sources": [],
            "tiebreaker": "desc",
        },
        now_iso=now.isoformat(),
    )
    first = ordered.first()
    assert first is not None
    assert first.id == newer.id


@pytest.mark.django_db
def test_pull_strategy_desc_uses_created_at_not_updated_at_for_ties():
    """Larger created_at wins even when that row has smaller updated_at (secondary sort is updated_at)."""
    tenant = TenantFactory()
    now = timezone.now()

    older_created = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="OldCreate", lead_stage="IN_QUEUE", lead_score=50),
    )
    newer_created = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="NewCreate", lead_stage="IN_QUEUE", lead_score=50),
    )
    # Older row was touched recently; newer row has stale updated_at.
    Record.objects.filter(pk=older_created.pk).update(
        created_at=now - timedelta(hours=3),
        updated_at=now,
    )
    Record.objects.filter(pk=newer_created.pk).update(
        created_at=now - timedelta(hours=1),
        updated_at=now - timedelta(hours=2),
    )

    qs = Record.objects.filter(
        tenant=tenant, entity_type="lead", id__in=[older_created.id, newer_created.id]
    )
    applier = PullStrategyApplier()
    ordered = applier.apply(
        qs=qs,
        strategy={
            "order_by": "score_desc",
            "include_snoozed_due": False,
            "ignore_score_for_sources": [],
            "tiebreaker": "desc",
        },
        now_iso=now.isoformat(),
    )
    first = ordered.first()
    assert first is not None
    assert first.id == newer_created.id


@pytest.mark.django_db
def test_pull_strategy_default_tiebreaker_desc_on_created_at():
    """Omitted tiebreaker defaults to desc (newest created_at first)."""
    tenant = TenantFactory()
    now = timezone.now()

    older = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Older", lead_stage="IN_QUEUE", lead_score=50),
    )
    newer = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Newer", lead_stage="IN_QUEUE", lead_score=50),
    )
    Record.objects.filter(pk=older.pk).update(created_at=now - timedelta(hours=2))
    Record.objects.filter(pk=newer.pk).update(created_at=now)

    qs = Record.objects.filter(tenant=tenant, entity_type="lead", id__in=[older.id, newer.id])
    applier = PullStrategyApplier()
    ordered = applier.apply(
        qs=qs,
        strategy={
            "order_by": "score_desc",
            "include_snoozed_due": False,
            "ignore_score_for_sources": [],
        },
        now_iso=now.isoformat(),
    )
    assert ordered.first().id == newer.id


@pytest.mark.django_db
def test_pull_strategy_legacy_lifo_fifo_aliases():
    """Stored JSON may still use lifo/fifo; they map to desc/asc."""
    tenant = TenantFactory()
    now = timezone.now()

    older = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Older", lead_stage="IN_QUEUE", lead_score=50),
    )
    newer = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Newer", lead_stage="IN_QUEUE", lead_score=50),
    )
    Record.objects.filter(pk=older.pk).update(created_at=now - timedelta(hours=2))
    Record.objects.filter(pk=newer.pk).update(created_at=now)

    qs = Record.objects.filter(tenant=tenant, entity_type="lead", id__in=[older.id, newer.id])
    applier = PullStrategyApplier()
    desc_like = applier.apply(
        qs=qs,
        strategy={
            "order_by": "score_desc",
            "include_snoozed_due": False,
            "ignore_score_for_sources": [],
            "tiebreaker": "lifo",
            "tiebreaker_field": "created_at",
        },
        now_iso=now.isoformat(),
    )
    assert desc_like.first().id == newer.id

    asc_like = applier.apply(
        qs=qs,
        strategy={
            "order_by": "score_desc",
            "include_snoozed_due": False,
            "ignore_score_for_sources": [],
            "tiebreaker": "fifo",
            "tiebreaker_field": "created_at",
        },
        now_iso=now.isoformat(),
    )
    assert asc_like.first().id == older.id


@pytest.mark.django_db
def test_pull_strategy_desc_on_updated_at_when_tiebreaker_field_set():
    """``tiebreaker_field: updated_at`` + ``desc`` — newer ``updated_at`` wins among same score (same ``created_at``)."""
    tenant = TenantFactory()
    now = timezone.now()
    same_created = now - timedelta(days=1)

    older_touch = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="StaleTouch", lead_stage="IN_QUEUE", lead_score=50),
    )
    newer_touch = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="FreshTouch", lead_stage="IN_QUEUE", lead_score=50),
    )
    Record.objects.filter(pk=older_touch.pk).update(
        created_at=same_created,
        updated_at=now - timedelta(hours=2),
    )
    Record.objects.filter(pk=newer_touch.pk).update(
        created_at=same_created,
        updated_at=now,
    )

    qs = Record.objects.filter(
        tenant=tenant, entity_type="lead", id__in=[older_touch.id, newer_touch.id]
    )
    applier = PullStrategyApplier()
    ordered = applier.apply(
        qs=qs,
        strategy={
            "order_by": "score_desc",
            "include_snoozed_due": False,
            "ignore_score_for_sources": [],
            "tiebreaker": "desc",
            "tiebreaker_field": "updated_at",
        },
        now_iso=now.isoformat(),
    )
    assert ordered.first().id == newer_touch.id


@pytest.mark.django_db
def test_pull_strategy_unknown_tiebreaker_field_defaults_to_created_at():
    """Invalid ``tiebreaker_field`` values are ignored; ordering matches ``created_at`` tiebreak."""
    tenant = TenantFactory()
    now = timezone.now()

    older = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Older", lead_stage="IN_QUEUE", lead_score=50),
    )
    newer = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Newer", lead_stage="IN_QUEUE", lead_score=50),
    )
    Record.objects.filter(pk=older.pk).update(created_at=now - timedelta(hours=2))
    Record.objects.filter(pk=newer.pk).update(created_at=now)
    # Same updated_at so secondary does not flip order.
    Record.objects.filter(pk__in=[older.pk, newer.pk]).update(updated_at=now - timedelta(hours=5))

    qs = Record.objects.filter(tenant=tenant, entity_type="lead", id__in=[older.id, newer.id])
    applier = PullStrategyApplier()
    ordered = applier.apply(
        qs=qs,
        strategy={
            "order_by": "score_desc",
            "include_snoozed_due": False,
            "ignore_score_for_sources": [],
            "tiebreaker": "desc",
            "tiebreaker_field": "not_a_column",
        },
        now_iso=now.isoformat(),
    )
    assert ordered.first().id == newer.id


# ===================================================================
# INTEGRATION — LeadPipeline
# ===================================================================


@pytest.mark.django_db
def test_pipeline_no_user_identifier_returns_none():
    pipeline = LeadPipeline()
    user = SimpleNamespace(supabase_uid=None, email=None)
    assert pipeline.get_next(tenant=TenantFactory(), request_user=user) is None


@pytest.mark.django_db
def test_pipeline_no_tenant_membership_returns_none():
    """BucketResolver resolves no rows without membership → no assignments → no lead."""
    tenant = TenantFactory()
    uid = str(uuid.uuid4())
    user = UserFactory(
        supabase_uid=uid,
        email="orphan@pipeline.test",
        tenant_id=str(tenant.id),
    )
    SupabaseAuthUserFactory(id=uuid.UUID(uid), email=user.email)
    _seed_tenant_buckets(tenant)

    pipeline = LeadPipeline()
    assert pipeline.get_next(tenant=tenant, request_user=user) is None


@pytest.mark.django_db
def test_pipeline_filters_affiliated_party_party_types():
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(
        tenant,
        eligible_parties=["BJP", "AAP"],
        lead_sources=[],
        lead_statuses=["SALES LEAD"],
    )

    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Congress",
            lead_stage="IN_QUEUE",
            affiliated_party="Congress",
            lead_score=999,
        ),
    )
    match = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="BJP",
            lead_stage="IN_QUEUE",
            affiliated_party="BJP",
            lead_score=1,
        ),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == match.pk


@pytest.mark.django_db
def test_pipeline_no_party_filter_when_empty_value_list():
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, _ = _make_rm_user(
        tenant,
        eligible_parties=[],
        lead_sources=[],
        lead_statuses=["SALES LEAD"],
    )
    lead = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Any",
            lead_stage="IN_QUEUE",
            affiliated_party="TDP",
        ),
    )
    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == lead.pk


@pytest.mark.django_db
def test_pipeline_filters_lead_source_crm_values_not_lead_status():
    """
    Eligible sources apply to ``data.lead_source`` (e.g. PREMIUM_REFERRAL), not ``lead_status``.
    """
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, _ = _make_rm_user(
        tenant,
        eligible_parties=[],
        lead_sources=["PREMIUM_REFERRAL"],
        lead_statuses=["SALES LEAD"],
    )
    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Wrong source",
            lead_stage="IN_QUEUE",
            lead_source="OTHER_SOURCE",
            lead_score=500,
        ),
    )
    good = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Right source",
            lead_stage="IN_QUEUE",
            lead_source="PREMIUM_REFERRAL",
            lead_score=1,
        ),
    )
    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == good.pk


@pytest.mark.django_db
def test_pipeline_filters_lead_status():
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, _ = _make_rm_user(
        tenant,
        eligible_parties=[],
        lead_sources=[],
        lead_statuses=["SALES LEAD"],
    )
    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Self trial",
            lead_stage="IN_QUEUE",
            lead_status="SELF TRIAL",
            lead_source="PREMIUM_REFERRAL",
        ),
    )
    sales = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Sales",
            lead_stage="IN_QUEUE",
            lead_status="SALES LEAD",
            lead_source="PREMIUM_REFERRAL",
        ),
    )
    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == sales.pk


@pytest.mark.django_db
def test_pipeline_fresh_bucket_returns_higher_score_first():
    """Within fresh bucket, higher lead_score wins (PullStrategyApplier score_desc)."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, _ = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])

    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Low", lead_stage="IN_QUEUE", lead_score=100),
    )
    high = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="High", lead_stage="IN_QUEUE", lead_score=500),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == high.pk


@pytest.mark.django_db
def test_pipeline_desc_tiebreaker_newer_created_at_wins():
    """Same score: newer created_at wins (desc on created_at)."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, _ = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    now = timezone.now()

    older = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Older", lead_stage="IN_QUEUE", lead_score=100),
    )
    newer = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Newer", lead_stage="IN_QUEUE", lead_score=100),
    )
    Record.objects.filter(pk=older.pk).update(created_at=now - timedelta(hours=2))
    Record.objects.filter(pk=newer.pk).update(created_at=now)

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == newer.pk


@pytest.mark.django_db
def test_pipeline_desc_on_created_at_not_latest_updated_at():
    """End-to-end: tiebreak is created_at desc, not which row was updated most recently."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, _ = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    now = timezone.now()

    older_created = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="OldCreate", lead_stage="IN_QUEUE", lead_score=100),
    )
    newer_created = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="NewCreate", lead_stage="IN_QUEUE", lead_score=100),
    )
    Record.objects.filter(pk=older_created.pk).update(
        created_at=now - timedelta(hours=3),
        updated_at=now,
    )
    Record.objects.filter(pk=newer_created.pk).update(
        created_at=now - timedelta(hours=1),
        updated_at=now - timedelta(hours=2),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == newer_created.pk


@pytest.mark.django_db
def test_pipeline_nc_retry_lower_attempts_first():
    """NC retry bucket: fewer call_attempts first (ascending)."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    past = (timezone.now() - timedelta(hours=1)).isoformat()

    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="4 attempts",
            lead_stage="NOT_CONNECTED",
            assigned_to=uid,
            call_attempts=4,
            next_call_at=past,
            lead_score=999,
        ),
    )
    low = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="1 attempt",
            lead_stage="NOT_CONNECTED",
            assigned_to=uid,
            call_attempts=1,
            next_call_at=past,
            lead_score=1,
        ),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == low.pk


@pytest.mark.django_db
def test_pipeline_closed_lead_excluded_from_all_buckets():
    """Terminal CLOSED does not match any bucket lead_stage list."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])

    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Closed", lead_stage="CLOSED", assigned_to=uid, call_attempts=5),
    )

    pipeline = LeadPipeline()
    assert pipeline.get_next(tenant=tenant, request_user=user) is None


@pytest.mark.django_db
def test_pipeline_fresh_stage_literal_matches_fresh_bucket():
    """lead_stage FRESH matches fresh bucket (not only IN_QUEUE)."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, _ = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])

    lead = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Fresh stage", lead_stage="FRESH", call_attempts=0),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == lead.pk


@pytest.mark.django_db
def test_pipeline_snoozed_future_callback_skipped_fresh_returned():
    """SNOOZED with future next_call_at skipped by follow-up bucket; fresh bucket wins."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    future = (timezone.now() + timedelta(hours=2)).isoformat()

    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Not yet",
            lead_stage="SNOOZED",
            assigned_to=uid,
            call_attempts=1,
            next_call_at=future,
        ),
    )
    fresh = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Fresh", lead_stage="IN_QUEUE", call_attempts=0),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == fresh.pk


@pytest.mark.django_db
def test_pipeline_fresh_bucket_wins_before_not_connected_retry_when_both_exist():
    """
    Bucket order is follow-up → **fresh** → not-connected. With a due NOT_CONNECTED retry
    and an unassigned fresh lead, **fresh is assigned first** (business: new pool before retry queue).
    """
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    now = timezone.now()
    past = (now - timedelta(hours=1)).isoformat()

    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="NC retry",
            lead_stage="NOT_CONNECTED",
            assigned_to=uid,
            call_attempts=2,
            next_call_at=past,
            last_call_outcome="not_connected",
        ),
    )
    fresh = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Fresh first",
            lead_stage="IN_QUEUE",
            call_attempts=0,
        ),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == fresh.pk


@pytest.mark.django_db
def test_pipeline_snoozed_due_before_fresh_and_not_connected():
    """Follow-up bucket (priority 1) runs before fresh and NC retry."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    now = timezone.now()
    past = (now - timedelta(hours=1)).isoformat()

    snoozed = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Snoozed",
            lead_stage="SNOOZED",
            assigned_to=uid,
            call_attempts=1,
            next_call_at=past,
        ),
    )
    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Fresh",
            lead_stage="IN_QUEUE",
            call_attempts=0,
            lead_score=999,
        ),
    )
    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="NC",
            lead_stage="NOT_CONNECTED",
            assigned_to=uid,
            call_attempts=2,
            next_call_at=past,
        ),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == snoozed.pk


@pytest.mark.django_db
def test_pipeline_followup_fallback_unassigned_snoozed_when_not_assigned_to_me():
    """followup_callback tries ``me`` then ``unassigned`` for unassigned due snoozed leads."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    now = timezone.now()
    past = (now - timedelta(hours=1)).isoformat()

    unassigned = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Unassigned snoozed",
            lead_stage="SNOOZED",
            assigned_to=None,
            call_attempts=1,
            next_call_at=past,
        ),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == unassigned.pk
    result.refresh_from_db()
    assert result.data.get("assigned_to") == uid
    assert result.data.get("lead_stage") == "ASSIGNED"


@pytest.mark.django_db
def test_pipeline_fresh_unassigned_excludes_lead_assigned_to_other_user():
    """Unassigned scope excludes rows assigned to another RM."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    other = str(uuid.uuid4())

    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Owned by other",
            lead_stage="IN_QUEUE",
            call_attempts=0,
            assigned_to=other,
        ),
    )
    mine = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Unassigned pool",
            lead_stage="IN_QUEUE",
            call_attempts=0,
        ),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == mine.pk


@pytest.mark.django_db
def test_pipeline_daily_limit_skips_fresh_returns_none_without_retry():
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    now = timezone.now()
    user, _, uid = _make_rm_user(
        tenant,
        lead_sources=[],
        lead_statuses=["SALES LEAD"],
        daily_limit=1,
    )

    Record.objects.create(
        tenant=tenant,
        entity_type="lead",
        data={
            "lead_status": "SALES LEAD",
            "lead_source": "PREMIUM_REFERRAL",
            "first_assigned_to": uid,
            "first_assigned_at": now.isoformat(),
            "lead_stage": "ASSIGNED",
            "call_attempts": 0,
        },
    )
    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Fresh only",
            lead_stage="IN_QUEUE",
            call_attempts=0,
        ),
    )

    pipeline = LeadPipeline()
    assert pipeline.get_next(tenant=tenant, request_user=user) is None


@pytest.mark.django_db
def test_pipeline_not_connected_assignable_when_fresh_bucket_skipped_for_daily_limit():
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    now = timezone.now()
    user, _, uid = _make_rm_user(
        tenant,
        lead_sources=[],
        lead_statuses=["SALES LEAD"],
        daily_limit=1,
    )

    Record.objects.create(
        tenant=tenant,
        entity_type="lead",
        data={
            "lead_status": "SALES LEAD",
            "lead_source": "PREMIUM_REFERRAL",
            "first_assigned_to": uid,
            "first_assigned_at": now.isoformat(),
            "lead_stage": "ASSIGNED",
            "call_attempts": 0,
        },
    )
    retry = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="My retry",
            lead_stage="NOT_CONNECTED",
            assigned_to=uid,
            call_attempts=2,
            next_call_at=(now - timedelta(hours=1)).isoformat(),
            last_call_outcome="not_connected",
        ),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == retry.pk


@pytest.mark.django_db
def test_pipeline_call_attempts_six_excluded_from_buckets():
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    now = timezone.now()
    past = (now - timedelta(hours=1)).isoformat()

    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Exhausted",
            lead_stage="NOT_CONNECTED",
            assigned_to=uid,
            call_attempts=6,
            next_call_at=past,
        ),
    )

    pipeline = LeadPipeline()
    assert pipeline.get_next(tenant=tenant, request_user=user) is None


@pytest.mark.django_db
def test_pipeline_tenant_isolation():
    tenant_a = TenantFactory()
    tenant_b = TenantFactory()
    _seed_tenant_buckets(tenant_a)
    user, _, _ = _make_rm_user(tenant_a, lead_sources=[], lead_statuses=["SALES LEAD"])

    RecordFactory(
        tenant=tenant_b,
        entity_type="lead",
        data=_sales_lead_row(name="Other tenant", lead_stage="IN_QUEUE", call_attempts=0),
    )

    pipeline = LeadPipeline()
    assert pipeline.get_next(tenant=tenant_a, request_user=user) is None


@pytest.mark.django_db
def test_pipeline_routing_rule_filters_state():
    """Active lead routing rule narrows the pool (e.g. state)."""
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, membership, _ = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])

    RoutingRule.objects.create(
        tenant=tenant,
        tenant_membership=membership,
        user_id=membership.user_id,
        queue_type=RoutingRule.QUEUE_TYPE_LEAD,
        is_active=True,
        conditions={
            "filters": [
                {"field": "state", "op": "equals", "value": "Andhra Pradesh"},
            ]
        },
    )

    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Tamil Nadu lead",
            lead_stage="IN_QUEUE",
            state="Tamil Nadu",
            call_attempts=0,
        ),
    )
    ap = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="AP lead",
            lead_stage="IN_QUEUE",
            state="Andhra Pradesh",
            call_attempts=0,
        ),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    assert result.pk == ap.pk


@pytest.mark.django_db
def test_pipeline_fresh_assignment_sets_first_assigned_and_today_anchor():
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])

    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="Brand new",
            lead_stage="IN_QUEUE",
            call_attempts=0,
        ),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=tenant, request_user=user)
    assert result is not None
    result.refresh_from_db()
    assert result.data.get("first_assigned_to") == uid
    assert result.data.get("first_assigned_at")
    assert result.data.get("first_assigned_today_at")
    assert result.data.get("lead_stage") == "ASSIGNED"


@pytest.mark.django_db
def test_pipeline_retry_does_not_set_first_assigned_tracking():
    """
    NOT_CONNECTED retry already has assigned_to set → LeadAssigner does not set
    first_assigned_at / first_assigned_to (not a fresh pull from pool).
    Only NC + no fresh lead so the retry bucket is what runs after empty follow-up.
    """
    tenant = TenantFactory()
    _seed_tenant_buckets(tenant)
    user, _, uid = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    now = timezone.now()
    past = (now - timedelta(hours=1)).isoformat()

    nc_only = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(
            name="NC solo",
            lead_stage="NOT_CONNECTED",
            assigned_to=uid,
            call_attempts=2,
            next_call_at=past,
            last_call_outcome="not_connected",
        ),
    )

    pipeline = LeadPipeline()
    out = pipeline.get_next(tenant=tenant, request_user=user)
    assert out is not None
    assert out.pk == nc_only.pk
    out.refresh_from_db()
    assert out.data.get("first_assigned_at") is None
    assert out.data.get("first_assigned_to") is None


@pytest.mark.django_db
def test_pipeline_returns_none_without_bucket_assignments():
    tenant = TenantFactory()
    user, _, _ = _make_rm_user(tenant, lead_sources=[], lead_statuses=["SALES LEAD"])
    RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data=_sales_lead_row(name="Orphan", lead_stage="IN_QUEUE", call_attempts=0),
    )
    pipeline = LeadPipeline()
    assert pipeline.get_next(tenant=tenant, request_user=user) is None
