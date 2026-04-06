"""
Lead pipeline tests (SALES LEAD / bucketed LeadPipeline only).

Covers:
- Eligible party (affiliated_party) and lead_source filters from UserSettings
- Daily limit: fresh bucket skipped when limit reached; snoozed follow-up still assignable
- Snoozed due prioritized over fresh (pull_strategy ordering)
- 12h NOT_CONNECTED release clears assignee; once next_call_at is due, pipeline can assign
- Rule engine compute_next_call_from_attempts (fixed minutes) sets next_call_at
- NOT_CONNECTED / retry leads returned only after next_call_at <= now

Run (venv activated):

  cd pyro-backend && pytest src/tests/rest/crm_records/test_lead_pipeline_sales_lead.py -v

From repo root ``Pyro/`` (same paths, uses ``pyro-backend`` prefix):

  pytest pyro-backend/src/tests/rest/crm_records/test_lead_pipeline_sales_lead.py -v
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest
from django.core.cache import cache
from django.utils import timezone

from background_jobs.job_handlers import ReleaseLeadsAfter12hJobHandler
from background_jobs.models import JobType
from crm_records.lead_pipeline.pipeline import LeadPipeline
from crm_records.models import Bucket, Record, UserBucketAssignment
from crm_records.rule_engine import action_compute_next_call_from_attempts
from user_settings.models import UserSettings

from tests.factories import BackgroundJobFactory, RecordFactory, TenantFactory, UserFactory
from tests.factories import RoleFactory, SupabaseAuthUserFactory, TenantMembershipFactory


pytestmark = pytest.mark.django_db


def _uid():
    import uuid

    return str(uuid.uuid4())


def _local_date_iso(dt):
    """Local calendar date string; supports naive datetimes when USE_TZ is False."""
    if timezone.is_aware(dt):
        return timezone.localtime(dt).date().isoformat()
    return dt.date().isoformat()


@dataclass
class SalesLeadEnv:
    tenant: object
    user: object
    membership: object
    user_identifier: str


def _make_sales_lead_user_settings(
    env: SalesLeadEnv,
    *,
    eligible_parties: list | None = None,
    lead_sources: list | None = None,
    daily_limit: int | None = None,
):
    """LEAD_TYPE_ASSIGNMENT for SALES LEAD RMs (lead_statuses filter)."""
    UserSettings.objects.update_or_create(
        tenant=env.tenant,
        tenant_membership=env.membership,
        key="LEAD_TYPE_ASSIGNMENT",
        defaults={
            "value": eligible_parties if eligible_parties is not None else [],
            "lead_sources": lead_sources if lead_sources is not None else [],
            "lead_statuses": ["SALES LEAD"],
            "daily_limit": daily_limit,
        },
    )


def _seed_default_buckets(tenant) -> None:
    """
    Tenant-wide bucket order matching production: follow-up → fresh → not-connected retry.
    """
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
    strategy_no_snooze_boost = {
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
        pull_strategy=strategy_no_snooze_boost,
    )


@pytest.fixture
def sales_lead_env():
    cache.clear()
    tenant = TenantFactory()
    uid = _uid()
    user = UserFactory(
        supabase_uid=uid,
        email="pipeline-test@example.com",
        tenant_id=str(tenant.id),
    )
    import uuid as _uuid

    SupabaseAuthUserFactory(id=_uuid.UUID(uid), email=user.email)
    role = RoleFactory(tenant=tenant, key="pyro_admin", name="Pyro Admin")
    membership = TenantMembershipFactory(
        tenant=tenant,
        user_id=uid,
        email=user.email,
        role=role,
    )
    _seed_default_buckets(tenant)
    env = SalesLeadEnv(tenant=tenant, user=user, membership=membership, user_identifier=uid)
    yield env
    cache.clear()


def _sales_lead_data(**kwargs):
    base = {
        "lead_status": "SALES LEAD",
        "lead_source": "PREMIUM_REFERRAL",
        "affiliated_party": "Telugu Desam Party",
        "call_attempts": 0,
    }
    base.update(kwargs)
    return base


def test_pipeline_filters_by_eligible_affiliated_party_only(sales_lead_env):
    """UserSettings.value (parties) maps to data.affiliated_party; other parties are excluded."""
    env = sales_lead_env
    _make_sales_lead_user_settings(env, eligible_parties=["Telugu Desam Party"], lead_sources=[])

    RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Wrong party",
            affiliated_party="Other Party",
            lead_stage="IN_QUEUE",
            lead_score=999,
        ),
    )
    match = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Matching party",
            affiliated_party="Telugu Desam Party",
            lead_stage="IN_QUEUE",
            lead_score=1,
        ),
    )

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is not None
    assert record.pk == match.pk


def test_pipeline_filters_by_eligible_lead_sources_only(sales_lead_env):
    """UserSettings.lead_sources restricts data.lead_source."""
    env = sales_lead_env
    _make_sales_lead_user_settings(
        env,
        eligible_parties=[],
        lead_sources=["PREMIUM_REFERRAL"],
    )

    RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Wrong source",
            lead_source="OTHER_SOURCE",
            lead_stage="IN_QUEUE",
            lead_score=500,
        ),
    )
    good = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Matching source",
            lead_source="PREMIUM_REFERRAL",
            lead_stage="IN_QUEUE",
            lead_score=1,
        ),
    )

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is not None
    assert record.pk == good.pk


def test_daily_limit_skips_fresh_bucket_but_snoozed_due_still_assignable(sales_lead_env):
    """When daily limit is reached, buckets with daily_limit_applies are skipped; follow-up snoozed due still assigns."""
    env = sales_lead_env
    now = timezone.now()
    _make_sales_lead_user_settings(env, eligible_parties=[], lead_sources=[], daily_limit=1)

    Record.objects.create(
        tenant=env.tenant,
        entity_type="lead",
        data={
            "lead_status": "SALES LEAD",
            "lead_source": "PREMIUM_REFERRAL",
            "first_assigned_to": env.user_identifier,
            "first_assigned_at": now.isoformat(),
            "lead_stage": "ASSIGNED",
            "call_attempts": 0,
        },
    )

    RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Fresh blocked",
            lead_stage="IN_QUEUE",
            call_attempts=0,
            lead_score=999,
        ),
    )

    snoozed = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Snoozed due for me",
            lead_stage="SNOOZED",
            assigned_to=env.user_identifier,
            call_attempts=1,
            next_call_at=(now - timedelta(hours=1)).isoformat(),
            lead_score=1,
        ),
    )

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is not None
    assert record.pk == snoozed.pk


def test_daily_limit_reached_no_fresh_when_only_fresh_available(sales_lead_env):
    """Limit reached and only unassigned fresh leads exist → no assignment (no snoozed / retry)."""
    env = sales_lead_env
    now = timezone.now()
    _make_sales_lead_user_settings(env, eligible_parties=[], lead_sources=[], daily_limit=1)

    Record.objects.create(
        tenant=env.tenant,
        entity_type="lead",
        data={
            "lead_status": "SALES LEAD",
            "lead_source": "PREMIUM_REFERRAL",
            "first_assigned_to": env.user_identifier,
            "first_assigned_at": now.isoformat(),
            "lead_stage": "ASSIGNED",
            "call_attempts": 0,
        },
    )

    RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Only fresh",
            lead_stage="IN_QUEUE",
            call_attempts=0,
        ),
    )

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is None


def test_snoozed_due_prioritized_over_fresh_lead(sales_lead_env):
    """Pull strategy ranks expired snoozed first; pipeline returns snoozed before a fresh lead."""
    env = sales_lead_env
    _make_sales_lead_user_settings(env, eligible_parties=[], lead_sources=[])
    now = timezone.now()
    past = (now - timedelta(hours=1)).isoformat()

    snoozed = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Snoozed first",
            lead_stage="SNOOZED",
            assigned_to=env.user_identifier,
            next_call_at=past,
            call_attempts=1,
            lead_score=10,
        ),
    )
    RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Fresh second",
            lead_stage="IN_QUEUE",
            call_attempts=0,
            lead_score=999,
        ),
    )

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is not None
    assert record.pk == snoozed.pk


def test_not_connected_retry_only_after_next_call_at_passed(sales_lead_env):
    """next_call_due + PullStrategy require next_call_at <= now; future snooze is excluded."""
    env = sales_lead_env
    _make_sales_lead_user_settings(env, eligible_parties=[], lead_sources=[])
    now = timezone.now()
    future = (now + timedelta(hours=2)).isoformat()

    RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="NC not due",
            lead_stage="NOT_CONNECTED",
            assigned_to=env.user_identifier,
            call_attempts=2,
            next_call_at=future,
            last_call_outcome="not_connected",
        ),
    )
    fresh = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Fresh when retry blocked",
            lead_stage="IN_QUEUE",
            call_attempts=0,
        ),
    )

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is not None
    assert record.pk == fresh.pk


def test_not_connected_assigned_to_me_returned_when_due(sales_lead_env):
    """NOT_CONNECTED bucket: assigned-to-me with next_call_at in the past is assignable."""
    env = sales_lead_env
    _make_sales_lead_user_settings(env, eligible_parties=[], lead_sources=[])
    now = timezone.now()
    past = (now - timedelta(minutes=5)).isoformat()

    nc = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="NC due",
            lead_stage="NOT_CONNECTED",
            assigned_to=env.user_identifier,
            call_attempts=2,
            next_call_at=past,
            last_call_outcome="not_connected",
        ),
    )

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is not None
    assert record.pk == nc.pk


def test_release_after_12h_unassigns_then_pipeline_assigns_when_next_call_due(sales_lead_env):
    """
    ReleaseLeadsAfter12hJobHandler clears assigned_to (NOT_CONNECTED) and sets next_call_at ~ +1h.
    After moving next_call_at to the past, the unassigned retry lead is eligible and assigned.
    """
    env = sales_lead_env
    _make_sales_lead_user_settings(env, eligible_parties=[], lead_sources=[])
    now = timezone.now()
    anchor = (now - timedelta(hours=13)).isoformat()

    lead = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="NC 12h release",
            lead_stage="NOT_CONNECTED",
            assigned_to=env.user_identifier,
            call_attempts=2,
            next_call_at=(now - timedelta(hours=1)).isoformat(),
            first_assigned_today_at=anchor,
            first_assignment_today_date=_local_date_iso(now),
            first_assigned_to=env.user_identifier,
            first_assigned_at=(now - timedelta(days=1)).isoformat(),
            last_call_outcome="not_connected",
        ),
    )

    job = BackgroundJobFactory(
        tenant=env.tenant,
        job_type=JobType.RELEASE_LEADS_AFTER_12H,
        payload={},
    )
    handler = ReleaseLeadsAfter12hJobHandler()
    assert handler.process(job) is True
    lead.refresh_from_db()
    assert lead.data.get("assigned_to") in (None, "", "null")

    # Simulate time passing until next_call_at is due (job sets next_call_at ~ +1h).
    data = dict(lead.data or {})
    data["next_call_at"] = (now - timedelta(minutes=1)).isoformat()
    lead.data = data
    lead.save(update_fields=["data", "updated_at"])

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is not None
    assert record.pk == lead.pk
    assert record.data.get("assigned_to") == env.user_identifier


def test_nc_retry_fallback_unassigned_after_12h_release(sales_lead_env):
    """NC bucket falls back to unassigned scope -- picks up 12h-released leads."""
    env = sales_lead_env
    _make_sales_lead_user_settings(env, eligible_parties=[], lead_sources=[])
    past = (timezone.now() - timedelta(minutes=5)).isoformat()

    released = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Released NC",
            lead_stage="NOT_CONNECTED",
            assigned_to=None,
            call_attempts=3,
            next_call_at=past,
            last_call_outcome="not_connected",
        ),
    )

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is not None
    assert record.pk == released.pk
    record.refresh_from_db()
    assert record.data.get("assigned_to") == env.user_identifier


def test_daily_limit_fallback_picks_unassigned_nc_retry(sales_lead_env):
    """When daily limit reached and no assigned-to-me retries, unassigned NC retries are tried."""
    env = sales_lead_env
    now = timezone.now()
    _make_sales_lead_user_settings(env, eligible_parties=[], lead_sources=[], daily_limit=1)

    Record.objects.create(
        tenant=env.tenant,
        entity_type="lead",
        data={
            "lead_status": "SALES LEAD",
            "lead_source": "PREMIUM_REFERRAL",
            "first_assigned_to": env.user_identifier,
            "first_assigned_at": now.isoformat(),
            "lead_stage": "ASSIGNED",
            "call_attempts": 0,
        },
    )

    nc = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Unassigned NC retry",
            lead_stage="NOT_CONNECTED",
            assigned_to=None,
            call_attempts=2,
            next_call_at=(now - timedelta(minutes=5)).isoformat(),
            last_call_outcome="not_connected",
        ),
    )

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is not None
    assert record.pk == nc.pk


def test_daily_limit_fallback_includes_attempts_six(sales_lead_env):
    """Fallback uses call_attempts <= 6, so attempts=6 IS included (unlike NC bucket which uses < 6)."""
    env = sales_lead_env
    now = timezone.now()
    _make_sales_lead_user_settings(env, eligible_parties=[], lead_sources=[], daily_limit=1)

    Record.objects.create(
        tenant=env.tenant,
        entity_type="lead",
        data={
            "lead_status": "SALES LEAD",
            "lead_source": "PREMIUM_REFERRAL",
            "first_assigned_to": env.user_identifier,
            "first_assigned_at": now.isoformat(),
            "lead_stage": "ASSIGNED",
            "call_attempts": 0,
        },
    )

    six = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(
            name="Six attempts",
            lead_stage="NOT_CONNECTED",
            assigned_to=env.user_identifier,
            call_attempts=6,
            next_call_at=(now - timedelta(minutes=5)).isoformat(),
            last_call_outcome="not_connected",
        ),
    )

    pipeline = LeadPipeline()
    record = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert record is not None
    assert record.pk == six.pk


def test_end_to_end_fresh_to_not_connected_to_retry(sales_lead_env):
    """Full lifecycle: IN_QUEUE -> ASSIGNED -> NOT_CONNECTED -> pipeline retry."""
    env = sales_lead_env
    _make_sales_lead_user_settings(env, eligible_parties=[], lead_sources=[])
    now = timezone.now()

    lead = RecordFactory(
        tenant=env.tenant,
        entity_type="lead",
        data=_sales_lead_data(name="Lifecycle", lead_stage="IN_QUEUE", call_attempts=0, lead_score=100),
    )

    pipeline = LeadPipeline()
    result = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert result is not None
    assert result.pk == lead.pk
    result.refresh_from_db()
    assert result.data["lead_stage"] == "ASSIGNED"
    assert result.data["assigned_to"] == env.user_identifier

    data = dict(result.data)
    data["lead_stage"] = "NOT_CONNECTED"
    data["last_call_outcome"] = "not_connected"
    data["call_attempts"] = 1
    data["next_call_at"] = (now - timedelta(minutes=1)).isoformat()
    result.data = data
    result.save(update_fields=["data", "updated_at"])

    retry_result = pipeline.get_next(tenant=env.tenant, request_user=env.user, debug=False)
    assert retry_result is not None
    assert retry_result.pk == lead.pk
    retry_result.refresh_from_db()
    assert retry_result.data["lead_stage"] == "ASSIGNED"


def test_compute_next_call_from_attempts_fixed_minutes_matches_rule_engine():
    """Rule action compute_next_call_from_attempts with fixed_minutes (e.g. not_connected rules)."""
    tenant = TenantFactory()
    record = RecordFactory(
        tenant=tenant,
        entity_type="lead",
        data={"lead_status": "SALES LEAD", "call_attempts": 3},
    )
    ctx = {"record": record}
    before = timezone.now()
    action_compute_next_call_from_attempts(ctx, fixed_minutes=2, attempts_field="call_attempts", target_field="next_call_at")
    record.refresh_from_db()
    raw = record.data.get("next_call_at")
    assert raw
    parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    delta = (parsed - before).total_seconds()
    assert 120 - 30 <= delta <= 120 + 30
