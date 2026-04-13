"""
Tests for the race condition fix in LeadAssigner.assign_main_queue().

The bug: two RMs call /leads/next/ at the same time. Both scan and pick the
same candidate. RM A locks and assigns first. RM B then acquires the lock
(after RM A commits) and blindly overwrites assigned_to — stealing the lead.

The fix: after acquiring the lock, abort if assigned_to is already set to
someone other than the requesting RM.

Run:
  pytest src/tests/rest/crm_records/test_lead_assigner_race_condition.py -v
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone as std_utc
from unittest.mock import MagicMock, patch

import pytest
from django.utils import timezone

from crm_records.lead_pipeline.lead_assigner import LeadAssigner, AssignmentResult
from crm_records.lead_pipeline.candidate_selector import CandidateSelector
from crm_records.lead_pipeline.post_assignment import PostAssignmentActions
from crm_records.models import Record

from tests.factories import (
    RecordFactory,
    TenantFactory,
    TenantMembershipFactory,
    RoleFactory,
    SupabaseAuthUserFactory,
    UserFactory,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_user(tenant):
    uid = str(uuid.uuid4())
    user = UserFactory(supabase_uid=uid, email=f"{uid[:8]}@test.com", tenant_id=str(tenant.id))
    SupabaseAuthUserFactory(id=uuid.UUID(uid), email=user.email)
    role = RoleFactory(tenant=tenant, key="pyro_admin", name="Pyro Admin")
    membership = TenantMembershipFactory(tenant=tenant, user_id=uid, email=user.email, role=role)
    return user, membership, uid


def _noop_post_actions():
    """PostAssignmentActions stub that does nothing."""
    mock = MagicMock(spec=PostAssignmentActions)
    mock.run.return_value = None
    return mock


def _make_assigner():
    return LeadAssigner(post_actions=_noop_post_actions())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.django_db(transaction=True)
def test_assign_fresh_lead_succeeds():
    """Baseline: assigning an unassigned lead returns an AssignmentResult."""
    tenant = TenantFactory()
    user, membership, uid = _make_user(tenant)
    lead = RecordFactory(tenant=tenant, entity_type="lead", data={
        "lead_stage": "FRESH",
        "call_attempts": 0,
        "assigned_to": None,
    })

    assigner = _make_assigner()
    result = assigner.assign_main_queue(
        candidate_pk=lead.pk,
        tenant=tenant,
        user=user,
        tenant_membership=membership,
        user_identifier=uid,
        user_uuid=uid,
        now=timezone.now(),
    )

    assert result is not None
    assert isinstance(result, AssignmentResult)
    lead.refresh_from_db()
    assert lead.data["assigned_to"] == uid
    assert lead.data["lead_stage"] == "ASSIGNED"


@pytest.mark.django_db(transaction=True)
def test_race_condition_second_rm_is_rejected():
    """
    Core race condition fix: if RM A has already claimed the lead (assigned_to
    is set), RM B's assign_main_queue call must return None and must NOT
    overwrite assigned_to.
    """
    tenant = TenantFactory()
    user_a, membership_a, uid_a = _make_user(tenant)
    user_b, membership_b, uid_b = _make_user(tenant)

    # Lead already claimed by RM A (simulates RM A having committed first)
    lead = RecordFactory(tenant=tenant, entity_type="lead", data={
        "lead_stage": "ASSIGNED",
        "call_attempts": 0,
        "assigned_to": uid_a,
    })

    assigner = _make_assigner()
    result = assigner.assign_main_queue(
        candidate_pk=lead.pk,
        tenant=tenant,
        user=user_b,
        tenant_membership=membership_b,
        user_identifier=uid_b,
        user_uuid=uid_b,
        now=timezone.now(),
    )

    assert result is None, "RM B should be rejected — lead already belongs to RM A"
    lead.refresh_from_db()
    assert lead.data["assigned_to"] == uid_a, "assigned_to must not be overwritten by RM B"


@pytest.mark.django_db(transaction=True)
def test_reassign_to_same_rm_is_allowed():
    """
    An RM re-pulling their own lead (e.g. a snoozed callback) must not be
    blocked by the guard — previous_assigned_to == user_identifier is allowed.
    """
    tenant = TenantFactory()
    user, membership, uid = _make_user(tenant)
    past = (timezone.now() - __import__("datetime").timedelta(hours=1)).isoformat()
    lead = RecordFactory(tenant=tenant, entity_type="lead", data={
        "lead_stage": "SNOOZED",
        "call_attempts": 1,
        "assigned_to": uid,
        "next_call_at": past,
    })

    assigner = _make_assigner()
    result = assigner.assign_main_queue(
        candidate_pk=lead.pk,
        tenant=tenant,
        user=user,
        tenant_membership=membership,
        user_identifier=uid,
        user_uuid=uid,
        now=timezone.now(),
    )

    assert result is not None, "RM re-pulling their own snoozed lead must succeed"
    lead.refresh_from_db()
    assert lead.data["assigned_to"] == uid


@pytest.mark.django_db(transaction=True)
def test_assign_null_string_assigned_to_is_treated_as_fresh():
    """
    Legacy records may have assigned_to="null" or assigned_to="None".
    These should be treated as unassigned and allow a fresh assignment.
    """
    tenant = TenantFactory()
    user, membership, uid = _make_user(tenant)

    for null_value in (None, "", "null", "None"):
        lead = RecordFactory(tenant=tenant, entity_type="lead", data={
            "lead_stage": "FRESH",
            "call_attempts": 0,
            "assigned_to": null_value,
        })

        assigner = _make_assigner()
        result = assigner.assign_main_queue(
            candidate_pk=lead.pk,
            tenant=tenant,
            user=user,
            tenant_membership=membership,
            user_identifier=uid,
            user_uuid=uid,
            now=timezone.now(),
        )

        assert result is not None, f"assigned_to={null_value!r} should be treated as unassigned"
        lead.refresh_from_db()
        assert lead.data["assigned_to"] == uid


@pytest.mark.django_db(transaction=True)
def test_skip_locked_returns_none():
    """
    If the row is already locked by another transaction (skip_locked=True),
    assign_main_queue returns None without raising.
    """
    tenant = TenantFactory()
    user, membership, uid = _make_user(tenant)
    lead = RecordFactory(tenant=tenant, entity_type="lead", data={
        "lead_stage": "FRESH",
        "call_attempts": 0,
        "assigned_to": None,
    })

    assigner = _make_assigner()

    # Simulate skip_locked returning empty queryset (row locked by another worker)
    with patch(
        "crm_records.lead_pipeline.lead_assigner.Record.objects.select_for_update",
        return_value=MagicMock(
            **{"filter.return_value.first.return_value": None}
        ),
    ):
        result = assigner.assign_main_queue(
            candidate_pk=lead.pk,
            tenant=tenant,
            user=user,
            tenant_membership=membership,
            user_identifier=uid,
            user_uuid=uid,
            now=timezone.now(),
        )

    assert result is None
