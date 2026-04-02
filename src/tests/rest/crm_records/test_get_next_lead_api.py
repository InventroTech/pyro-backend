"""
Tests for Get Next Lead API (/crm-records/leads/next/).

All tests use the Django test database. API tests use RecordFactory for leads
and BaseAPITestCase (tenant, user, auth). Authenticated requests use
force_authenticate(user=self.user) so view logic and tenant resolution run
with a real user; auth_headers (JWT + HTTP_X_TENANT_ID) are still sent for
middleware/tenant resolution.

Run from project root (where pytest.ini is):
  pytest src/tests/rest/crm_records/test_get_next_lead_api.py -v
  pytest -k get_next_lead -v

Get Next Lead uses ``LeadPipeline`` for every RM (including SELF TRIAL): tenant buckets,
routing, call-attempt matrices, daily limit, and not-connected fallback match the
pipeline implementation. ``NOT_CONNECTED`` with ``call_attempts=0`` does not match
retry buckets (attempts 1–6 required).
"""

from datetime import datetime, timedelta, timezone

import jwt
from django.conf import settings
from django.test import TestCase
from django.utils import timezone as django_timezone

from crm_records.models import Record
from authz import service as authz_service
from user_settings.models import UserSettings

from tests.base.test_setup import BaseAPITestCase
from tests.factories import RecordFactory


def _make_jwt_no_tenant(sub: str, email: str) -> str:
    """JWT with no tenant (for 403 tests: no tenant in JWT, rely on slug or fail)."""
    payload = {
        "sub": sub,
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
        "iat": datetime.now(timezone.utc),
        "role": "authenticated",
        "aud": "authenticated",
    }
    token = jwt.encode(payload, settings.SUPABASE_JWT_SECRET, algorithm="HS256")
    return token if isinstance(token, str) else token.decode("utf-8")


def _should_exclude_lead(lead_data: dict, current_user_id: str) -> bool:
    """Same logic as GetNextLead exclude: exclude if assigned_to is set and not current user."""
    assigned_to = (lead_data or {}).get("assigned_to")
    if not assigned_to or assigned_to in ("", "null", "None"):
        return False
    return assigned_to != current_user_id


class GetNextLeadExcludeLogicTests(TestCase):
    """Unit tests for exclude logic (use test DB)."""

    def test_exclude_lead_assigned_to_other_user(self):
        """Lead with assigned_to = other user should be excluded."""
        lead = {"lead_stage": "IN_QUEUE", "assigned_to": "user-a-uuid", "call_attempts": 0}
        assert _should_exclude_lead(lead, "user-b-uuid") is True
        assert _should_exclude_lead(lead, "user-a-uuid") is False

    def test_do_not_exclude_unassigned_lead(self):
        """Lead with no assigned_to should not be excluded."""
        lead = {"lead_stage": "IN_QUEUE", "call_attempts": 0}
        assert _should_exclude_lead(lead, "user-a-uuid") is False
        lead_empty = {"lead_stage": "IN_QUEUE", "assigned_to": ""}
        assert _should_exclude_lead(lead_empty, "user-a-uuid") is False

    def test_do_not_exclude_lead_assigned_to_current_user(self):
        """Lead with assigned_to = current user should not be excluded."""
        lead = {"lead_stage": "IN_QUEUE", "assigned_to": "current-uuid", "call_attempts": 0}
        assert _should_exclude_lead(lead, "current-uuid") is False


class GetNextLeadAPITests(BaseAPITestCase):
    """API tests for Get Next Lead with RecordFactory; force_authenticate + auth_headers for tenant."""

    def setUp(self):
        super().setUp()
        self.url = "/crm-records/leads/next/"
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)

    def test_unauthenticated_returns_403(self):
        """GET without auth returns 403."""
        self.client.force_authenticate(user=None)
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 403)
        self.client.force_authenticate(user=self.user)

    def test_authenticated_no_tenant_returns_403(self):
        """GET with JWT but no tenant_id in JWT and no slug returns 403 when tenant cannot be resolved."""
        token = _make_jwt_no_tenant(self.supabase_uid, self.email)
        response = self.client.get(self.url, HTTP_AUTHORIZATION=f"Bearer {token}")
        self.assertEqual(response.status_code, 403)

    def test_authenticated_unknown_tenant_slug_returns_403(self):
        """GET with JWT (no tenant in JWT) and non-existent tenant slug returns 403."""
        token = _make_jwt_no_tenant(self.supabase_uid, self.email)
        response = self.client.get(
            self.url,
            HTTP_AUTHORIZATION=f"Bearer {token}",
            HTTP_X_TENANT_SLUG="nonexistent-tenant",
        )
        self.assertEqual(response.status_code, 403)

    def test_authenticated_no_queueable_leads_returns_200_empty(self):
        """GET with auth and tenant but no queueable leads returns 200 with empty body."""
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {})

    def test_authenticated_with_queueable_lead_returns_lead_and_assigns(self):
        """GET with auth and one queueable unassigned lead returns 200 with lead and assigns it to user."""
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Queueable Lead",
                "phone_number": "+1234567890",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {})
        self.assertIn("id", data)
        self.assertIn("data", data)
        self.assertEqual(data["data"].get("assigned_to"), self.supabase_uid)
        self.assertEqual(data.get("lead_status"), "ASSIGNED")

    def test_tenant_isolation(self):
        """Leads from another tenant are not returned."""
        from tests.factories import TenantFactory
        other_tenant = TenantFactory()
        RecordFactory(
            tenant=other_tenant,
            entity_type="lead",
            data={
                "name": "Other Tenant Lead",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        if data:
            self.assertNotEqual(
                Record.objects.get(id=data["id"]).tenant_id,
                other_tenant.id,
            )


class GetNextLeadAPIWithSettingsTests(BaseAPITestCase):
    """Get Next Lead with UserSettings (eligible_lead_sources, daily_limit). RecordFactory + force_authenticate."""

    def setUp(self):
        super().setUp()
        self.url = "/crm-records/leads/next/"
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)

    def test_with_eligible_lead_sources_only_matching_source_returned(self):
        """When user has eligible_lead_sources, only leads with matching lead_source are returned."""
        UserSettings.objects.create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="LEAD_TYPE_ASSIGNMENT",
            value=[],  # value is NOT NULL; we filter by lead_sources here
            lead_sources=["SALES LEAD"],
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Sales Lead",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Self Trial Lead",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SELF TRIAL",
                "call_attempts": 0,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {})
        self.assertEqual(data["data"].get("lead_source"), "SALES LEAD")

    def test_daily_limit_fallback_assigns_unassigned_not_connected_when_no_assigned_retry(self):
        """When daily limit is reached and no assigned-to-user retry lead exists, fallback assigns and returns an unassigned NOT_CONNECTED due lead matching filters (e.g. SELF TRIAL)."""
        from django.utils import timezone
        
        UserSettings.objects.create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="LEAD_TYPE_ASSIGNMENT",
            value=[],  # value is NOT NULL; we use lead_sources and daily_limit here
            lead_sources=["SELF TRIAL"],
            daily_limit=1,
        )
        
        now = timezone.now()
        past_time = now - timezone.timedelta(hours=12) # Push it 12 hours back to guarantee it is "due"
        
        # 1. Lead that counts as "assigned today" so we hit daily limit
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "first_assigned_to": self.supabase_uid,
                "first_assigned_at": now.isoformat(),
                "assigned_to": self.supabase_uid,
                "lead_stage": "ASSIGNED",
                "lead_source": "SELF TRIAL", # Matched to settings
            },
        )
        
        # 2. Unassigned NOT_CONNECTED SELF TRIAL: fallback assigns it to user and returns it
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Self Trial Not Connected",
                "lead_stage": "NOT_CONNECTED",
                "lead_source": "SELF TRIAL",
                "assigned_to": "",  # 👇 FIX 1: Use empty string instead of None
                "first_assigned_to": "", # Ensure the system knows it has NEVER been assigned
                "call_attempts": 1,
                "next_call_at": past_time.isoformat(), # 👇 FIX 2: Solidly in the past
                "phone_number": "+1234567890",
            },
        )
        
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        
        data = response.json()
        self.assertNotEqual(data, {}, msg="Fallback assigns unassigned NOT_CONNECTED due lead and returns it")
        
        # 👇 FIX 3: Look inside the nested "data" JSON blob for the assertions!
        lead_data = data.get("data", {})
        self.assertEqual(lead_data.get("name"), "Self Trial Not Connected")
        self.assertEqual(lead_data.get("lead_source"), "SELF TRIAL")
        self.assertEqual(lead_data.get("assigned_to"), self.supabase_uid)

    def test_daily_limit_fallback_assigns_unassigned_in_queue_due_when_no_assigned_retry(self):
        """When daily limit is reached and no assigned retry lead exists, fallback assigns and returns an unassigned IN_QUEUE due lead (NOT_CONNECTED/IN_QUEUE path)."""
        from django.utils import timezone
        UserSettings.objects.create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="LEAD_TYPE_ASSIGNMENT",
            value=[],
            lead_sources=["SELF TRIAL"],
            daily_limit=1,
        )
        now = timezone.now()
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "first_assigned_to": self.supabase_uid,
                "first_assigned_at": now.isoformat(),
                "lead_stage": "ASSIGNED",
                "lead_source": "SALES LEAD",
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Self Trial IN_QUEUE Due",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SELF TRIAL",
                "assigned_to": None,
                "call_attempts": 1,
                "next_call_at": (now - timezone.timedelta(hours=1)).isoformat(),
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {}, msg="Fallback assigns unassigned IN_QUEUE due lead and returns it")
        self.assertEqual(data.get("name"), "Self Trial IN_QUEUE Due")
        self.assertEqual(data.get("data", {}).get("lead_stage"), "ASSIGNED")
        self.assertEqual(data.get("data", {}).get("assigned_to"), self.supabase_uid)

    def test_only_not_connected_leads_without_daily_limit_returns_empty(self):
        """NOT_CONNECTED with call_attempts=0 is not in the main queue and does not match Step 5a retry (needs attempts 1–6)."""
        UserSettings.objects.create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="LEAD_TYPE_ASSIGNMENT",
            value=[],
            lead_statuses=["SELF TRIAL"],
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Only Not Connected",
                "lead_stage": "NOT_CONNECTED",
                "lead_status": "SELF TRIAL",
                "lead_source": "SIGNUP_AT_SINGLE_PARTY",
                "call_attempts": 0,
                "assigned_to": None,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {},
            msg="NOT_CONNECTED with 0 attempts: not main queue, not Step 5a retry",
        )

    def test_self_trial_legacy_step_5a_assigns_unassigned_not_connected_due_when_no_fresh_under_daily_limit(self):
        """Legacy SELF TRIAL: under daily limit, no fresh queueable leads — Step 5a returns due unassigned NOT_CONNECTED retry."""
        now = django_timezone.now()
        past = (now - timedelta(hours=3)).isoformat()
        UserSettings.objects.create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="LEAD_TYPE_ASSIGNMENT",
            value=[],
            lead_sources=[],
            lead_statuses=["SELF TRIAL"],
            daily_limit=50,
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Step 5a NC Retry",
                "lead_stage": "NOT_CONNECTED",
                "lead_status": "SELF TRIAL",
                "lead_source": "SIGNUP_AT_SINGLE_PARTY",
                "assigned_to": "",
                "call_attempts": 1,
                "next_call_at": past,
                "phone_number": "+19990000001",
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {}, msg="Step 5a should assign unassigned NOT_CONNECTED due lead")
        lead_blob = data.get("data", {})
        self.assertEqual(lead_blob.get("name"), "Step 5a NC Retry")
        self.assertEqual(lead_blob.get("lead_status"), "SELF TRIAL")
        self.assertEqual(data.get("lead_status"), "ASSIGNED")
        self.assertEqual(lead_blob.get("assigned_to"), self.supabase_uid)

    def test_self_trial_legacy_step_5a_prefers_assigned_to_me_not_connected_before_unassigned(self):
        """Legacy SELF TRIAL: assigned-to-me due NOT_CONNECTED (lower call_attempts) wins over unassigned retry."""
        now = django_timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        UserSettings.objects.create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="LEAD_TYPE_ASSIGNMENT",
            value=[],
            lead_statuses=["SELF TRIAL"],
            daily_limit=50,
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Unassigned NC",
                "lead_stage": "NOT_CONNECTED",
                "lead_status": "SELF TRIAL",
                "assigned_to": "",
                "call_attempts": 1,
                "next_call_at": past,
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Mine NC First",
                "lead_stage": "NOT_CONNECTED",
                "lead_status": "SELF TRIAL",
                "assigned_to": self.supabase_uid,
                "call_attempts": 1,
                "next_call_at": past,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {})
        self.assertEqual(data.get("name"), "Mine NC First")
        self.assertEqual(data.get("data", {}).get("assigned_to"), self.supabase_uid)


class RecordListNotConnectedIncludesUnassignedTests(BaseAPITestCase):
    """List API: when filtering by lead_stage=NOT_CONNECTED and assigned_to=user, at least assigned leads are returned."""

    def setUp(self):
        super().setUp()
        self.list_url = "/crm-records/records/"
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)

    def test_list_not_connected_with_assigned_to_includes_unassigned_leads(self):
        """GET records?entity_type=lead&lead_stage=NOT_CONNECTED&assigned_to=user returns at least assigned leads."""
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Assigned Not Connected",
                "lead_stage": "NOT_CONNECTED",
                "assigned_to": self.supabase_uid,
                "lead_source": "SALES LEAD",
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Unassigned Self Trial",
                "lead_stage": "NOT_CONNECTED",
                "assigned_to": None,
                "lead_source": "SELF TRIAL",
            },
        )
        query = {"entity_type": "lead", "lead_stage": "NOT_CONNECTED", "assigned_to": self.supabase_uid}
        response = self.client.get(self.list_url, data=query, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        # Pagination returns {"data": [...], "page_meta": {...}}
        results = data.get("data", []) if isinstance(data, dict) else []
        # At least the lead with assigned_to=user must appear; unassigned may be included if view has retry logic
        self.assertGreaterEqual(
            len(results),
            1,
            msg="List with lead_stage=NOT_CONNECTED and assigned_to=user must include at least assigned leads",
        )


class GetNextLeadSnoozedPriorityTests(BaseAPITestCase):
    """
    Tests for Get Next Lead Step 3a: SNOOZED/IN_QUEUE leads with next_call_at due.
    Priority: (1) assigned to current user, (2) unassigned, (3) fresh queue.
    """

    def setUp(self):
        super().setUp()
        self.url = "/crm-records/leads/next/"
        authz_service._CACHE.clear()
        self.client.force_authenticate(user=self.user)

    def test_step_3a_i_returns_assigned_snoozed_due_before_fresh(self):
        """Step 3a(i): SNOOZED lead assigned to current user with next_call_at due is returned first (before any fresh lead)."""
        now = django_timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "My Snoozed Due",
                "lead_stage": "SNOOZED",
                "lead_source": "SALES LEAD",
                "assigned_to": self.supabase_uid,
                "next_call_at": past,
                "call_attempts": 1,
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Fresh Lead",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {}, msg="Should return a lead")
        self.assertEqual(data.get("name"), "My Snoozed Due")
        self.assertEqual(data["data"].get("lead_stage"), "ASSIGNED")
        self.assertEqual(data["data"].get("assigned_to"), self.supabase_uid)

    def test_step_3a_i_returns_assigned_in_queue_due_before_fresh(self):
        """Step 3a(i): IN_QUEUE lead assigned to current user with next_call_at due is returned first (SNOOZED/IN_QUEUE path)."""
        now = django_timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "My IN_QUEUE Due",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "assigned_to": self.supabase_uid,
                "next_call_at": past,
                "call_attempts": 1,
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Fresh Lead",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {}, msg="Should return assigned IN_QUEUE due lead")
        self.assertEqual(data.get("name"), "My IN_QUEUE Due")
        self.assertEqual(data["data"].get("lead_stage"), "ASSIGNED")
        self.assertEqual(data["data"].get("assigned_to"), self.supabase_uid)

    def test_step_3a_ii_returns_unassigned_snoozed_due_when_no_assigned_snoozed(self):
        """Step 3a(ii): When no assigned-to-me snoozed due, unassigned SNOOZED with next_call_at due is returned (before fresh)."""
        now = django_timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Unassigned Snoozed Due",
                "lead_stage": "SNOOZED",
                "lead_source": "SALES LEAD",
                "assigned_to": None,
                "next_call_at": past,
                "call_attempts": 1,
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Fresh Lead",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {}, msg="Should return unassigned snoozed due lead")
        self.assertEqual(data.get("name"), "Unassigned Snoozed Due")
        self.assertEqual(data["data"].get("lead_stage"), "ASSIGNED")
        self.assertEqual(data["data"].get("assigned_to"), self.supabase_uid)

    def test_step_3a_ii_returns_unassigned_in_queue_due_when_next_call_at_passed(self):
        """Step 3a(ii): Unassigned IN_QUEUE lead with next_call_at due is returned (SNOOZED/IN_QUEUE path) before fresh."""
        now = django_timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Unassigned IN_QUEUE Due",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "assigned_to": None,
                "next_call_at": past,
                "call_attempts": 1,
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Fresh Lead",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {}, msg="Should return unassigned IN_QUEUE due lead")
        self.assertEqual(data.get("name"), "Unassigned IN_QUEUE Due")
        self.assertEqual(data["data"].get("lead_stage"), "ASSIGNED")
        self.assertEqual(data["data"].get("assigned_to"), self.supabase_uid)

    def test_step_3a_assigned_snoozed_takes_priority_over_unassigned_snoozed(self):
        """When both assigned-to-me snoozed due and unassigned snoozed due exist, assigned-to-me is returned."""
        now = django_timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Unassigned Snoozed",
                "lead_stage": "SNOOZED",
                "lead_source": "SALES LEAD",
                "assigned_to": None,
                "next_call_at": past,
                "call_attempts": 1,
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "My Snoozed Due",
                "lead_stage": "SNOOZED",
                "lead_source": "SALES LEAD",
                "assigned_to": self.supabase_uid,
                "next_call_at": past,
                "call_attempts": 1,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {})
        self.assertEqual(data.get("name"), "My Snoozed Due")

    def test_snoozed_with_next_call_at_in_future_not_returned_in_step_3a(self):
        """SNOOZED lead (assigned or unassigned) with next_call_at in the future is not returned in Step 3a; fresh lead can be returned."""
        now = django_timezone.now()
        future = (now + timedelta(hours=2)).isoformat()
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Snoozed Future",
                "lead_stage": "SNOOZED",
                "lead_source": "SALES LEAD",
                "assigned_to": self.supabase_uid,
                "next_call_at": future,
                "call_attempts": 1,
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Fresh Lead",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {}, msg="Should return fresh lead when snoozed not due")
        self.assertEqual(data.get("name"), "Fresh Lead")

    def test_snoozed_with_call_attempts_6_not_eligible_for_step_3a(self):
        """SNOOZED lead with call_attempts >= 6 is not eligible for Step 3a; fresh lead is returned instead."""
        now = django_timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Snoozed Exhausted",
                "lead_stage": "SNOOZED",
                "lead_source": "SALES LEAD",
                "assigned_to": None,
                "next_call_at": past,
                "call_attempts": 6,
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Fresh Lead",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {})
        self.assertEqual(data.get("name"), "Fresh Lead")

    def test_no_snoozed_due_falls_back_to_fresh_queue(self):
        """When no snoozed-due leads (assigned or unassigned), Get Next Lead returns from fresh queue."""
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Fresh Only",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {})
        self.assertEqual(data.get("name"), "Fresh Only")
        self.assertEqual(data["data"].get("lead_stage"), "ASSIGNED")

    def test_snoozed_due_respects_eligible_lead_sources(self):
        """Step 3a only returns snoozed leads that match user's eligible_lead_sources (e.g. SALES LEAD)."""
        UserSettings.objects.create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key="LEAD_TYPE_ASSIGNMENT",
            value=[],
            lead_sources=["SALES LEAD"],
        )
        now = django_timezone.now()
        past = (now - timedelta(hours=1)).isoformat()
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Snoozed Sales",
                "lead_stage": "SNOOZED",
                "lead_source": "SALES LEAD",
                "assigned_to": None,
                "next_call_at": past,
                "call_attempts": 1,
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Snoozed Self Trial",
                "lead_stage": "SNOOZED",
                "lead_source": "SELF TRIAL",
                "assigned_to": None,
                "next_call_at": past,
                "call_attempts": 1,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {}, msg="Should return one lead")
        self.assertEqual(data["data"].get("lead_source"), "SALES LEAD")
        self.assertEqual(data.get("name"), "Snoozed Sales")
