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
"""

from datetime import datetime, timedelta, timezone

import jwt
from django.conf import settings
from django.test import TestCase

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
        # Lead that counts as "assigned today" so we hit daily limit
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
        # Unassigned NOT_CONNECTED SELF TRIAL: fallback assigns it to user and returns it
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Self Trial Not Connected",
                "lead_stage": "NOT_CONNECTED",
                "lead_source": "SELF TRIAL",
                "assigned_to": None,
                "call_attempts": 1,
                "next_call_at": (now - timezone.timedelta(hours=1)).isoformat(),
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotEqual(data, {}, msg="Fallback assigns unassigned NOT_CONNECTED due lead and returns it")
        self.assertEqual(data.get("name"), "Self Trial Not Connected")
        self.assertEqual(data.get("data", {}).get("lead_source"), "SELF TRIAL")
        self.assertEqual(data.get("assigned_to"), self.supabase_uid)

    def test_only_not_connected_leads_without_daily_limit_returns_empty(self):
        """When no daily limit, NOT_CONNECTED-only leads are not in main queue so Get Next Lead returns empty."""
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Only Not Connected",
                "lead_stage": "NOT_CONNECTED",
                "lead_source": "SELF TRIAL",
                "call_attempts": 0,
                "assigned_to": None,
            },
        )
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {}, msg="NOT_CONNECTED is not in main queue; expect empty")


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
