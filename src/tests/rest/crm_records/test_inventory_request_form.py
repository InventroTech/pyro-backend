"""
Tests for inventory request form backend: creating records with entity_type=inventory_request
and full form payload (department, vendor, product_link, urgency_level, etc.).
"""
import os
import uuid
from unittest.mock import patch

from django.core.cache import cache
from django.test import TestCase
from django.contrib.auth import get_user_model
from rest_framework.test import APIClient

from core.models import Tenant
from authz import service as authz_service
from authz.models import Role, TenantMembership
from crm_records.models import Record

User = get_user_model()


class InventoryRequestFormBackendTests(TestCase):
    """Test that inventory request form data is stored correctly via records API."""

    def setUp(self):
        # Clear authz permissions cache so this test's membership is used (avoids stale cache from prior test)
        authz_service._CACHE.clear()

        self.tenant = Tenant.objects.create(
            id=uuid.uuid4(),
            name="Test Tenant",
            slug=f"test-tenant-{uuid.uuid4().hex[:8]}",  # <--- THE FIX
        )
        # Clear tenant middleware cache so this test's tenant is resolved (avoids stale tenant from prior test)
        cache.delete(f"tenant:slug:{self.tenant.slug}")
        cache.delete(f"tenant:id:{self.tenant.id}")

        self.user = User.objects.create_user(
            email="requester@example.com",
            password="pass1234",
            supabase_uid=str(uuid.uuid4()),
        )
        # IsTenantAuthenticated requires an active TenantMembership for the user in this tenant
        role = Role.objects.create(
            tenant=self.tenant,
            key="AGENT",
            name="Agent",
        )
        self.requester_membership = TenantMembership.objects.create(
            tenant=self.tenant,
            user_id=self.user.supabase_uid,
            email=self.user.email,
            role=role,
            is_active=True,
        )
        self.team_lead_user = User.objects.create_user(
            email="teamlead@example.com",
            password="pass1234",
            supabase_uid=str(uuid.uuid4()),
        )
        self.team_lead_role = Role.objects.create(
            tenant=self.tenant,
            key="team_lead_unmannd",
            name="Team Lead",
        )
        self.team_lead_membership = TenantMembership.objects.create(
            tenant=self.tenant,
            user_id=self.team_lead_user.supabase_uid,
            email=self.team_lead_user.email,
            role=self.team_lead_role,
            is_active=True,
            name="Team Lead User",
        )
        self.requester_membership.user_parent_id = self.team_lead_membership
        self.requester_membership.save(update_fields=["user_parent_id"])
        self.client = APIClient()
        self.list_url = "/crm-records/records/"

    def _auth_headers(self):
        self.client.force_login(self.user)
        return {"HTTP_X_Tenant_Slug": self.tenant.slug}

    def test_create_inventory_request_stores_all_form_fields(self):
        """POST with entity_type=inventory_request stores full form data in record.data."""
        payload = {
            "entity_type": "inventory_request",
            "data": {
                "status": "NEW_REQUEST",
                "request_date": "2026-02-09",
                "requester_id": str(self.user.id),
                "requester_name": "Test Requester",
                "department": "Engineering",
                "item_name_freeform": "Laptop stand",
                "quantity_required": 2,
                "urgency_level": "HIGH",
                "comments": "Need by next week",
                "vendor": "Acme Corp",
                "product_link": "https://example.com/product/123",
                "additional_link": "https://example.com/spec",
            },
        }
        response = self.client.post(
            self.list_url,
            payload,
            format="json",
            **self._auth_headers(),
        )
        self.assertEqual(response.status_code, 201, response.data)
        self.assertIn("id", response.data)
        self.assertEqual(response.data["entity_type"], "inventory_request")

        data = response.data["data"]
        self.assertEqual(data["status"], "NEW_REQUEST")
        self.assertEqual(data["request_date"], "2026-02-09")
        self.assertEqual(data["requester_id"], str(self.user.id))
        self.assertEqual(data["requester_name"], "Test Requester")
        self.assertEqual(data["department"], "Engineering")
        self.assertEqual(data["item_name_freeform"], "Laptop stand")
        self.assertEqual(data["quantity_required"], 2)
        self.assertEqual(data["urgency_level"], "HIGH")
        self.assertEqual(data["comments"], "Need by next week")
        self.assertEqual(data["vendor"], "Acme Corp")
        self.assertEqual(data["product_link"], "https://example.com/product/123")
        self.assertEqual(data["additional_link"], "https://example.com/spec")

        record = Record.objects.get(id=response.data["id"])
        self.assertEqual(record.tenant_id, self.tenant.id)
        self.assertEqual(record.data["vendor"], "Acme Corp")
        self.assertEqual(record.data["department"], "Engineering")

    @patch("crm_records.views.send_email")
    def test_create_inventory_request_sends_email_to_team_lead(self, mock_send_email):
        mock_send_email.return_value = (True, "ok")
        payload = {
            "entity_type": "inventory_request",
            "data": {
                "status": "NEW_REQUEST",
                "status_text": "Submitted",
                "requester_id": str(self.user.supabase_uid),
                "requester_name": "Test Requester",
                "item_name_freeform": "Laptop stand",
                "team_lead": self.team_lead_membership.id,
            },
        )
        request = SimpleNamespace(
            tenant=self.tenant,
            user=self.user,
            build_absolute_uri=lambda path: f"https://example.com{path}",
        )
        _notify_team_lead_for_inventory_request(request, record)
        emails = [c.kwargs.get("to_emails") for c in mock_send_email.call_args_list]
        # No separate manager → team_lead used for both manager + team_lead slots (deduped) + requester
        self.assertIn("teamlead@example.com", emails)
        self.assertIn("requester@example.com", emails)

    @patch("crm_records.views.send_email")
    def test_create_emails_include_pm_manager_and_team_lead(self, mock_send_email):
        """Create emails go to PM (manager role), Team Lead role, and requestor."""
        mock_send_email.return_value = (True, "ok")
        from types import SimpleNamespace
        from crm_records.views import _notify_team_lead_for_inventory_request

        pm_role = Role.objects.create(
            tenant=self.tenant,
            key="PM",
            name="Procurement Manager",
        )
        pm_user = User.objects.create_user(
            email="pm@example.com",
            password="pass1234",
            supabase_uid=str(uuid.uuid4()),
        )
        pm_membership = TenantMembership.objects.create(
            tenant=self.tenant,
            user_id=pm_user.supabase_uid,
            email=pm_user.email,
            role=pm_role,
            is_active=True,
            name="PM User",
            user_parent_id=self.team_lead_membership,
        )
        self.requester_membership.user_parent_id = pm_membership
        self.requester_membership.save(update_fields=["user_parent_id"])

        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={
                "status": "NEW_REQUEST",
                "requester_id": str(self.user.supabase_uid),
                "requester_name": "Test Requester",
                "item_name_freeform": "Drone",
                # Intentionally inverted stored fields — backend should still find PM by role.
                "team_lead": pm_membership.id,
                "manager": self.team_lead_membership.id,
            },
        )
        request = SimpleNamespace(
            tenant=self.tenant,
            user=self.user,
            build_absolute_uri=lambda path: f"https://example.com{path}",
        )
        _notify_team_lead_for_inventory_request(request, record)
        emails = [c.kwargs.get("to_emails") for c in mock_send_email.call_args_list]
        self.assertIn("pm@example.com", emails)
        self.assertIn("teamlead@example.com", emails)
        self.assertIn("requester@example.com", emails)

    @patch("crm_records.views.send_email")
    def test_manager_approve_emails_requestor_and_team_lead(self, mock_send_email):
        mock_send_email.return_value = (True, "ok")
        from types import SimpleNamespace
        from crm_records.views import _notify_on_manager_approved

        manager_user = User.objects.create_user(
            email="manager@example.com",
            password="pass1234",
            supabase_uid=str(uuid.uuid4()),
        )
        manager_membership = TenantMembership.objects.create(
            tenant=self.tenant,
            user_id=manager_user.supabase_uid,
            email=manager_user.email,
            role=Role.objects.get(tenant=self.tenant, key="AGENT"),
            is_active=True,
            name="Manager User",
        )
        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={
                "status": "VENDOR_IDENTIFIED",
                "status_text": "VENDOR_IDENTIFIED",
                "requester_id": str(self.user.supabase_uid),
                "requester_name": "Test Requester",
                "item_name_freeform": "Drone",
                "team_lead": self.team_lead_membership.id,
                "manager": manager_membership.id,
            },
        )
        request = SimpleNamespace(
            tenant=self.tenant,
            user=manager_user,
            build_absolute_uri=lambda path: f"https://example.com{path}",
        )
        _notify_on_manager_approved(request, record, previous_status="NEW_REQUEST")

        emails = [c.kwargs.get("to_emails") for c in mock_send_email.call_args_list]
        self.assertIn("requester@example.com", emails)
        self.assertIn("teamlead@example.com", emails)
        self.assertNotIn("manager@example.com", emails)
        self.assertTrue(
            all(c.kwargs.get("client_name") == "RequestApprovedNotification" for c in mock_send_email.call_args_list)
        )

    @patch("crm_records.views.send_email")
    def test_manager_approve_emails_team_lead_when_field_is_requestor(self, mock_send_email):
        """If data.team_lead was wrongly saved as the requestor, still email real Team Lead."""
        mock_send_email.return_value = (True, "ok")
        from types import SimpleNamespace
        from crm_records.views import _notify_on_manager_approved

        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={
                "status": "VENDOR_IDENTIFIED",
                "status_text": "VENDOR_IDENTIFIED",
                "requester_id": str(self.user.supabase_uid),
                "requester_name": "Test Requester",
                "item_name_freeform": "Drone",
                # Bug case seen in prod: team_lead saved as requestor's own membership id.
                "team_lead": self.requester_membership.id,
            },
        )
        request = SimpleNamespace(
            tenant=self.tenant,
            user=self.user,
            build_absolute_uri=lambda path: f"https://example.com{path}",
        )
        _notify_on_manager_approved(request, record, previous_status="NEW_REQUEST")

        emails = [c.kwargs.get("to_emails") for c in mock_send_email.call_args_list]
        self.assertIn("requester@example.com", emails)
        self.assertIn("teamlead@example.com", emails)
        self.assertEqual(len(emails), 2)

    @patch("crm_records.views.send_email")
    def test_reject_emails_requestor(self, mock_send_email):
        mock_send_email.return_value = (True, "ok")
        from types import SimpleNamespace
        from crm_records.views import _notify_requester_when_rejected

        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={
                "status": "REJECTED",
                "status_text": "REJECTED",
                "requester_id": str(self.user.supabase_uid),
                "requester_name": "Test Requester",
                "item_name_freeform": "Drone",
                "team_lead": self.team_lead_membership.id,
            },
        )
        request = SimpleNamespace(
            tenant=self.tenant,
            user=self.user,
            build_absolute_uri=lambda path: f"https://example.com{path}",
        )
        _notify_requester_when_rejected(request, record, previous_status="NEW_REQUEST")

        self.assertEqual(mock_send_email.call_count, 1)
        self.assertEqual(mock_send_email.call_args.kwargs.get("to_emails"), "requester@example.com")
        self.assertEqual(
            mock_send_email.call_args.kwargs.get("client_name"),
            "RequestRejectedNotification",
        )

    @patch.dict(os.environ, {"PYRO_FRONTEND_URL": "https://app.thepyro.ai"}, clear=False)
    @patch("crm_records.views.send_email")
    def test_create_unmannd_request_sends_email_with_app_redirect(self, mock_send_email):
        mock_send_email.return_value = (True, "ok")
        payload = {
            "entity_type": "unmannd_request",
            "data": {
                "status": "NEW_REQUEST",
                "status_text": "New request submitted",
                "request_date": "2026-02-09",
                "requester_id": str(self.user.supabase_uid),
                "requester_name": "Test Requester",
                "department": "Engineering",
                "item_name_freeform": "Drone",
                "quantity_required": 1,
                "urgency_level": "CRITICAL",
                "team_lead": self.team_lead_membership.id,
            },
        }
        response = self.client.post(
            self.list_url,
            payload,
            format="json",
            **self._auth_headers(),
        )
        self.assertEqual(response.status_code, 201, response.data)
        html_message = mock_send_email.call_args_list[0].kwargs.get("html_message", "")
        self.assertIn(f"https://app.thepyro.ai/app/{self.tenant.slug}", html_message)

    def test_create_inventory_request_with_empty_optional_fields(self):
        """Optional fields can be empty string; record still created."""
        payload = {
            "entity_type": "inventory_request",
            "data": {
                "status": "NEW_REQUEST",
                "request_date": "2026-02-09",
                "requester_id": str(self.user.id),
                "requester_name": "User",
                "department": "",
                "item_name_freeform": "Desk lamp",
                "quantity_required": 1,
                "urgency_level": "",
                "comments": "",
                "vendor": "",
                "product_link": "",
                "additional_link": "",
            },
        }
        response = self.client.post(
            self.list_url,
            payload,
            format="json",
            **self._auth_headers(),
        )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["data"]["item_name_freeform"], "Desk lamp")
        self.assertEqual(response.data["data"]["quantity_required"], 1)
        self.assertEqual(response.data["data"]["vendor"], "")
        self.assertEqual(response.data["data"]["additional_link"], "")
