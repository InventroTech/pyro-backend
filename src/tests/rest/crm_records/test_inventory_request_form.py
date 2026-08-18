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

    def _patch_request(self, record, data, user=None):
        self.client.force_login(user or self.user)
        return self.client.patch(
            f"/crm-records/records/{record.id}/",
            {"data": data},
            format="json",
            HTTP_X_Tenant_Slug=self.tenant.slug,
        )

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
        from types import SimpleNamespace
        from crm_records.views import _notify_team_lead_for_inventory_request

        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="inventory_request",
            data={
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
        # Create notification goes to Team Lead only.
        self.assertEqual(emails, ["teamlead@example.com"])

    @patch("crm_records.views.send_email")
    def test_create_emails_team_lead_only_not_pm_or_requestor(self, mock_send_email):
        """Create emails go to Team Lead only (not PM and not requestor)."""
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
                # Intentionally inverted stored fields — backend should still find TL by role.
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
        self.assertEqual(emails, ["teamlead@example.com"])
        self.assertNotIn("pm@example.com", emails)
        self.assertNotIn("requester@example.com", emails)

    @patch("crm_records.views.send_email")
    def test_approve_status_emails_requestor_only(self, mock_send_email):
        """On Approve (VENDOR_IDENTIFIED), only the requestor is emailed — not Team Lead."""
        mock_send_email.return_value = (True, "ok")
        from types import SimpleNamespace
        from crm_records.views import _notify_request_status_emails

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
        _notify_request_status_emails(request, record, previous_status="NEW_REQUEST")

        emails = [c.kwargs.get("to_emails") for c in mock_send_email.call_args_list]
        self.assertEqual(emails, ["requester@example.com"])
        self.assertEqual(
            mock_send_email.call_args.kwargs.get("client_name"),
            "RequestApprovedNotification",
        )

    @patch("crm_records.views.send_email")
    def test_approve_does_not_email_team_lead_helper(self, mock_send_email):
        """Legacy approve helper no longer sends TL-only emails."""
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
                "team_lead": self.requester_membership.id,
            },
        )
        request = SimpleNamespace(
            tenant=self.tenant,
            user=self.user,
            build_absolute_uri=lambda path: f"https://example.com{path}",
        )
        _notify_on_manager_approved(request, record, previous_status="NEW_REQUEST")

        self.assertEqual(mock_send_email.call_count, 0)

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

    @patch("crm_records.views.send_email")
    def test_on_hold_emails_requestor(self, mock_send_email):
        mock_send_email.return_value = (True, "ok")
        from types import SimpleNamespace
        from crm_records.views import _notify_requester_when_on_hold

        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={
                "status": "ON_HOLD",
                "status_text": "ON_HOLD",
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
        _notify_requester_when_on_hold(request, record, previous_status="NEW_REQUEST")

        self.assertEqual(mock_send_email.call_count, 1)
        self.assertEqual(mock_send_email.call_args.kwargs.get("to_emails"), "requester@example.com")
        self.assertEqual(
            mock_send_email.call_args.kwargs.get("client_name"),
            "RequestOnHoldNotification",
        )

    @patch("crm_records.views.send_email")
    def test_order_status_emails_requestor_only(self, mock_send_email):
        """On Order (IN_SHIPPING), only the requestor is emailed — not PM or Team Lead."""
        mock_send_email.return_value = (True, "ok")
        from types import SimpleNamespace
        from crm_records.views import _notify_request_status_emails

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
                "status": "IN_SHIPPING",
                "status_text": "IN_SHIPPING",
                "requester_id": str(self.user.supabase_uid),
                "requester_name": "Test Requester",
                "item_name_freeform": "Drone",
                "team_lead": self.team_lead_membership.id,
                "manager": pm_membership.id,
            },
        )
        request = SimpleNamespace(
            tenant=self.tenant,
            user=self.team_lead_user,
            build_absolute_uri=lambda path: f"https://example.com{path}",
        )
        _notify_request_status_emails(request, record, previous_status="VENDOR_IDENTIFIED")

        emails = [c.kwargs.get("to_emails") for c in mock_send_email.call_args_list]
        self.assertEqual(emails, ["requester@example.com"])
        self.assertEqual(
            mock_send_email.call_args.kwargs.get("client_name"),
            "RequestOrderedNotification",
        )

    @patch("crm_records.views.send_email")
    def test_req_to_verify_emails_requestor(self, mock_send_email):
        """Send to requestor to verify emails the requestor with the verify template."""
        mock_send_email.return_value = (True, "ok")
        from types import SimpleNamespace
        from crm_records.views import _notify_request_status_emails

        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={
                "status": "REQ TO VERIFY",
                "status_text": "REQ TO VERIFY",
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
        _notify_request_status_emails(request, record, previous_status="NEW_REQUEST")

        self.assertEqual(mock_send_email.call_count, 1)
        self.assertEqual(mock_send_email.call_args.kwargs.get("to_emails"), "requester@example.com")
        self.assertEqual(
            mock_send_email.call_args.kwargs.get("client_name"),
            "RequestToVerifyNotification",
        )
        subject = mock_send_email.call_args.kwargs.get("subject", "")
        self.assertIn("verify", subject.lower())

    @patch("crm_records.views.send_email")
    def test_any_status_change_emails_requestor(self, mock_send_email):
        """Requester gets an email for every status change, including previously uncovered ones."""
        mock_send_email.return_value = (True, "ok")
        from types import SimpleNamespace
        from crm_records.views import _notify_request_status_emails

        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={
                "status": "IN_CART",
                "status_text": "IN_CART",
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
        _notify_request_status_emails(request, record, previous_status="IN_SHIPPING")

        self.assertEqual(mock_send_email.call_count, 1)
        self.assertEqual(mock_send_email.call_args.kwargs.get("to_emails"), "requester@example.com")
        self.assertEqual(
            mock_send_email.call_args.kwargs.get("client_name"),
            "RequestStatusChangedNotification",
        )
        subject = mock_send_email.call_args.kwargs.get("subject", "")
        self.assertIn("IN_CART", subject)

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
        created_id = response.data["id"]
        self.assertEqual(mock_send_email.call_count, 1)
        self.assertEqual(
            mock_send_email.call_args.kwargs.get("to_emails"),
            "teamlead@example.com",
        )
        team_lead_href = (
            "https://app.thepyro.ai/app/unmannd/pages/"
            "cca4ebe2-58b8-489c-a686-65559f2a58aa"
            f"?entity_type=unmannd_request&page=1&page_size=10&record_id={created_id}"
        )
        html_message = mock_send_email.call_args.kwargs.get("html_message", "")
        self.assertIn(team_lead_href, html_message)

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

    def _patch_request(self, record, data, user=None):
        self.client.force_login(user or self.user)
        return self.client.patch(
            f"/crm-records/records/{record.id}/",
            {"data": data},
            format="json",
            HTTP_X_Tenant_Slug=self.tenant.slug,
        )

    def test_requester_can_edit_new_request(self):
        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="inventory_request",
            data={
                "status": "NEW_REQUEST",
                "requester_id": str(self.user.supabase_uid),
                "item_name_freeform": "Mouse",
                "quantity_required": 1,
            },
        )
        response = self._patch_request(
            record,
            {**record.data, "item_name_freeform": "Wireless Mouse", "quantity_required": 2},
        )
        self.assertEqual(response.status_code, 200, response.data)
        record.refresh_from_db()
        self.assertEqual(record.data["item_name_freeform"], "Wireless Mouse")
        self.assertEqual(record.data["quantity_required"], 2)

    def test_requester_cannot_edit_after_approval(self):
        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="inventory_request",
            data={
                "status": "VENDOR_IDENTIFIED",
                "requester_id": str(self.user.supabase_uid),
                "item_name_freeform": "Mouse",
                "quantity_required": 1,
            },
        )
        response = self._patch_request(
            record,
            {**record.data, "item_name_freeform": "Should not save"},
        )
        self.assertEqual(response.status_code, 403)
        record.refresh_from_db()
        self.assertEqual(record.data["item_name_freeform"], "Mouse")

    def test_team_lead_can_edit_approved_request(self):
        record = Record.objects.create(
            tenant=self.tenant,
            entity_type="unmannd_request",
            data={
                "status": "VENDOR_IDENTIFIED",
                "requester_id": str(self.user.supabase_uid),
                "item_name_freeform": "Drone",
                "quantity_required": 1,
                "team_lead": self.team_lead_membership.id,
            },
        )
        response = self._patch_request(
            record,
            {**record.data, "status": "IN_SHIPPING"},
            user=self.team_lead_user,
        )
        self.assertEqual(response.status_code, 200, response.data)
        record.refresh_from_db()
        self.assertEqual(record.data["status"], "IN_SHIPPING")
