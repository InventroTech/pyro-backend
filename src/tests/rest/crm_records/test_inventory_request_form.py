"""
Tests for inventory request form backend: creating records with entity_type=inventory_request
and full form payload (department, vendor, product_link, urgency_level, etc.).
"""
import uuid

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
        TenantMembership.objects.create(
            tenant=self.tenant,
            user_id=self.user.supabase_uid,
            email=self.user.email,
            role=role,
            is_active=True,
        )
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
                "status": "DRAFT",
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
        self.assertEqual(data["status"], "DRAFT")
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

    def test_create_inventory_request_with_empty_optional_fields(self):
        """Optional fields can be empty string; record still created."""
        payload = {
            "entity_type": "inventory_request",
            "data": {
                "status": "DRAFT",
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
