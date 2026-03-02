"""
Unit tests for entity API (PrajaLeadsAPIView).
Tests CRUD operations via /entity/ endpoint with X-Secret-Pyro authentication.
"""

import uuid
import time
from django.test import TestCase, Client, override_settings
from django.contrib.auth import get_user_model
from core.models import Tenant
from crm_records.models import Record

User = get_user_model()


@override_settings(
    PYRO_SECRET="test-pyro-secret-123",
    DEFAULT_TENANT_SLUG="test-tenant"
)
class EntityApiTests(TestCase):
    def setUp(self):
        # Create default tenant (used by entity API)
        self.default_tenant = Tenant.objects.create(
            id=uuid.uuid4(),
            name="Test Tenant",
            slug="test-tenant"
        )
        
        # Create another tenant (for isolation testing)
        self.other_tenant = Tenant.objects.create(
            id=uuid.uuid4(),
            name="Other Tenant",
            slug="other-tenant"
        )
        
        # Create client
        self.client = Client()
        
        # API endpoint
        self.entity_url = "/entity/"
        
        # Valid headers for authentication (Django test client converts to HTTP_X_SECRET_PYRO in META)
        self.valid_headers = {"HTTP_X_SECRET_PYRO": "test-pyro-secret-123"}
        
        # Create some existing records for testing
        self.existing_record = Record.objects.create(
            tenant=self.default_tenant,
            entity_type="lead",
            data={
                "name": "Existing Lead",
                "praja_id": "PRAJA001",
                "phone_number": "+1234567890",
                "lead_score": 80,
                "lead_stage": "in_queue",
                "poster": "free"
            }
        )

    def test_create_lead_returns_201(self):
        """Test: POST /entity/ → Create lead → returns 201"""
        start_time = time.time()
        
        payload = {
            "name": "New Lead",
            "data": {
                "praja_id": "PRAJA123",
                "phone_number": "+1234567890",
                "lead_score": 85,
                "lead_stage": "in_queue",
                "poster": "free"
            }
        }
        
        response = self.client.post(
            self.entity_url,
            payload,
            format="json",
            **self.valid_headers
        )
        
        # Verify response
        self.assertEqual(response.status_code, 201)
        self.assertIn("id", response.data)
        self.assertEqual(response.data["entity_type"], "lead")
        self.assertEqual(response.data["data"]["name"], "New Lead")
        self.assertEqual(response.data["data"]["praja_id"], "PRAJA123")
        
        # Verify database
        record = Record.objects.get(id=response.data["id"])
        self.assertEqual(record.tenant_id, self.default_tenant.id)
        self.assertEqual(record.data["praja_id"], "PRAJA123")
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Create operation took {elapsed:.3f}s, expected < 0.3s")

    def test_create_lead_without_secret_returns_403(self):
        """Test: POST /entity/ without X-Secret-Pyro → returns 403"""
        payload = {
            "name": "New Lead",
            "data": {"praja_id": "PRAJA123"}
        }
        
        response = self.client.post(
            self.entity_url,
            payload,
            format="json"
        )
        
        self.assertEqual(response.status_code, 403)

    def test_create_lead_with_invalid_secret_returns_403(self):
        """Test: POST /entity/ with invalid secret → returns 403"""
        payload = {
            "name": "New Lead",
            "data": {"praja_id": "PRAJA123"}
        }
        
        invalid_headers = {"HTTP_X_SECRET_PYRO": "wrong-secret"}
        response = self.client.post(
            self.entity_url,
            payload,
            format="json",
            **invalid_headers
        )
        
        self.assertEqual(response.status_code, 403)

    def test_get_all_leads_returns_200(self):
        """Test: GET /entity/ → Get all leads → returns 200"""
        start_time = time.time()
        
        # Create additional records
        Record.objects.create(
            tenant=self.default_tenant,
            entity_type="lead",
            data={"name": "Lead 2", "praja_id": "PRAJA002", "lead_stage": "assigned"}
        )
        
        response = self.client.get(self.entity_url, **self.valid_headers)
        
        # Verify response
        self.assertEqual(response.status_code, 200)
        self.assertIn("results", response.data)
        self.assertIn("count", response.data)
        self.assertGreaterEqual(response.data["count"], 2)
        
        # Verify all records belong to default tenant
        for record in response.data["results"]:
            self.assertEqual(record["entity_type"], "lead")
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"List operation took {elapsed:.3f}s, expected < 0.3s")

    def test_get_lead_by_record_id_returns_200(self):
        """Test: GET /entity/?record_id=X → Get specific lead → returns 200"""
        start_time = time.time()
        
        response = self.client.get(
            f"{self.entity_url}?record_id={self.existing_record.id}",
            **self.valid_headers
        )
        
        # Verify response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["id"], self.existing_record.id)
        self.assertEqual(response.data["data"]["name"], "Existing Lead")
        self.assertEqual(response.data["data"]["praja_id"], "PRAJA001")
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Get by ID operation took {elapsed:.3f}s, expected < 0.3s")

    def test_get_lead_by_lead_id_returns_200(self):
        """Test: GET /entity/?lead_id=X → Get specific lead → returns 200"""
        response = self.client.get(
            f"{self.entity_url}?lead_id={self.existing_record.id}",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["id"], self.existing_record.id)

    def test_get_nonexistent_record_returns_404(self):
        """Test: GET /entity/?record_id=99999 → returns 404"""
        response = self.client.get(
            f"{self.entity_url}?record_id=99999",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 404)
        self.assertIn("error", response.data)

    def test_get_leads_filtered_by_lead_stage_returns_200(self):
        """Test: GET /entity/?lead_stage=X → Filter by lead_stage → returns 200"""
        # Create records with different stages
        Record.objects.create(
            tenant=self.default_tenant,
            entity_type="lead",
            data={"name": "Assigned Lead", "praja_id": "PRAJA003", "lead_stage": "assigned"}
        )
        
        response = self.client.get(
            f"{self.entity_url}?lead_stage=assigned",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 200)
        # All returned leads should have lead_stage=assigned
        for record in response.data["results"]:
            self.assertEqual(record["data"]["lead_stage"], "assigned")

    def test_get_leads_filtered_by_poster_returns_200(self):
        """Test: GET /entity/?poster=X → Filter by poster → returns 200"""
        # Create records with different posters
        Record.objects.create(
            tenant=self.default_tenant,
            entity_type="lead",
            data={"name": "Premium Lead", "praja_id": "PRAJA004", "poster": "premium"}
        )
        
        response = self.client.get(
            f"{self.entity_url}?poster=premium",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 200)
        # All returned leads should have poster=premium
        for record in response.data["results"]:
            self.assertEqual(record["data"]["poster"], "premium")

    def test_patch_update_lead_returns_200(self):
        """Test: PATCH /entity/?praja_id=X → Update lead → returns 200"""
        start_time = time.time()
        
        update_data = {
            "lead_score": 95,
            "lead_stage": "assigned",
            "data": {
                "lead_score": 95,
                "lead_stage": "assigned",
                "latest_remarks": "Updated via PATCH"
            }
        }
        
        response = self.client.patch(
            f"{self.entity_url}?praja_id=PRAJA001",
            update_data,
            format="json",
            **self.valid_headers
        )
        
        # Verify response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["data"]["lead_score"], 95)
        self.assertEqual(response.data["data"]["lead_stage"], "assigned")
        self.assertEqual(response.data["data"]["latest_remarks"], "Updated via PATCH")
        
        # Verify database
        self.existing_record.refresh_from_db()
        self.assertEqual(self.existing_record.data["lead_score"], 95)
        self.assertEqual(self.existing_record.data["lead_stage"], "assigned")
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"PATCH operation took {elapsed:.3f}s, expected < 0.3s")

    def test_patch_update_lead_with_praja_id_in_body_returns_200(self):
        """Test: PATCH /entity/ with praja_id in body → Update lead → returns 200"""
        update_data = {
            "praja_id": "PRAJA001",
            "lead_score": 90,
            "data": {
                "lead_score": 90
            }
        }
        
        response = self.client.patch(
            self.entity_url,
            update_data,
            format="json",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["data"]["lead_score"], 90)

    def test_patch_update_lead_without_praja_id_returns_400(self):
        """Test: PATCH /entity/ without praja_id → returns 400"""
        update_data = {
            "lead_score": 90
        }
        
        response = self.client.patch(
            self.entity_url,
            update_data,
            format="json",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.data)
        self.assertIn("praja_id is required", response.data["error"])

    def test_patch_update_nonexistent_lead_returns_404(self):
        """Test: PATCH /entity/?praja_id=INVALID → returns 404"""
        update_data = {
            "lead_score": 90
        }
        
        response = self.client.patch(
            f"{self.entity_url}?praja_id=INVALID",
            update_data,
            format="json",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 404)
        self.assertIn("error", response.data)

    def test_patch_update_task_returns_200(self):
        """Test: PATCH /entity/ with update_task → Update specific task → returns 200"""
        # First, create a record with tasks
        record = Record.objects.create(
            tenant=self.default_tenant,
            entity_type="lead",
            data={
                "name": "Lead with Tasks",
                "praja_id": "PRAJA005",
                "tasks": [
                    {"task": "call", "status": "pending"},
                    {"task": "email", "status": "pending"}
                ]
            }
        )
        
        update_data = {
            "praja_id": "PRAJA005",
            "update_task": {
                "task_name": "call",
                "status": "completed"
            }
        }
        
        response = self.client.patch(
            self.entity_url,
            update_data,
            format="json",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 200)
        # Find the updated task
        tasks = response.data["data"]["tasks"]
        call_task = next((t for t in tasks if t["task"] == "call"), None)
        self.assertIsNotNone(call_task)
        self.assertEqual(call_task["status"], "completed")

    def test_put_full_replacement_returns_200(self):
        """Test: PUT /entity/?praja_id=X → Full replacement → returns 200"""
        start_time = time.time()
        
        replacement_data = {
            "praja_id": "PRAJA001",
            "name": "Updated Name",
            "data": {
                "praja_id": "PRAJA001",
                "phone_number": "+9876543210",
                "lead_score": 100,
                "lead_stage": "won",
                "poster": "premium",
                "latest_remarks": "Fully replaced"
            }
        }
        
        response = self.client.put(
            f"{self.entity_url}?praja_id=PRAJA001",
            replacement_data,
            format="json",
            **self.valid_headers
        )
        
        # Verify response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["data"]["name"], "Updated Name")
        self.assertEqual(response.data["data"]["lead_score"], 100)
        self.assertEqual(response.data["data"]["lead_stage"], "won")
        self.assertEqual(response.data["data"]["latest_remarks"], "Fully replaced")
        
        # Verify database - full replacement occurred
        self.existing_record.refresh_from_db()
        self.assertEqual(self.existing_record.data["lead_score"], 100)
        self.assertEqual(self.existing_record.data["lead_stage"], "won")
        self.assertEqual(self.existing_record.data["latest_remarks"], "Fully replaced")
        # Verify old field value was replaced
        self.assertEqual(self.existing_record.data["poster"], "premium")
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"PUT operation took {elapsed:.3f}s, expected < 0.3s")

    def test_put_without_data_object_returns_400(self):
        """Test: PUT /entity/ without data object → returns 400"""
        replacement_data = {
            "praja_id": "PRAJA001",
            "name": "Updated Name"
        }
        
        response = self.client.put(
            f"{self.entity_url}?praja_id=PRAJA001",
            replacement_data,
            format="json",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.data)
        self.assertIn("PUT requires a complete", response.data["error"])

    def test_put_with_different_entity_type_returns_200(self):
        """Test: PUT /entity/?entity=ticket → Update ticket → returns 200"""
        # Create a ticket
        ticket = Record.objects.create(
            tenant=self.default_tenant,
            entity_type="ticket",
            data={"name": "Test Ticket", "praja_id": "TICKET001", "status": "open"}
        )
        
        replacement_data = {
            "praja_id": "TICKET001",
            "name": "Updated Ticket",
            "data": {
                "praja_id": "TICKET001",
                "status": "closed",
                "resolution": "Fixed"
            }
        }
        
        response = self.client.put(
            f"{self.entity_url}?praja_id=TICKET001&entity=ticket",
            replacement_data,
            format="json",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["entity_type"], "ticket")
        self.assertEqual(response.data["data"]["status"], "closed")

    def test_delete_lead_returns_200(self):
        """Test: DELETE /entity/?praja_id=X → Delete lead → returns 200"""
        start_time = time.time()
        
        # Create a record to delete
        record_to_delete = Record.objects.create(
            tenant=self.default_tenant,
            entity_type="lead",
            data={"name": "To Delete", "praja_id": "PRAJA_DELETE"}
        )
        
        response = self.client.delete(
            f"{self.entity_url}?praja_id=PRAJA_DELETE",
            **self.valid_headers
        )
        
        # Verify response
        self.assertEqual(response.status_code, 200)
        self.assertIn("message", response.data)
        self.assertIn("deleted successfully", response.data["message"])
        
        # Verify database - record should be deleted
        self.assertFalse(Record.objects.filter(id=record_to_delete.id).exists())
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"DELETE operation took {elapsed:.3f}s, expected < 0.3s")

    def test_delete_lead_with_praja_id_in_body_returns_200(self):
        """Test: DELETE /entity/ with praja_id in body → Delete lead → returns 200"""
        record_to_delete = Record.objects.create(
            tenant=self.default_tenant,
            entity_type="lead",
            data={"name": "To Delete 2", "praja_id": "PRAJA_DELETE2"}
        )
        
        delete_data = {"praja_id": "PRAJA_DELETE2"}
        
        response = self.client.delete(
            self.entity_url,
            delete_data,
            format="json",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertFalse(Record.objects.filter(id=record_to_delete.id).exists())

    def test_delete_lead_without_praja_id_returns_400(self):
        """Test: DELETE /entity/ without praja_id → returns 400"""
        response = self.client.delete(
            self.entity_url,
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.data)
        self.assertIn("praja_id is required", response.data["error"])

    def test_delete_nonexistent_lead_returns_404(self):
        """Test: DELETE /entity/?praja_id=INVALID → returns 404"""
        response = self.client.delete(
            f"{self.entity_url}?praja_id=INVALID",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 404)
        self.assertIn("error", response.data)

    def test_create_with_different_entity_type_returns_201(self):
        """Test: POST /entity/?entity=ticket → Create ticket → returns 201"""
        payload = {
            "name": "New Ticket",
            "data": {
                "praja_id": "TICKET123",
                "status": "open",
                "priority": "high"
            }
        }
        
        response = self.client.post(
            f"{self.entity_url}?entity=ticket",
            payload,
            format="json",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["entity_type"], "ticket")
        self.assertEqual(response.data["data"]["praja_id"], "TICKET123")

    def test_get_with_different_entity_type_returns_200(self):
        """Test: GET /entity/?entity=ticket → Get tickets → returns 200"""
        # Create tickets
        Record.objects.create(
            tenant=self.default_tenant,
            entity_type="ticket",
            data={"name": "Ticket 1", "praja_id": "TICKET001", "status": "open"}
        )
        
        response = self.client.get(
            f"{self.entity_url}?entity=ticket",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 200)
        # All returned records should be tickets
        for record in response.data["results"]:
            self.assertEqual(record["entity_type"], "ticket")

    def test_pagination_returns_200(self):
        """Test: GET /entity/?page=1&page_size=2 → Pagination works → returns 200"""
        # Create multiple records
        for i in range(5):
            Record.objects.create(
                tenant=self.default_tenant,
                entity_type="lead",
                data={"name": f"Lead {i}", "praja_id": f"PRAJA{i:03d}"}
            )
        
        response = self.client.get(
            f"{self.entity_url}?page=1&page_size=2",
            **self.valid_headers
        )
        
        self.assertEqual(response.status_code, 200)
        # Should have pagination structure
        self.assertIn("results", response.data)
        self.assertIn("count", response.data)
        # Results should be limited to page_size
        self.assertLessEqual(len(response.data["results"]), 2)

    def test_tenant_isolation_only_default_tenant_records_returned(self):
        """Test: GET /entity/ → Only default tenant records returned"""
        # Create record for other tenant
        Record.objects.create(
            tenant=self.other_tenant,
            entity_type="lead",
            data={"name": "Other Tenant Lead", "praja_id": "OTHER001"}
        )
        
        response = self.client.get(self.entity_url, **self.valid_headers)
        
        self.assertEqual(response.status_code, 200)
        # Should not include other tenant's records
        praja_ids = [r["data"].get("praja_id") for r in response.data["results"]]
        self.assertNotIn("OTHER001", praja_ids)

    def test_comprehensive_crud_flow(self):
        """Test: Complete CRUD flow"""
        start_time = time.time()
        
        # CREATE
        create_payload = {
            "name": "CRUD Test Lead",
            "data": {
                "praja_id": "CRUD001",
                "phone_number": "+1234567890",
                "lead_score": 50,
                "lead_stage": "in_queue"
            }
        }
        
        create_response = self.client.post(
            self.entity_url,
            create_payload,
            format="json",
            **self.valid_headers
        )
        self.assertEqual(create_response.status_code, 201)
        created_id = create_response.data["id"]
        praja_id = create_response.data["data"]["praja_id"]
        
        # READ
        read_response = self.client.get(
            f"{self.entity_url}?record_id={created_id}",
            **self.valid_headers
        )
        self.assertEqual(read_response.status_code, 200)
        self.assertEqual(read_response.data["data"]["name"], "CRUD Test Lead")
        
        # UPDATE (PATCH)
        patch_data = {
            "lead_score": 75,
            "data": {"lead_score": 75, "lead_stage": "assigned"}
        }
        patch_response = self.client.patch(
            f"{self.entity_url}?praja_id={praja_id}",
            patch_data,
            format="json",
            **self.valid_headers
        )
        self.assertEqual(patch_response.status_code, 200)
        self.assertEqual(patch_response.data["data"]["lead_score"], 75)
        
        # DELETE
        delete_response = self.client.delete(
            f"{self.entity_url}?praja_id={praja_id}",
            **self.valid_headers
        )
        self.assertEqual(delete_response.status_code, 200)
        
        # Verify deletion
        verify_response = self.client.get(
            f"{self.entity_url}?record_id={created_id}",
            **self.valid_headers
        )
        self.assertEqual(verify_response.status_code, 404)
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.5, f"Complete CRUD flow took {elapsed:.3f}s, expected < 0.5s")

