import uuid
import time
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth import get_user_model
from core.models import Tenant
from crm_records.models import Record
from crm_records.serializers import RecordSerializer

User = get_user_model()


class RecordApiTests(TestCase):
    def setUp(self):
        # Create two tenants
        self.tenant_a = Tenant.objects.create(id=uuid.uuid4(), name="Tenant A", slug="tenant-a")
        self.tenant_b = Tenant.objects.create(id=uuid.uuid4(), name="Tenant B", slug="tenant-b")

        # Create users (authenticated)
        self.user_a = User.objects.create_user(
            email="a@x.com", 
            password="pass1234",
            supabase_uid=str(uuid.uuid4())
        )
        self.user_b = User.objects.create_user(
            email="b@x.com", 
            password="pass1234",
            supabase_uid=str(uuid.uuid4())
        )

        # Create client
        self.client = Client()

        # Create some records for each tenant
        self.rec_a1 = Record.objects.create(
            tenant=self.tenant_a, 
            entity_type="lead", 
            data={"name": "Lead A1", "status": "new", "email": "lead1@tenant-a.com"}
        )
        self.rec_a2 = Record.objects.create(
            tenant=self.tenant_a, 
            entity_type="ticket", 
            data={"name": "Ticket A1", "priority": "high", "status": "open"}
        )
        self.rec_b1 = Record.objects.create(
            tenant=self.tenant_b, 
            entity_type="lead", 
            data={"name": "Lead B1", "status": "new", "email": "lead1@tenant-b.com"}
        )

        self.list_url = "/crm-records/records/"

    def authenticate_and_set_tenant(self, user, tenant):
        """Helper to authenticate user and set tenant header"""
        self.client.force_login(user)
        return {"HTTP_X_Tenant_Slug": tenant.slug}

    def test_create_record_returns_201(self):
        """Test: Create Record → returns 201"""
        start_time = time.time()
        
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        payload = {
            "entity_type": "ticket",
            "data": {"name": "New Ticket", "priority": "low", "status": "open", "description": "Test ticket"}
        }
        
        response = self.client.post(self.list_url, payload, format="json", **headers)
        
        # Verify response
        self.assertEqual(response.status_code, 201)
        self.assertIn("id", response.data)
        self.assertEqual(response.data["entity_type"], "ticket")
        self.assertEqual(response.data["data"]["name"], "New Ticket")
        
        # Verify database
        record = Record.objects.get(id=response.data["id"])
        self.assertEqual(record.tenant_id, self.tenant_a.id)
        self.assertEqual(record.data["priority"], "low")
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Create operation took {elapsed:.3f}s, expected < 0.3s")

    def test_get_record_list_shows_only_tenant_records(self):
        """Test: Get Record list → shows only tenant's records"""
        start_time = time.time()
        
        # Test Tenant A
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        response = self.client.get(self.list_url, **headers)
        self.assertEqual(response.status_code, 200)
        
        # Check pagination structure
        self.assertIn("data", response.data)
        self.assertIn("page_meta", response.data)
        
        # Check tenant isolation
        names = [r["data"]["name"] for r in response.data["data"]]
        self.assertIn("Lead A1", names)
        self.assertIn("Ticket A1", names)
        self.assertNotIn("Lead B1", names)  # Tenant B's record should not appear
        
        # Test Tenant B
        self.client.force_login(self.user_b)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_b.slug}
        
        response = self.client.get(self.list_url, **headers)
        self.assertEqual(response.status_code, 200)
        
        names = [r["data"]["name"] for r in response.data["data"]]
        self.assertIn("Lead B1", names)
        self.assertNotIn("Lead A1", names)  # Tenant A's records should not appear
        self.assertNotIn("Ticket A1", names)
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"List operation took {elapsed:.3f}s, expected < 0.3s")

    def test_update_record_json_field_modifies_correctly(self):
        """Test: Update Record → JSON field modifies correctly"""
        start_time = time.time()
        
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        detail_url = f"/crm-records/records/{self.rec_a1.id}/"
        
        # Update JSON data
        update_data = {
            "data": {
                "name": "Updated Lead A1",
                "status": "contacted",
                "email": "lead1@tenant-a.com",
                "phone": "+1234567890",
                "notes": "Called customer, interested in product"
            }
        }
        
        response = self.client.patch(detail_url, update_data, format="json", **headers)
        
        # Verify response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["data"]["name"], "Updated Lead A1")
        self.assertEqual(response.data["data"]["status"], "contacted")
        self.assertEqual(response.data["data"]["phone"], "+1234567890")
        
        # Verify database
        self.rec_a1.refresh_from_db()
        self.assertEqual(self.rec_a1.data["name"], "Updated Lead A1")
        self.assertEqual(self.rec_a1.data["status"], "contacted")
        self.assertEqual(self.rec_a1.data["phone"], "+1234567890")
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Update operation took {elapsed:.3f}s, expected < 0.3s")

    def test_tenant_b_cannot_access_tenant_a_record_403(self):
        """Test: Tenant B cannot access Tenant A record (403)"""
        start_time = time.time()
        
        # Try to access Tenant A's record as Tenant B
        self.client.force_login(self.user_b)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_b.slug}
        
        detail_url = f"/crm-records/records/{self.rec_a1.id}/"
        
        # GET should return 403
        response = self.client.get(detail_url, **headers)
        self.assertEqual(response.status_code, 403)
        
        # PATCH should return 403
        response = self.client.patch(detail_url, {"data": {"name": "Hacked"}}, format="json", **headers)
        self.assertEqual(response.status_code, 403)
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Security check took {elapsed:.3f}s, expected < 0.3s")

    def test_swagger_schema_loads_successfully(self):
        """Test: Swagger schema loads successfully"""
        start_time = time.time()
        
        # Test schema endpoint
        response = self.client.get("/api/schema/")
        self.assertEqual(response.status_code, 200)
        
        # Check that crm_records endpoints are in schema
        schema_content = response.content.decode('utf-8')
        self.assertIn("/crm-records/records/", schema_content)
        self.assertIn("crm_records_records_list", schema_content)
        self.assertIn("crm_records_records_create", schema_content)
        self.assertIn("Record", schema_content)
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Schema load took {elapsed:.3f}s, expected < 0.3s")

    def test_tenant_isolation_comprehensive(self):
        """Comprehensive test of tenant isolation"""
        # Create additional records for thorough testing
        rec_a3 = Record.objects.create(
            tenant=self.tenant_a,
            entity_type="contact",
            data={"name": "Contact A3", "email": "contact3@tenant-a.com"}
        )
        
        rec_b2 = Record.objects.create(
            tenant=self.tenant_b,
            entity_type="ticket",
            data={"name": "Ticket B2", "priority": "medium"}
        )
        
        # Test Tenant A can only see their records
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        response = self.client.get(self.list_url, **headers)
        self.assertEqual(response.status_code, 200)
        
        tenant_a_records = [r["data"]["name"] for r in response.data["data"]]
        self.assertEqual(len(tenant_a_records), 3)  # A1, A2, A3
        self.assertIn("Lead A1", tenant_a_records)
        self.assertIn("Ticket A1", tenant_a_records)
        self.assertIn("Contact A3", tenant_a_records)
        self.assertNotIn("Lead B1", tenant_a_records)
        self.assertNotIn("Ticket B2", tenant_a_records)
        
        # Test Tenant B can only see their records
        self.client.force_login(self.user_b)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_b.slug}
        
        response = self.client.get(self.list_url, **headers)
        self.assertEqual(response.status_code, 200)
        
        tenant_b_records = [r["data"]["name"] for r in response.data["data"]]
        self.assertEqual(len(tenant_b_records), 2)  # B1, B2
        self.assertIn("Lead B1", tenant_b_records)
        self.assertIn("Ticket B2", tenant_b_records)
        self.assertNotIn("Lead A1", tenant_b_records)
        self.assertNotIn("Ticket A1", tenant_b_records)
        self.assertNotIn("Contact A3", tenant_b_records)

    def test_serializer_validation(self):
        """Test serializer validation and field constraints"""
        # Test valid data
        valid_data = {
            "entity_type": "lead",
            "data": {"name": "Valid Lead", "email": "valid@example.com", "status": "new"}
        }
        
        serializer = RecordSerializer(data=valid_data)
        self.assertTrue(serializer.is_valid())
        
        # Test invalid entity_type (empty)
        invalid_data = {
            "entity_type": "",
            "data": {"name": "Invalid Lead", "email": "invalid@example.com"}
        }
        
        serializer = RecordSerializer(data=invalid_data)
        self.assertFalse(serializer.is_valid())
        self.assertIn("entity_type", serializer.errors)
        
        # Test invalid data (not dict)
        invalid_data = {
            "entity_type": "lead",
            "data": "not_a_dict"
        }
        
        serializer = RecordSerializer(data=invalid_data)
        self.assertFalse(serializer.is_valid())
        self.assertIn("data", serializer.errors)
