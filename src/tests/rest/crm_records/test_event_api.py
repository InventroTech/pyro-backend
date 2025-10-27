"""
Unit tests for crm_records event system.
Tests event logging, dispatching, and tenant isolation.
"""

import uuid
import time
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth import get_user_model
from core.models import Tenant
from crm_records.models import Record, EventLog
from crm_records.serializers import EventLogSerializer
from crm_records.events import dispatch_event, get_event_history, simulate_workflow_actions

User = get_user_model()


class EventApiTests(TestCase):
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
            name="Lead A1", 
            data={"status": "new", "email": "lead1@tenant-a.com"}
        )
        self.rec_a2 = Record.objects.create(
            tenant=self.tenant_a, 
            entity_type="ticket", 
            name="Ticket A1", 
            data={"priority": "high", "status": "open"}
        )
        self.rec_b1 = Record.objects.create(
            tenant=self.tenant_b, 
            entity_type="lead", 
            name="Lead B1", 
            data={"status": "new", "email": "lead1@tenant-b.com"}
        )

        self.event_url = f"/crm-records/records/{self.rec_a1.id}/events/"
        self.events_list_url = "/crm-records/events/"

    def authenticate_and_set_tenant(self, user, tenant):
        """Helper to authenticate user and set tenant header"""
        self.client.force_login(user)
        return {"HTTP_X_Tenant_Slug": tenant.slug}

    def test_create_event_returns_200(self):
        """Test: Create Event → returns 200"""
        start_time = time.time()
        
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        payload = {
            "event": "button_click",
            "payload": {
                "button_type": "call_later",
                "user_id": "user123",
                "timestamp": "2025-01-01T00:00:00Z"
            }
        }
        
        response = self.client.post(self.event_url, payload, format="json", **headers)
        
        # Verify response
        self.assertEqual(response.status_code, 200)
        self.assertIn("ok", response.data)
        self.assertIn("logged", response.data)
        self.assertTrue(response.data["ok"])
        self.assertTrue(response.data["logged"])
        self.assertIn("event_id", response.data)
        
        # Verify database
        event_log = EventLog.objects.get(id=response.data["event_id"])
        self.assertEqual(event_log.event, "button_click")
        self.assertEqual(event_log.record, self.rec_a1)
        self.assertEqual(event_log.tenant, self.tenant_a)
        self.assertEqual(event_log.payload["button_type"], "call_later")
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Create event operation took {elapsed:.3f}s, expected < 0.3s")

    def test_create_event_missing_event_name_returns_400(self):
        """Test: Create Event without event name → returns 400"""
        start_time = time.time()
        
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        payload = {
            "payload": {"button_type": "call_later"}
        }
        
        response = self.client.post(self.event_url, payload, format="json", **headers)
        
        # Verify response
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.data)
        self.assertIn("Event name is required", response.data["error"])
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Validation operation took {elapsed:.3f}s, expected < 0.3s")

    def test_create_event_invalid_payload_returns_400(self):
        """Test: Create Event with invalid payload → returns 400"""
        start_time = time.time()
        
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        payload = {
            "event": "button_click",
            "payload": "not_a_dict"
        }
        
        response = self.client.post(self.event_url, payload, format="json", **headers)
        
        # Verify response
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.data)
        self.assertIn("Payload must be a valid JSON object", response.data["error"])
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Validation operation took {elapsed:.3f}s, expected < 0.3s")

    def test_create_event_nonexistent_record_returns_404(self):
        """Test: Create Event for non-existent record → returns 404"""
        start_time = time.time()
        
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        payload = {
            "event": "button_click",
            "payload": {"button_type": "call_later"}
        }
        
        # Use non-existent record ID
        response = self.client.post("/crm-records/records/99999/events/", payload, format="json", **headers)
        
        # Verify response
        self.assertEqual(response.status_code, 404)
        self.assertIn("error", response.data)
        self.assertIn("Record not found or access denied", response.data["error"])
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"404 operation took {elapsed:.3f}s, expected < 0.3s")

    def test_tenant_b_cannot_access_tenant_a_record_404(self):
        """Test: Tenant B cannot access Tenant A record (404)"""
        start_time = time.time()
        
        # Try to access Tenant A's record as Tenant B
        self.client.force_login(self.user_b)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_b.slug}
        
        payload = {
            "event": "button_click",
            "payload": {"button_type": "call_later"}
        }
        
        response = self.client.post(self.event_url, payload, format="json", **headers)
        
        # Verify response
        self.assertEqual(response.status_code, 404)
        self.assertIn("error", response.data)
        self.assertIn("Record not found or access denied", response.data["error"])
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Security check took {elapsed:.3f}s, expected < 0.3s")

    def test_get_events_list_shows_only_tenant_events(self):
        """Test: Get Events list → shows only tenant's events"""
        start_time = time.time()
        
        # Create events for both tenants
        EventLog.objects.create(
            record=self.rec_a1,
            tenant=self.tenant_a,
            event="button_click",
            payload={"button_type": "call_later"}
        )
        EventLog.objects.create(
            record=self.rec_a2,
            tenant=self.tenant_a,
            event="win_clicked",
            payload={"button_type": "win"}
        )
        EventLog.objects.create(
            record=self.rec_b1,
            tenant=self.tenant_b,
            event="lost_clicked",
            payload={"button_type": "lost"}
        )
        
        # Test Tenant A
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        response = self.client.get(self.events_list_url, **headers)
        self.assertEqual(response.status_code, 200)
        
        # Check response structure
        self.assertIn("data", response.data)
        self.assertIn("page_meta", response.data)
        self.assertIn("summary", response.data)
        
        # Check tenant isolation
        events = response.data["data"]
        self.assertEqual(len(events), 2)  # Only Tenant A's events
        
        event_names = [e["event"] for e in events]
        self.assertIn("button_click", event_names)
        self.assertIn("win_clicked", event_names)
        self.assertNotIn("lost_clicked", event_names)  # Tenant B's event should not appear
        
        # Check summary
        summary = response.data["summary"]
        self.assertEqual(summary["total_events"], 2)
        self.assertIn("button_click", summary["event_counts"])
        self.assertIn("win_clicked", summary["event_counts"])
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"List operation took {elapsed:.3f}s, expected < 0.3s")

    def test_events_list_filtering_by_record(self):
        """Test: Events list filtering by record ID"""
        start_time = time.time()
        
        # Create events for different records
        EventLog.objects.create(
            record=self.rec_a1,
            tenant=self.tenant_a,
            event="button_click",
            payload={"button_type": "call_later"}
        )
        EventLog.objects.create(
            record=self.rec_a2,
            tenant=self.tenant_a,
            event="win_clicked",
            payload={"button_type": "win"}
        )
        
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        # Filter by record A1
        response = self.client.get(f"{self.events_list_url}?record={self.rec_a1.id}", **headers)
        self.assertEqual(response.status_code, 200)
        
        events = response.data["data"]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "button_click")
        self.assertEqual(events[0]["record_id"], self.rec_a1.id)
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Filtering operation took {elapsed:.3f}s, expected < 0.3s")

    def test_events_list_filtering_by_event_name(self):
        """Test: Events list filtering by event name"""
        start_time = time.time()
        
        # Create different event types
        EventLog.objects.create(
            record=self.rec_a1,
            tenant=self.tenant_a,
            event="button_click",
            payload={"button_type": "call_later"}
        )
        EventLog.objects.create(
            record=self.rec_a1,
            tenant=self.tenant_a,
            event="win_clicked",
            payload={"button_type": "win"}
        )
        EventLog.objects.create(
            record=self.rec_a1,
            tenant=self.tenant_a,
            event="button_click",
            payload={"button_type": "lost"}
        )
        
        self.client.force_login(self.user_a)
        headers = {"HTTP_X_Tenant_Slug": self.tenant_a.slug}
        
        # Filter by event name
        response = self.client.get(f"{self.events_list_url}?event=button_click", **headers)
        self.assertEqual(response.status_code, 200)
        
        events = response.data["data"]
        self.assertEqual(len(events), 2)  # Only button_click events
        
        for event in events:
            self.assertEqual(event["event"], "button_click")
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Filtering operation took {elapsed:.3f}s, expected < 0.3s")

    def test_event_dispatcher_stub_works(self):
        """Test: Event dispatcher stub works correctly"""
        start_time = time.time()
        
        # Test dispatch_event function
        result = dispatch_event("test_event", self.rec_a1, {"test": "data"})
        self.assertTrue(result)
        
        # Test get_event_history function
        # Create some events first
        EventLog.objects.create(
            record=self.rec_a1,
            tenant=self.tenant_a,
            event="test_event_1",
            payload={"test": "data1"}
        )
        EventLog.objects.create(
            record=self.rec_a1,
            tenant=self.tenant_a,
            event="test_event_2",
            payload={"test": "data2"}
        )
        
        # Test event history
        history = get_event_history(self.rec_a1)
        self.assertEqual(len(history), 2)
        
        # Test filtering by event name
        filtered_history = get_event_history(self.rec_a1, "test_event_1")
        self.assertEqual(len(filtered_history), 1)
        self.assertEqual(filtered_history[0].event, "test_event_1")
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Dispatcher operation took {elapsed:.3f}s, expected < 0.3s")

    def test_workflow_simulation_works(self):
        """Test: Workflow simulation works correctly"""
        start_time = time.time()
        
        # Test different workflow actions
        test_cases = [
            ("win_clicked", {"action": "update_fields", "updates": {"status": "won"}}),
            ("lost_clicked", {"action": "update_fields", "updates": {"status": "lost"}}),
            ("call_later_clicked", {"action": "update_fields", "updates": {"status": "call_later"}}),
            ("button_click", {"action": "log_event", "message": "Button clicked"})
        ]
        
        for event_name, expected_action in test_cases:
            action = simulate_workflow_actions(event_name, self.rec_a1, {"user_id": "user123"})
            self.assertEqual(action["action"], expected_action["action"])
            self.assertIn("message", action)
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Workflow simulation took {elapsed:.3f}s, expected < 0.3s")

    def test_event_serializer_validation(self):
        """Test: EventLogSerializer validation works correctly"""
        start_time = time.time()
        
        # Test valid data
        valid_data = {
            "event": "test_event",
            "payload": {"test": "data"}
        }
        
        serializer = EventLogSerializer(data=valid_data)
        self.assertTrue(serializer.is_valid())
        
        # Test invalid event (empty)
        invalid_data = {
            "event": "",
            "payload": {"test": "data"}
        }
        
        serializer = EventLogSerializer(data=invalid_data)
        self.assertFalse(serializer.is_valid())
        self.assertIn("event", serializer.errors)
        
        # Test invalid payload (not dict)
        invalid_data = {
            "event": "test_event",
            "payload": "not_a_dict"
        }
        
        serializer = EventLogSerializer(data=invalid_data)
        self.assertFalse(serializer.is_valid())
        self.assertIn("payload", serializer.errors)
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.3, f"Serializer validation took {elapsed:.3f}s, expected < 0.3s")

    def test_comprehensive_event_flow(self):
        """Test: Complete event flow from creation to listing"""
        start_time = time.time()
        
        # Step 1: Create multiple events
        events_to_create = [
            ("button_click", {"button_type": "call_later", "user_id": "user123"}),
            ("win_clicked", {"button_type": "win", "user_id": "user123"}),
            ("lost_clicked", {"button_type": "lost", "user_id": "user123"})
        ]
        
        created_events = []
        for event_name, payload in events_to_create:
            response = self.client.post(
                self.event_url,
                {"event": event_name, "payload": payload},
                format="json",
                **self.authenticate_and_set_tenant(self.user_a, self.tenant_a)
            )
            self.assertEqual(response.status_code, 200)
            created_events.append(response.data["event_id"])
        
        # Step 2: Verify events in database
        self.assertEqual(EventLog.objects.filter(record=self.rec_a1).count(), 3)
        
        # Step 3: List events and verify
        response = self.client.get(
            self.events_list_url,
            **self.authenticate_and_set_tenant(self.user_a, self.tenant_a)
        )
        self.assertEqual(response.status_code, 200)
        
        events = response.data["data"]
        self.assertEqual(len(events), 3)
        
        # Step 4: Test filtering
        response = self.client.get(
            f"{self.events_list_url}?event=button_click",
            **self.authenticate_and_set_tenant(self.user_a, self.tenant_a)
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data["data"]), 1)
        
        # Performance check
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.5, f"Complete flow took {elapsed:.3f}s, expected < 0.5s")

    def test_tenant_isolation_comprehensive(self):
        """Comprehensive test of tenant isolation for events"""
        # Create events for both tenants
        EventLog.objects.create(
            record=self.rec_a1,
            tenant=self.tenant_a,
            event="tenant_a_event",
            payload={"tenant": "A"}
        )
        EventLog.objects.create(
            record=self.rec_b1,
            tenant=self.tenant_b,
            event="tenant_b_event",
            payload={"tenant": "B"}
        )
        
        # Test Tenant A can only see their events
        response = self.client.get(
            self.events_list_url,
            **self.authenticate_and_set_tenant(self.user_a, self.tenant_a)
        )
        self.assertEqual(response.status_code, 200)
        
        tenant_a_events = [e["event"] for e in response.data["data"]]
        self.assertIn("tenant_a_event", tenant_a_events)
        self.assertNotIn("tenant_b_event", tenant_a_events)
        
        # Test Tenant B can only see their events
        response = self.client.get(
            self.events_list_url,
            **self.authenticate_and_set_tenant(self.user_b, self.tenant_b)
        )
        self.assertEqual(response.status_code, 200)
        
        tenant_b_events = [e["event"] for e in response.data["data"]]
        self.assertIn("tenant_b_event", tenant_b_events)
        self.assertNotIn("tenant_a_event", tenant_b_events)
