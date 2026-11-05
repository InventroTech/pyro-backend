"""Who may call make-closed, which methods are allowed, and tenant isolation."""

from rest_framework import status
from rest_framework.test import APIClient

from crm_records.models import EventLog, Record
from .helpers import MAKE_CLOSED_URL, MakeClosedAPITestCase, worked_lead_data
from tests.factories import TenantFactory


class MakeClosedAuthTests(MakeClosedAPITestCase):
    def test_rejects_a_request_without_login(self):
        lead = self.lead()
        anonymous = APIClient()

        response = anonymous.post(
            MAKE_CLOSED_URL, {"praja_ids": ["9001"]}, format="json", **self.make_unauthenticated_headers()
        )

        self.assertIn(response.status_code, (status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN))
        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "NOT_CONNECTED")

    def test_only_post_is_allowed(self):
        for method in ("get", "put", "patch", "delete"):
            with self.subTest(method=method):
                response = getattr(self.client, method)(MAKE_CLOSED_URL, format="json", **self.headers)
                self.assertEqual(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_records_the_caller_as_the_actor(self):
        lead = self.lead()

        self.post_ids(["9001"])

        event = EventLog.objects.get(record=lead, event="lead.made_closed")
        self.assertEqual(event.payload["actor"], self.email)


class MakeClosedTenantIsolationTests(MakeClosedAPITestCase):
    def setUp(self):
        super().setUp()
        self.other_tenant = TenantFactory()
        self.other_user, other_auth = self.make_authenticated_user(
            email="other-tenant@example.com", tenant=self.other_tenant
        )
        self.other_headers = {**other_auth, "HTTP_X_TENANT_SLUG": self.other_tenant.slug}

    def other_tenant_lead(self):
        return Record.objects.create(tenant=self.other_tenant, entity_type="lead", data=worked_lead_data())

    def test_does_not_close_another_tenants_lead(self):
        theirs = self.other_tenant_lead()

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["not_found"], ["9001"])
        theirs.refresh_from_db()
        self.assertEqual(theirs.data["lead_stage"], "NOT_CONNECTED")
        self.assertEqual(theirs.data["assigned_to"], "rm-123")

    def test_same_praja_id_in_two_tenants_closes_only_the_callers(self):
        mine = self.lead()
        theirs = self.other_tenant_lead()

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["updated"], ["9001"])
        mine.refresh_from_db()
        theirs.refresh_from_db()
        self.assertEqual(mine.data["lead_stage"], "CLOSED")
        self.assertEqual(theirs.data["lead_stage"], "NOT_CONNECTED")

    def test_other_tenant_closes_only_their_own_lead(self):
        mine = self.lead()
        theirs = self.other_tenant_lead()
        other_client = APIClient()
        other_client.force_authenticate(user=self.other_user)

        response = other_client.post(MAKE_CLOSED_URL, {"praja_ids": ["9001"]}, format="json", **self.other_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["updated"], ["9001"])
        mine.refresh_from_db()
        theirs.refresh_from_db()
        self.assertEqual(mine.data["lead_stage"], "NOT_CONNECTED")
        self.assertEqual(theirs.data["lead_stage"], "CLOSED")

    def test_event_is_logged_under_the_callers_tenant(self):
        self.lead()
        self.other_tenant_lead()

        self.post_ids(["9001"])

        events = EventLog.objects.filter(event="lead.made_closed")
        self.assertEqual(events.count(), 1)
        self.assertEqual(events.get().tenant_id, self.tenant.id)
