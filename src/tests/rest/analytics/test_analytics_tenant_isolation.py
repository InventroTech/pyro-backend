from datetime import timedelta

from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIRequestFactory

from analytics.utils import tenant_scoped_qs
from crm_records.models import Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from support_ticket.records import filter_records_by_tenant_param, support_ticket_records_qs
from tests.base.test_setup import BaseAPITestCase
from tests.factories import TenantFactory
from tests.factories.support_ticket_dump_factory import dump_data


class AnalyticsTenantIsolationHelpersTest(BaseAPITestCase):
    def test_tenant_scoped_qs_returns_empty_without_user_tenant_id(self):
        user = self.user
        user.tenant_id = None
        self.assertEqual(tenant_scoped_qs(user).count(), 0)

    def test_filter_records_by_tenant_param_scopes_to_request_tenant(self):
        other_tenant = TenantFactory()
        Record.objects.create(
            tenant=other_tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(user_id="other", name="Other Tenant"),
        )
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(user_id="mine", name="My Tenant"),
        )

        factory = APIRequestFactory()
        request = factory.get("/")
        request.tenant = self.tenant

        scoped = filter_records_by_tenant_param(
            support_ticket_records_qs(),
            request,
        )
        self.assertEqual(scoped.count(), 1)
        self.assertEqual(scoped.first().tenant_id, self.tenant.id)

    def test_filter_records_by_tenant_param_returns_empty_without_request_tenant(self):
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(user_id="mine", name="My Tenant"),
        )

        factory = APIRequestFactory()
        request = factory.get("/?tenant_id={}".format(self.tenant.id))

        scoped = filter_records_by_tenant_param(
            support_ticket_records_qs(),
            request,
        )
        self.assertEqual(scoped.count(), 0)


class DailyResolvedTicketsTenantIsolationAPITest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.url = reverse("analytics:daily-resolved-tickets")
        today = timezone.now()
        completed_at = today.isoformat()
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(user_id="mine", name="Mine"),
                "completed_at": completed_at,
                "resolution_status": "Resolved",
            },
        )
        other_tenant = TenantFactory()
        Record.objects.create(
            tenant=other_tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(user_id="other", name="Other"),
                "completed_at": completed_at,
                "resolution_status": "Resolved",
            },
        )

    def test_daily_resolved_requires_authentication(self):
        response = self.client.get(self.url)
        self.assertIn(
            response.status_code,
            (status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN),
        )

    def test_daily_resolved_counts_only_request_tenant(self):
        start = (timezone.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        end = (timezone.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        response = self.client.get(
            self.url,
            data={"start": start, "end": end},
            **self.auth_headers,
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        total = sum(point["y"] for point in response.data)
        self.assertEqual(total, 1)
