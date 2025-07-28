from tests.base.test_setup import BaseAPITestCase
from tests.base.assertions import DRFResponseAssertionsMixin
from django.urls import reverse
from datetime import datetime, timedelta
from tests.factories.support_ticket_factory import SupportTicketFactory

class TestAnalyticsFiltering(BaseAPITestCase, DRFResponseAssertionsMixin):
    def setUp(self):
        super().setUp()
        self.now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # One ticket outside filter range
        SupportTicketFactory.create(
            dumped_at=self.now - timedelta(days=10),
            completed_at=self.now - timedelta(days=9),
            tenant_id=self.tenant_id
        )
        # One ticket inside filter range
        SupportTicketFactory.create(
            dumped_at=self.now - timedelta(days=1),
            completed_at=self.now,
            tenant_id=self.tenant_id
        )

    def test_start_end_filtering(self):
        """
        The API should only return results whose date 'x' is between start and end.
        """
        url = reverse("analytics:stacked-bar")
        start = (self.now - timedelta(days=5)).date().isoformat()
        end = self.now.date().isoformat()

        response = self.client.get(url, {"start": start, "end": end}, **self.auth_headers)
        self.assert_success_response(response)
        data = response.json()
        for item in data:
            self.assertTrue(
                start <= item["x"] <= end,
                f"Date {item['x']} not within [{start}, {end}]"
            )

    def test_resolution_status_case_insensitive(self):
        """
        Should count tickets as resolved even if resolution_status is mixed case.
        """
        SupportTicketFactory.create(
            resolution_status="ReSolVed",
            tenant_id=self.tenant_id,
            dumped_at=self.now - timedelta(days=2),
            completed_at=self.now - timedelta(days=1)
        )
        url = reverse("analytics:stacked-bar")
        response = self.client.get(url, **self.auth_headers)
        self.assert_success_response(response)
        data = response.json()
        # At least one date should have y1 (resolved count) > 0
        self.assertTrue(
            any(item.get("y1", 0) > 0 for item in data),
            "No resolved ticket counted (y1 == 0 everywhere)"
        )
