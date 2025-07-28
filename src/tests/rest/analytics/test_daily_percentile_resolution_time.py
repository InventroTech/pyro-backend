from django.urls import reverse
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_factory import SupportTicketFactory

class TestDailyPercentileResolutionTimeView(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        # Create sample resolved tickets with varied times
        SupportTicketFactory(
            dumped_at="2024-07-20T10:00:00Z",
            completed_at="2024-07-20T12:00:00Z",
            tenant_id=self.tenant_id
        )
        SupportTicketFactory(
            dumped_at="2024-07-20T09:00:00Z",
            completed_at="2024-07-20T10:30:00Z",
            tenant_id=self.tenant_id
        )

    def test_percentile_resolution_time(self):
        url = reverse("analytics:daily-resolution-percentile")
        response = self.client.get(url, {"percentile": 90, "unit": "hours"}, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        expected_output = [{"x": "2024-07-20", "y": 1.95}]
        self.assertEqual(response.json(), expected_output)
