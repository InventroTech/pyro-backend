from django.urls import reverse
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_factory import SupportTicketFactory

class TestDailyResolvedTicketsView(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        SupportTicketFactory.create_batch(2, completed_at="2024-07-20", tenant_id=self.tenant_id)
        SupportTicketFactory.create_batch(1, completed_at="2024-07-21", tenant_id=self.tenant_id)

    def test_daily_resolved_tickets(self):
        url = reverse("analytics:daily-resolved-tickets")
        response = self.client.get(url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        expected_output = [
            {"x": "2024-07-20", "y": 2},
            {"x": "2024-07-21", "y": 1}
        ]
        self.assertEqual(response.json(), expected_output)
