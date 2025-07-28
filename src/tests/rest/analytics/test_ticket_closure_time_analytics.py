from django.urls import reverse
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_factory import SupportTicketFactory

class TestTicketClosureTimeAnalytics(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        SupportTicketFactory(
            dumped_at="2024-07-20T08:00:00Z",
            completed_at="2024-07-20T12:00:00Z",
            tenant_id=self.tenant_id
        )

    def test_ticket_closure_time(self):
        url = reverse("analytics:ticket-close-time")
        response = self.client.get(url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        expected_output = [{"x": "2024-07-20", "y": 4.0}]
        self.assertEqual(response.json(), expected_output)
