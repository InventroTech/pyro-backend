from django.urls import reverse
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_factory import SupportTicketFactory
from analytics.models import SupportTicket

class TestStackedBarResolvedUnresolvedView(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        # Create tickets for several days
        SupportTicketFactory.create_batch(3, dumped_at="2024-07-20", completed_at="2024-07-20", resolution_status="Resolved", tenant_id=self.tenant_id)
        SupportTicketFactory.create_batch(2, dumped_at="2024-07-20", completed_at=None, resolution_status="Pending", tenant_id=self.tenant_id)

    def test_stacked_bar_data(self):
        url = reverse("analytics:stacked-bar")
        response = self.client.get(url, **self.auth_headers)
        self.assertEqual(response.status_code, 200)
        expected_output = [
            {"x": "2024-07-20", "y1": 3, "y2": 2}
        ]
        self.assertEqual(response.json(), expected_output)
