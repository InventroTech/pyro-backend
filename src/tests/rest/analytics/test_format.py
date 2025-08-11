from tests.base.test_setup import BaseAPITestCase
from tests.base.assertions import DRFResponseAssertionsMixin  # Updated import!
from django.urls import reverse
from tests.factories.support_ticket_factory import SupportTicketFactory

class TestAnalyticsResponseFormat(BaseAPITestCase, DRFResponseAssertionsMixin):
    def setUp(self):
        super().setUp()
        SupportTicketFactory(tenant_id=self.tenant_id)

    def test_daily_resolution_percentile_field_types(self):
        """
        Response should have list of dicts, each with 'x': str (date), 'y': float or int (value).
        """
        url = reverse("analytics:daily-resolution-percentile")
        response = self.client.get(
            url,
            {"percentile": 90, "unit": "hours"},
            **self.auth_headers
        )
        self.assert_success_response(response)
        data = response.json()
        self.assertTrue(data, "Expected at least one item in response")
        item = data[0]
        self.assert_response_keys(item, ["x", "y"])
        self.assertIsInstance(item["x"], str, f"Expected 'x' to be str, got {type(item['x'])}")
        self.assertIsInstance(item["y"], (float, int), f"Expected 'y' to be float or int, got {type(item['y'])}")
