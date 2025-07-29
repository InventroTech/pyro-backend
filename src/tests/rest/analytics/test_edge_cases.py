from tests.base.test_setup import BaseAPITestCase
from tests.base.assertions import DRFResponseAssertionsMixin
from django.urls import reverse
from tests.factories.support_ticket_factory import SupportTicketFactory
from datetime import date

class TestAnalyticsEdgeCases(BaseAPITestCase, DRFResponseAssertionsMixin):
    def test_no_tickets(self):
        """
        All analytics endpoints should return a single entry with today's date and zeroed values if there are no tickets.
        """
        today = str(date.today())
        # Define the expected empty shapes for each endpoint
        expected_responses = {
            "analytics:stacked-bar": [{"x": today, "y1": 0, "y2": 0}],
            "analytics:daily-resolved-tickets": [{"x": today, "y": 0}],
            "analytics:ticket-close-time": [{"x": today, "y": 0}],
            "analytics:daily-resolution-percentile": [{"x": today, "y": 0}],
        }

        for url_name, expected in expected_responses.items():
            url = reverse(url_name)
            response = self.client.get(url, **self.auth_headers)
            self.assert_success_response(response)
            self.assertEqual(response.json(), expected)

    def test_single_ticket_daily_resolved_tickets(self):
        """
        Single ticket: resolved tickets endpoint should return a list of length 1, with correct keys.
        """
        SupportTicketFactory(tenant_id=self.tenant_id)
        url = reverse("analytics:daily-resolved-tickets")
        response = self.client.get(url, **self.auth_headers)
        self.assert_success_response(response)
        data = response.json()
        self.assert_response_list_length(data, 1)
        self.assert_response_keys(data[0], ["x", "y"])

    def test_malformed_percentile_returns_4xx(self):
        """
        If a non-numeric percentile is provided, the percentile view should return 400 or 422.
        """
        url = reverse("analytics:daily-resolution-percentile")
        response = self.client.get(url, {"percentile": "not-a-number"}, **self.auth_headers)
        self.assert_error_response(response, allowed_status=(400, 422))
