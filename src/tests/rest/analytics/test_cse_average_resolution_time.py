from django.urls import reverse
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_factory import SupportTicketFactory
from datetime import datetime, timedelta


class TestCSEAverageResolutionTimeView(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        
        # Create sample tickets for different CSEs
        self.cse1_name = "John Doe"
        self.cse2_name = "Jane Smith"
        
        # CSE 1 tickets - 2 tickets with different resolution times
        SupportTicketFactory(
            cse_name=self.cse1_name,
            resolution_time="02:00",  # 2 hours in MM:SS format
            completed_at="2024-07-20T12:00:00Z",
            tenant_id=self.tenant_id
        )
        SupportTicketFactory(
            cse_name=self.cse1_name,
            resolution_time="02:30",  # 2.5 hours in MM:SS format
            completed_at="2024-07-20T16:30:00Z",
            tenant_id=self.tenant_id
        )
        
        # CSE 2 tickets - 1 ticket
        SupportTicketFactory(
            cse_name=self.cse2_name,
            resolution_time="02:00",  # 2 hours in MM:SS format
            completed_at="2024-07-20T11:00:00Z",
            tenant_id=self.tenant_id
        )

    def test_cse_average_resolution_time(self):
        """Test CSE average resolution time calculation"""
        url = reverse("analytics:cse-average-resolution-time")
        response = self.client.get(url, **self.auth_headers)
        
        self.assertEqual(response.status_code, 200)
        result = response.json()
        
        # Should have 2 CSEs
        self.assertEqual(len(result), 2)
        
        # Find CSE 1 (John Doe) - average should be (2 + 2.5) / 2 = 2.25 hours = 135 minutes
        cse1_result = next((item for item in result if item['cse_name'] == self.cse1_name), None)
        self.assertIsNotNone(cse1_result)
        self.assertEqual(cse1_result['ticket_count'], 2)
        self.assertAlmostEqual(cse1_result['average_resolution_time'], 135.0, places=2)
        self.assertEqual(cse1_result['unit'], 'minutes')
        
        # Find CSE 2 (Jane Smith) - average should be 2 hours = 120 minutes
        cse2_result = next((item for item in result if item['cse_name'] == self.cse2_name), None)
        self.assertIsNotNone(cse2_result)
        self.assertEqual(cse2_result['ticket_count'], 1)
        self.assertAlmostEqual(cse2_result['average_resolution_time'], 120.0, places=2)
        self.assertEqual(cse2_result['unit'], 'minutes')

    def test_cse_average_resolution_time_with_date_range(self):
        """Test CSE average resolution time with specific date range"""
        url = reverse("analytics:cse-average-resolution-time")
        response = self.client.get(
            url, 
            {
                'start': '2024-07-20',
                'end': '2024-07-20'
            }, 
            **self.auth_headers
        )
        
        self.assertEqual(response.status_code, 200)
        result = response.json()
        
        # Should still have 2 CSEs for the specific date
        self.assertEqual(len(result), 2)

    def test_cse_average_resolution_time_different_unit(self):
        """Test CSE average resolution time with different time unit"""
        url = reverse("analytics:cse-average-resolution-time")
        response = self.client.get(
            url, 
            {'unit': 'minutes'}, 
            **self.auth_headers
        )
        
        self.assertEqual(response.status_code, 200)
        result = response.json()
        
        # CSE 1 average in minutes should be 2.25 * 60 = 135 minutes
        cse1_result = next((item for item in result if item['cse_name'] == self.cse1_name), None)
        self.assertIsNotNone(cse1_result)
        self.assertAlmostEqual(cse1_result['average_resolution_time'], 135.0, places=2)
        self.assertEqual(cse1_result['unit'], 'minutes')

    def test_cse_average_resolution_time_no_data(self):
        """Test CSE average resolution time with no data for date range"""
        url = reverse("analytics:cse-average-resolution-time")
        response = self.client.get(
            url, 
            {
                'start': '2024-01-01',
                'end': '2024-01-02'
            }, 
            **self.auth_headers
        )
        
        self.assertEqual(response.status_code, 200)
        result = response.json()
        
        # Should return empty list when no data
        self.assertEqual(result, [])

    def test_cse_average_resolution_time_excludes_empty_cse_names(self):
        """Test that tickets with empty CSE names are excluded"""
        # Create a ticket with empty CSE name
        SupportTicketFactory(
            cse_name="",
            resolution_time="02:00",
            completed_at="2024-07-20T12:00:00Z",
            tenant_id=self.tenant_id
        )
        
        # Create a ticket with null CSE name
        SupportTicketFactory(
            cse_name=None,
            resolution_time="02:00",
            completed_at="2024-07-20T12:00:00Z",
            tenant_id=self.tenant_id
        )
        
        # Create a ticket with empty resolution_time
        SupportTicketFactory(
            cse_name="Test CSE",
            resolution_time="",
            completed_at="2024-07-20T12:00:00Z",
            tenant_id=self.tenant_id
        )
        
        url = reverse("analytics:cse-average-resolution-time")
        response = self.client.get(url, **self.auth_headers)
        
        self.assertEqual(response.status_code, 200)
        result = response.json()
        
        # Should still only have 2 CSEs (excluding empty/null names and empty resolution_time)
        self.assertEqual(len(result), 2)
        
        # Verify no empty or null CSE names in results
        for item in result:
            self.assertIsNotNone(item['cse_name'])
            self.assertNotEqual(item['cse_name'], '')
