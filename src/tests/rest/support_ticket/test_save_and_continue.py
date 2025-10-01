import json
from unittest.mock import patch, MagicMock
from django.urls import reverse
from rest_framework import status
from django.utils import timezone

from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_factory import SupportTicketFactory
from support_ticket.models import SupportTicket
from support_ticket.services import TicketTimeService, MixpanelService


class TicketTimeServiceTest(BaseAPITestCase):
    """Test the time calculation service"""
    
    def test_add_time_strings(self):
        """Test time string addition"""
        service = TicketTimeService()
        
        # Test normal addition
        result = service.add_time_strings("5:30", "2:45")
        self.assertEqual(result, "8:15")
        
        # Test with carryover
        result = service.add_time_strings("5:30", "3:45")
        self.assertEqual(result, "9:15")
        
        # Test with carryover of seconds
        result = service.add_time_strings("1:45", "2:30")
        self.assertEqual(result, "4:15")
        
        # Test with None values
        result = service.add_time_strings(None, "5:30")
        self.assertEqual(result, "5:30")
        
        result = service.add_time_strings("5:30", None)
        self.assertEqual(result, "5:30")
        
        # Test with invalid format
        result = service.add_time_strings("invalid", "5:30")
        self.assertEqual(result, "5:30")
        
        # Test both None
        result = service.add_time_strings(None, None)
        self.assertEqual(result, "0:00")


class MixpanelServiceTest(BaseAPITestCase):
    """Test the Mixpanel service"""
    
    @patch('requests.post')
    @patch('os.environ.get')
    def test_send_to_mixpanel_success(self, mock_env, mock_post):
        """Test successful Mixpanel event sending"""
        # Mock environment variable
        mock_env.return_value = 'test_token'
        
        # Mock successful response
        mock_response = MagicMock()
        mock_response.ok = True
        mock_post.return_value = mock_response
        
        service = MixpanelService()
        result = service.send_to_mixpanel_sync(
            user_id="123",
            event_name="test_event",
            properties={"key": "value"}
        )
        
        self.assertTrue(result)
        mock_post.assert_called_once()
        
        # Verify the call was made with correct parameters
        call_args = mock_post.call_args
        expected_payload = {
            'user_id': 123,
            'event_name': 'test_event',
            'properties': {'key': 'value'}
        }
        self.assertEqual(call_args[1]['json'], expected_payload)
    
    @patch('os.environ.get')
    def test_send_to_mixpanel_no_token(self, mock_env):
        """Test Mixpanel service without token"""
        # Mock missing token
        mock_env.return_value = None
        
        service = MixpanelService()
        result = service.send_to_mixpanel_sync(
            user_id="123",
            event_name="test_event",
            properties={"key": "value"}
        )
        
        self.assertFalse(result)
    
    @patch('requests.post')
    @patch('os.environ.get')
    def test_send_to_mixpanel_api_error(self, mock_env, mock_post):
        """Test Mixpanel API error handling"""
        # Mock environment variable
        mock_env.return_value = 'test_token'
        
        # Mock API error response
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_post.return_value = mock_response
        
        service = MixpanelService()
        result = service.send_to_mixpanel_sync(
            user_id="123",
            event_name="test_event",
            properties={"key": "value"}
        )
        
        self.assertFalse(result)


class SaveAndContinueViewTest(BaseAPITestCase):
    """Test the save-and-continue API endpoint"""
    
    def setUp(self):
        """Set up test data"""
        super().setUp()
        
        self.support_ticket = SupportTicketFactory.create(
            user_id="test_user_123",
            name="Test User",
            phone="1234567890",
            tenant_id=self.tenant_id,
            resolution_time="0:00",
            call_attempts=0
        )
        
        self.url = reverse('support_ticket:save-and-continue')
    
    @patch('support_ticket.services.MixpanelService.send_to_mixpanel_sync')
    def test_save_and_continue_success(self, mock_mixpanel):
        """Test successful save and continue operation"""
        mock_mixpanel.return_value = True
        
        data = {
            'ticketId': self.support_ticket.id,
            'resolutionStatus': 'Resolved',
            'callStatus': 'Answered',
            'cseRemarks': 'Issue resolved successfully',
            'resolutionTime': '5:30',
            'otherReasons': ['Network issue'],
            'isReadOnly': False
        }
        
        response = self.client.post(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['success'])
        self.assertEqual(response.data['message'], 'Ticket updated successfully')
        self.assertEqual(response.data['userId'], self.supabase_uid)
        self.assertEqual(response.data['userEmail'], self.email)
        self.assertEqual(response.data['totalResolutionTime'], '5:30')
        
        # Verify ticket was updated
        updated_ticket = SupportTicket.objects.get(id=self.support_ticket.id)
        self.assertEqual(updated_ticket.resolution_status, 'Resolved')
        self.assertEqual(updated_ticket.call_status, 'Answered')
        self.assertEqual(updated_ticket.cse_remarks, 'Issue resolved successfully')
        self.assertEqual(updated_ticket.cse_name, self.email)
        self.assertEqual(updated_ticket.resolution_time, '5:30')
        self.assertEqual(updated_ticket.call_attempts, 1)
        self.assertEqual(updated_ticket.other_reasons, ['Network issue'])
        self.assertIsNotNone(updated_ticket.completed_at)
        
        # Verify Mixpanel calls
        self.assertEqual(mock_mixpanel.call_count, 2)  # pyro_connected + pyro_resolve
    
    def test_save_and_continue_time_accumulation(self):
        """Test time accumulation functionality"""
        # Set initial resolution time
        self.support_ticket.resolution_time = "3:15"
        self.support_ticket.save()
        
        data = {
            'ticketId': self.support_ticket.id,
            'resolutionStatus': 'WIP',
            'resolutionTime': '2:30',
            'isReadOnly': False
        }
        
        with patch('support_ticket.services.MixpanelService.send_to_mixpanel_sync'):
            response = self.client.post(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['totalResolutionTime'], '5:45')
        
        # Verify database update
        updated_ticket = SupportTicket.objects.get(id=self.support_ticket.id)
        self.assertEqual(updated_ticket.resolution_time, '5:45')
    
    def test_ticket_not_found(self):
        """Test handling of non-existent ticket"""
        data = {
            'ticketId': 99999,  # Non-existent ticket
            'resolutionStatus': 'Resolved',
            'isReadOnly': False
        }
        
        response = self.client.post(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('Ticket not found', str(response.data))
    
    def test_read_only_ticket(self):
        """Test handling of read-only tickets"""
        data = {
            'ticketId': self.support_ticket.id,
            'resolutionStatus': 'Resolved',
            'isReadOnly': True
        }
        
        response = self.client.post(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('read-only', str(response.data))
    
    def test_missing_ticket_id(self):
        """Test handling of missing ticket ID"""
        data = {
            'resolutionStatus': 'Resolved',
            'isReadOnly': False
        }
        
        response = self.client.post(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('Invalid request data', str(response.data))
    
    def test_unauthorized_request(self):
        """Test handling of unauthorized requests"""
        data = {
            'ticketId': self.support_ticket.id,
            'resolutionStatus': 'Resolved',
            'isReadOnly': False
        }
        
        # Make request without auth headers
        response = self.client.post(self.url, data, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
    
    @patch('support_ticket.services.MixpanelService.send_to_mixpanel_sync')
    def test_mixpanel_events_for_different_statuses(self, mock_mixpanel):
        """Test that correct Mixpanel events are sent for different resolution statuses"""
        mock_mixpanel.return_value = True
        
        test_cases = [
            ('Resolved', 'pyro_resolve'),
            ("Can't Resolve", 'pyro_cannot_resolve'),
            ('WIP', 'pyro_call_later'),
        ]
        
        for resolution_status, expected_event in test_cases:
            with self.subTest(resolution_status=resolution_status):
                mock_mixpanel.reset_mock()
                
                data = {
                    'ticketId': self.support_ticket.id,
                    'resolutionStatus': resolution_status,
                    'isReadOnly': False
                }
                
                response = self.client.post(self.url, data, format='json', **self.auth_headers)
                
                self.assertEqual(response.status_code, status.HTTP_200_OK)
                
                # Verify Mixpanel calls
                self.assertEqual(mock_mixpanel.call_count, 2)
                
                # Check the calls
                calls = mock_mixpanel.call_args_list
                self.assertEqual(calls[0][0][1], 'pyro_connected')  # First call
                self.assertEqual(calls[1][0][1], expected_event)    # Second call
    
    @patch('support_ticket.services.MixpanelService.send_to_mixpanel_sync')
    def test_no_mixpanel_event_without_user_id(self, mock_mixpanel):
        """Test that no Mixpanel events are sent when ticket has no user_id"""
        mock_mixpanel.return_value = True
        
        # Create ticket without user_id
        ticket_without_user = SupportTicketFactory.create(
            user_id=None,  # No user_id
            tenant_id=self.tenant_id,
            resolution_time="0:00",
            call_attempts=0
        )
        
        data = {
            'ticketId': ticket_without_user.id,
            'resolutionStatus': 'Resolved',
            'isReadOnly': False
        }
        
        response = self.client.post(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify no Mixpanel calls were made
        mock_mixpanel.assert_not_called()
    
    def test_options_request_cors(self):
        """Test CORS preflight OPTIONS request"""
        response = self.client.options(self.url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response['Access-Control-Allow-Origin'], '*')
        self.assertIn('Authorization', response['Access-Control-Allow-Headers'])
        self.assertIn('POST', response['Access-Control-Allow-Methods'])
