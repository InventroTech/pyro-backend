import json
from unittest.mock import patch, MagicMock
from django.urls import reverse
from rest_framework import status
from django.utils import timezone

from crm_records.models import EventLog, Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from tests.rest.support_ticket.support_rules import seed_support_ticket_rules
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_dump_factory import dump_data
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

        seed_support_ticket_rules(self.tenant)
        self.record = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id="test_user_123",
                name="Test User",
                phone="1234567890",
                resolution_time="0:00",
                call_attempts=0,
                release_build_number="4.5.6",
            ),
        )

        self.url = reverse('support_ticket:save-and-continue')
    
    @patch("support_ticket.events._enqueue_mixpanel_event")
    def test_save_and_continue_success(self, mock_enqueue_mixpanel):
        """Test successful save and continue operation"""
        
        data = {
            'ticketId': self.record.id,
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
        
        self.record.refresh_from_db()
        data = self.record.data
        self.assertEqual(data['resolution_status'], 'Resolved')
        self.assertEqual(data['call_status'], 'Answered')
        self.assertEqual(data['cse_remarks'], 'Issue resolved successfully')
        self.assertEqual(data['cse_name'], self.email)
        self.assertEqual(data['resolution_time'], '5:30')
        self.assertEqual(data['call_attempts'], 1)
        self.assertEqual(data['other_reasons'], ['Network issue'])
        self.assertIsNotNone(data['completed_at'])
        self.assertEqual(EventLog.objects.filter(record=self.record).count(), 1)
        self.assertEqual(
            EventLog.objects.filter(record=self.record).first().event,
            'support.resolved',
        )
        
        # Verify Mixpanel enqueue (pyro_st_connected + pyro_st_resolve)
        self.assertEqual(mock_enqueue_mixpanel.call_count, 2)
        names = [c.kwargs["event_name"] for c in mock_enqueue_mixpanel.call_args_list]
        self.assertEqual(names, ["pyro_st_connected", "pyro_st_resolve"])
        resolve_call = mock_enqueue_mixpanel.call_args_list[1]
        self.assertEqual(resolve_call.kwargs["properties"]["release_build_number"], "4.5.6")
    
    def test_save_and_continue_time_accumulation(self):
        """Test time accumulation functionality"""
        self.record.data = {**self.record.data, "resolution_time": "3:15"}
        self.record.save(update_fields=["data"])

        data = {
            'ticketId': self.record.id,
            'resolutionStatus': 'WIP',
            'resolutionTime': '2:30',
            'isReadOnly': False
        }

        with patch("support_ticket.events._enqueue_mixpanel_event"):
            response = self.client.post(self.url, data, format='json', **self.auth_headers)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['totalResolutionTime'], '5:45')

        self.record.refresh_from_db()
        self.assertEqual(self.record.data['resolution_time'], '5:45')
    
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
            'ticketId': self.record.id,
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
            'ticketId': self.record.id,
            'resolutionStatus': 'Resolved',
            'isReadOnly': False
        }
        
        # Make request without auth headers
        response = self.client.post(self.url, data, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
    
    @patch("support_ticket.events._enqueue_mixpanel_event")
    def test_mixpanel_events_for_different_statuses(self, mock_enqueue_mixpanel):
        """Test that correct Mixpanel events are sent for different resolution statuses"""
        
        test_cases = [
            ('Resolved', 'pyro_st_resolve'),
            ("Can't Resolve", 'pyro_st_cannot_resolve'),
            ('WIP', 'pyro_st_call_later'),
        ]
        
        for resolution_status, expected_event in test_cases:
            with self.subTest(resolution_status=resolution_status):
                mock_enqueue_mixpanel.reset_mock()
                
                data = {
                    'ticketId': self.record.id,
                    'resolutionStatus': resolution_status,
                    'isReadOnly': False
                }
                
                response = self.client.post(self.url, data, format='json', **self.auth_headers)
                
                self.assertEqual(response.status_code, status.HTTP_200_OK)
                
                self.assertEqual(mock_enqueue_mixpanel.call_count, 2)
                names = [c.kwargs["event_name"] for c in mock_enqueue_mixpanel.call_args_list]
                self.assertEqual(names[0], "pyro_st_connected")
                self.assertEqual(names[1], expected_event)

    @patch("support_ticket.events._enqueue_mixpanel_event")
    def test_no_mixpanel_event_without_user_id(self, mock_enqueue_mixpanel):
        """Test that no Mixpanel events are sent when ticket has no user_id"""
        record_without_user = Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=dump_data(
                user_id=None,
                resolution_time="0:00",
                call_attempts=0,
            ),
        )

        data = {
            'ticketId': record_without_user.id,
            'resolutionStatus': 'Resolved',
            'isReadOnly': False
        }
        
        response = self.client.post(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        mock_enqueue_mixpanel.assert_not_called()
    
    def test_options_request_cors(self):
        """Test CORS preflight OPTIONS request"""
        # 👇 Add **self.auth_headers here! 👇
        response = self.client.options(self.url, **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response['Access-Control-Allow-Origin'], '*')
        self.assertIn('Authorization', response['Access-Control-Allow-Headers'])
        self.assertIn('POST', response['Access-Control-Allow-Methods'])
