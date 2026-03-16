from unittest.mock import patch, MagicMock
from django.test import TestCase

from support_ticket.services import TicketTimeService, MixpanelService


class TicketTimeServiceTest(TestCase):
    """Test the time calculation service independently"""
    
    def setUp(self):
        self.service = TicketTimeService()
    
    def test_add_time_strings_basic(self):
        """Test basic time string addition"""
        result = self.service.add_time_strings("5:30", "2:15")
        self.assertEqual(result, "7:45")
    
    def test_add_time_strings_with_seconds_carryover(self):
        """Test time addition with seconds carryover"""
        result = self.service.add_time_strings("5:45", "2:30")
        self.assertEqual(result, "8:15")
    
    def test_add_time_strings_with_minutes_carryover(self):
        """Test time addition with minutes carryover from seconds"""
        result = self.service.add_time_strings("3:45", "2:35")
        self.assertEqual(result, "6:20")
    
    def test_add_time_strings_large_numbers(self):
        """Test time addition with large numbers"""
        result = self.service.add_time_strings("45:30", "120:45")
        self.assertEqual(result, "166:15")
    
    def test_add_time_strings_with_none(self):
        """Test time addition with None values"""
        result = self.service.add_time_strings(None, "5:30")
        self.assertEqual(result, "5:30")
        
        result = self.service.add_time_strings("5:30", None)
        self.assertEqual(result, "5:30")
        
        result = self.service.add_time_strings(None, None)
        self.assertEqual(result, "0:00")
    
    def test_add_time_strings_with_invalid_format(self):
        """Test time addition with invalid formats"""
        result = self.service.add_time_strings("invalid", "5:30")
        self.assertEqual(result, "5:30")
        
        result = self.service.add_time_strings("5:30", "invalid")
        self.assertEqual(result, "5:30")
        
        result = self.service.add_time_strings("", "5:30")
        self.assertEqual(result, "5:30")
        
        result = self.service.add_time_strings("5:30", "")
        self.assertEqual(result, "5:30")
    
    def test_add_time_strings_edge_cases(self):
        """Test edge cases for time addition"""
        # Test with zero values
        result = self.service.add_time_strings("0:00", "5:30")
        self.assertEqual(result, "5:30")
        
        result = self.service.add_time_strings("5:30", "0:00")
        self.assertEqual(result, "5:30")
        
        # Test with single digit minutes/seconds
        result = self.service.add_time_strings("1:5", "2:3")
        self.assertEqual(result, "3:08")


class MixpanelServiceTest(TestCase):
    """Test the Mixpanel service independently"""
    
    # Notice: setUp(self) has been completely removed!

    @patch('os.environ.get')
    def test_mixpanel_service_initialization(self, mock_env):
        """Test Mixpanel service initialization"""
        mock_env.return_value = 'test_token'
        service = MixpanelService() # <-- Initialized AFTER the mock is active
        self.assertEqual(service.mixpanel_api_url, "https://api.thecircleapp.in/pyro/send_to_mixpanel")
        self.assertEqual(service.mixpanel_token, 'test_token')
    
    @patch('requests.post')
    @patch('os.environ.get')
    def test_send_to_mixpanel_sync_success(self, mock_env, mock_post):
        """Test successful synchronous Mixpanel event sending"""
        mock_env.return_value = 'test_token'
        service = MixpanelService() # <-- Initialized here now!
        
        mock_response = MagicMock()
        mock_response.ok = True
        mock_post.return_value = mock_response
        
        result = service.send_to_mixpanel_sync(
            user_id="123",
            event_name="test_event",
            properties={"key": "value"}
        )
        
        self.assertTrue(result)
        mock_post.assert_called_once()
        
        # Verify the request payload
        call_args = mock_post.call_args
        expected_payload = {
            'user_id': 123,
            'event_name': 'test_event',
            'properties': {'key': 'value'}
        }
        self.assertEqual(call_args[1]['json'], expected_payload)
        
        # Verify headers
        expected_headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer test_token'
        }
        self.assertEqual(call_args[1]['headers'], expected_headers)
    
    @patch('requests.post')
    @patch('os.environ.get')
    def test_send_to_mixpanel_sync_api_error(self, mock_env, mock_post):
        """Test Mixpanel API error handling"""
        mock_env.return_value = 'test_token'
        service = MixpanelService() # <-- Initialized here now!
        
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_post.return_value = mock_response
        
        result = service.send_to_mixpanel_sync(
            user_id="123",
            event_name="test_event",
            properties={"key": "value"}
        )
        
        self.assertFalse(result)
    
    @patch('requests.post')
    @patch('os.environ.get')
    def test_send_to_mixpanel_sync_request_exception(self, mock_env, mock_post):
        """Test Mixpanel request exception handling"""
        mock_env.return_value = 'test_token'
        service = MixpanelService() # <-- Initialized here now!
        
        mock_post.side_effect = Exception("Network error")
        
        result = service.send_to_mixpanel_sync(
            user_id="123",
            event_name="test_event",
            properties={"key": "value"}
        )
        
        self.assertFalse(result)
    
    @patch('os.environ.get')
    def test_send_to_mixpanel_sync_no_token(self, mock_env):
        """Test Mixpanel service without token"""
        mock_env.return_value = None
        service = MixpanelService() # <-- Initialized here now!
        
        result = service.send_to_mixpanel_sync(
            user_id="123",
            event_name="test_event",
            properties={"key": "value"}
        )
        
        self.assertFalse(result)
    
    @patch('requests.post')
    @patch('os.environ.get')
    def test_send_to_mixpanel_sync_timeout(self, mock_env, mock_post):
        """Test Mixpanel request timeout handling"""
        mock_env.return_value = 'test_token'
        service = MixpanelService() # <-- Initialized here now!
        
        mock_post.side_effect = Exception("Timeout")
        
        result = service.send_to_mixpanel_sync(
            user_id="123",
            event_name="test_event",
            properties={"key": "value"}
        )
        
        self.assertFalse(result)
