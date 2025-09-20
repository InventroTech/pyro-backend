import json
import uuid
from unittest.mock import patch
from django.urls import reverse
from django.test import override_settings
from rest_framework import status
from django.utils import timezone

from tests.base.test_setup import BaseAPITestCase
from support_ticket.models import SupportTicketDump


class DumpTicketWebhookViewTest(BaseAPITestCase):
    """Test the dump-ticket-webhook API endpoint"""
    
    def setUp(self):
        """Set up test data"""
        super().setUp()
        self.url = reverse('support_ticket:dump-ticket-webhook')
        self.webhook_secret = 'test_webhook_secret_123'
        self.valid_headers = {
            'x-webhook-secret': self.webhook_secret,
            'Content-Type': 'application/json'
        }
        
        # Valid payload matching the ALLOWED_FIELDS
        self.valid_payload = {
            'tenant_id': str(self.tenant_id),
            'ticket_date': '2023-12-01T10:00:00Z',
            'user_id': 'test_user_123',
            'name': 'John Doe',
            'phone': '1234567890',
            'reason': 'Account issue',
            'layout_status': 'pending',
            'badge': 'premium',
            'poster': 'support_agent',
            'subscription_status': 'active',
            'atleast_paid_once': True,
            'source': 'mobile_app',
            'praja_dashboard_user_link': 'https://www.thecircleapp.in/admin/users/abc123',
            'display_pic_url': 'https://example.com/pic.jpg'
        }
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_success(self):
        """Test successful ticket dump creation"""
        response = self.client.post(
            self.url, 
            data=json.dumps(self.valid_payload),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['message'])
        self.assertIn('ticket_id', response.data)
        
        # Verify the ticket was created in the database
        ticket_dump = SupportTicketDump.objects.get(id=response.data['ticket_id'])
        self.assertEqual(ticket_dump.tenant_id, uuid.UUID(self.tenant_id))
        self.assertEqual(ticket_dump.user_id, 'test_user_123')
        self.assertEqual(ticket_dump.name, 'John Doe')
        self.assertEqual(ticket_dump.phone, '1234567890')
        self.assertEqual(ticket_dump.reason, 'Account issue')
        self.assertEqual(ticket_dump.layout_status, 'pending')
        self.assertEqual(ticket_dump.badge, 'premium')
        self.assertEqual(ticket_dump.poster, 'support_agent')
        self.assertEqual(ticket_dump.subscription_status, 'active')
        self.assertTrue(ticket_dump.atleast_paid_once)
        self.assertEqual(ticket_dump.source, 'mobile_app')
        self.assertEqual(ticket_dump.praja_dashboard_user_link, 'https://www.thecircleapp.in/admin/users/abc123')
        self.assertEqual(ticket_dump.display_pic_url, 'https://example.com/pic.jpg')
        self.assertFalse(ticket_dump.is_processed)  # Should default to False
        self.assertIsNotNone(ticket_dump.created_at)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_minimal_payload(self):
        """Test webhook with minimal required fields only"""
        minimal_payload = {
            'tenant_id': str(self.tenant_id)
        }
        
        response = self.client.post(
            self.url,
            data=json.dumps(minimal_payload),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify the ticket was created with minimal data
        ticket_dump = SupportTicketDump.objects.get(id=response.data['ticket_id'])
        self.assertEqual(ticket_dump.tenant_id, uuid.UUID(self.tenant_id))
        self.assertIsNone(ticket_dump.user_id)
        self.assertIsNone(ticket_dump.name)
        self.assertIsNotNone(ticket_dump.ticket_date)  # Should be set to current time
        self.assertFalse(ticket_dump.is_processed)
    
    def test_dump_ticket_webhook_missing_secret(self):
        """Test webhook request without secret header"""
        response = self.client.post(
            self.url,
            data=json.dumps(self.valid_payload),
            content_type='application/json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertIn('Unauthorized', response.data['error'])
        
        # Verify no ticket was created
        self.assertEqual(SupportTicketDump.objects.count(), 0)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_invalid_secret(self):
        """Test webhook request with invalid secret"""
        response = self.client.post(
            self.url,
            data=json.dumps(self.valid_payload),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': 'invalid_secret'}
        )
        
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertIn('Unauthorized', response.data['error'])
        
        # Verify no ticket was created
        self.assertEqual(SupportTicketDump.objects.count(), 0)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_missing_tenant_id(self):
        """Test webhook request without required tenant_id"""
        payload_without_tenant = self.valid_payload.copy()
        del payload_without_tenant['tenant_id']
        
        response = self.client.post(
            self.url,
            data=json.dumps(payload_without_tenant),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('tenant_id', response.data['error'])
        
        # Verify no ticket was created
        self.assertEqual(SupportTicketDump.objects.count(), 0)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_invalid_json(self):
        """Test webhook request with invalid JSON payload"""
        response = self.client.post(
            self.url,
            data='invalid json string',
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('error', response.data)
        
        # Verify no ticket was created
        self.assertEqual(SupportTicketDump.objects.count(), 0)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_empty_payload(self):
        """Test webhook request with empty payload"""
        response = self.client.post(
            self.url,
            data=json.dumps({}),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('tenant_id', response.data['error'])
        
        # Verify no ticket was created
        self.assertEqual(SupportTicketDump.objects.count(), 0)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_null_values(self):
        """Test webhook request with null values for optional fields"""
        payload_with_nulls = {
            'tenant_id': str(self.tenant_id),
            'user_id': None,
            'name': None,
            'phone': None,
            'reason': 'Test reason',
            'atleast_paid_once': None
        }
        
        response = self.client.post(
            self.url,
            data=json.dumps(payload_with_nulls),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify the ticket was created with null values handled correctly
        ticket_dump = SupportTicketDump.objects.get(id=response.data['ticket_id'])
        self.assertEqual(ticket_dump.tenant_id, uuid.UUID(self.tenant_id))
        self.assertIsNone(ticket_dump.user_id)
        self.assertIsNone(ticket_dump.name)
        self.assertIsNone(ticket_dump.phone)
        self.assertEqual(ticket_dump.reason, 'Test reason')
        self.assertIsNone(ticket_dump.atleast_paid_once)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_extra_fields_ignored(self):
        """Test that extra fields not in ALLOWED_FIELDS are ignored"""
        payload_with_extra = self.valid_payload.copy()
        payload_with_extra.update({
            'extra_field_1': 'should be ignored',
            'extra_field_2': 123,
            'malicious_field': '<script>alert("xss")</script>'
        })
        
        response = self.client.post(
            self.url,
            data=json.dumps(payload_with_extra),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify only allowed fields were saved
        ticket_dump = SupportTicketDump.objects.get(id=response.data['ticket_id'])
        self.assertEqual(ticket_dump.name, 'John Doe')
        # Extra fields should not exist in the model
        with self.assertRaises(AttributeError):
            ticket_dump.extra_field_1
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_invalid_uuid_tenant_id(self):
        """Test webhook with invalid UUID format for tenant_id"""
        payload_invalid_uuid = self.valid_payload.copy()
        payload_invalid_uuid['tenant_id'] = 'invalid-uuid-format'
        
        response = self.client.post(
            self.url,
            data=json.dumps(payload_invalid_uuid),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        
        # Verify no ticket was created
        self.assertEqual(SupportTicketDump.objects.count(), 0)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_boolean_field_handling(self):
        """Test proper handling of boolean fields"""
        test_cases = [
            (True, True),
            (False, False),
            ('true', True),  # String representation
            ('false', False),
            (1, True),
            (0, False),
        ]
        
        for input_value, expected_value in test_cases:
            with self.subTest(input_value=input_value):
                payload = self.valid_payload.copy()
                payload['atleast_paid_once'] = input_value
                
                response = self.client.post(
                    self.url,
                    data=json.dumps(payload),
                    content_type='application/json',
                    **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
                )
                
                self.assertEqual(response.status_code, status.HTTP_200_OK)
                
                ticket_dump = SupportTicketDump.objects.get(id=response.data['ticket_id'])
                self.assertEqual(ticket_dump.atleast_paid_once, expected_value)
                
                # Clean up for next iteration
                ticket_dump.delete()
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_url_field_validation(self):
        """Test URL field validation for praja_dashboard_user_link and display_pic_url"""
        valid_urls = [
            'https://www.thecircleapp.in/admin/users/abc123',
            'http://example.com/image.jpg',
            'https://cdn.example.com/path/to/image.png?v=123'
        ]
        
        for url in valid_urls:
            with self.subTest(url=url):
                payload = self.valid_payload.copy()
                payload['praja_dashboard_user_link'] = url
                payload['display_pic_url'] = url
                
                response = self.client.post(
                    self.url,
                    data=json.dumps(payload),
                    content_type='application/json',
                    **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
                )
                
                self.assertEqual(response.status_code, status.HTTP_200_OK)
                
                ticket_dump = SupportTicketDump.objects.get(id=response.data['ticket_id'])
                self.assertEqual(ticket_dump.praja_dashboard_user_link, url)
                self.assertEqual(ticket_dump.display_pic_url, url)
                
                # Clean up for next iteration
                ticket_dump.delete()
    
    def test_dump_ticket_webhook_options_request(self):
        """Test CORS preflight OPTIONS request"""
        response = self.client.options(self.url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response['Access-Control-Allow-Origin'], '*')
        self.assertIn('x-webhook-secret', response['Access-Control-Allow-Headers'].lower())
        self.assertIn('POST', response['Access-Control-Allow-Methods'])
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_method_not_allowed(self):
        """Test that only POST and OPTIONS methods are allowed"""
        methods_to_test = ['GET', 'PUT', 'PATCH', 'DELETE']
        
        for method in methods_to_test:
            with self.subTest(method=method):
                response = getattr(self.client, method.lower())(self.url)
                self.assertEqual(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_default_ticket_date(self):
        """Test that ticket_date defaults to current time when not provided"""
        payload_no_date = self.valid_payload.copy()
        del payload_no_date['ticket_date']
        
        # Mock timezone.now() to control the expected time
        with patch('django.utils.timezone.now') as mock_now:
            mock_time = timezone.now()
            mock_now.return_value = mock_time
            
            response = self.client.post(
                self.url,
                data=json.dumps(payload_no_date),
                content_type='application/json',
                **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
            )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        ticket_dump = SupportTicketDump.objects.get(id=response.data['ticket_id'])
        self.assertIsNotNone(ticket_dump.ticket_date)
        # The ticket_date should be close to the mock time (within a few seconds)
        time_diff = abs((ticket_dump.ticket_date - mock_time).total_seconds())
        self.assertLess(time_diff, 5)  # Within 5 seconds
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_concurrent_requests(self):
        """Test handling multiple concurrent webhook requests"""
        import threading
        import time
        
        results = []
        errors = []
        
        def make_request(thread_id):
            try:
                payload = self.valid_payload.copy()
                payload['user_id'] = f'user_{thread_id}'
                payload['name'] = f'User {thread_id}'
                
                response = self.client.post(
                    self.url,
                    data=json.dumps(payload),
                    content_type='application/json',
                    **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
                )
                results.append((thread_id, response.status_code, response.data))
            except Exception as e:
                errors.append((thread_id, str(e)))
        
        # Create and start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=make_request, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify all requests succeeded
        self.assertEqual(len(errors), 0, f"Errors occurred: {errors}")
        self.assertEqual(len(results), 5)
        
        for thread_id, status_code, data in results:
            self.assertEqual(status_code, status.HTTP_200_OK)
            self.assertIn('ticket_id', data)
        
        # Verify all tickets were created
        self.assertEqual(SupportTicketDump.objects.count(), 5)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_large_payload(self):
        """Test webhook with large text fields"""
        large_text = 'A' * 10000  # 10KB of text
        
        payload = self.valid_payload.copy()
        payload['reason'] = large_text
        
        response = self.client.post(
            self.url,
            data=json.dumps(payload),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        ticket_dump = SupportTicketDump.objects.get(id=response.data['ticket_id'])
        self.assertEqual(len(ticket_dump.reason), 10000)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    @patch('support_ticket.views.logger')
    def test_dump_ticket_webhook_logging(self, mock_logger):
        """Test that appropriate logging occurs during webhook processing"""
        response = self.client.post(
            self.url,
            data=json.dumps(self.valid_payload),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify that logging was called (exact calls depend on implementation)
        self.assertTrue(mock_logger.warning.called or mock_logger.info.called or mock_logger.error.called)
    
    def test_rate_limiting_behavior(self):
        """Test webhook behavior under rapid successive requests"""
        # This test depends on your rate limiting configuration
        # Adjust the number of requests based on your throttle settings
        
        for i in range(10):  # Make 10 rapid requests
            response = self.client.post(
                self.url,
                data=json.dumps({'tenant_id': str(self.tenant_id)}),
                content_type='application/json',
                **{'HTTP_X_WEBHOOK_SECRET': 'test_webhook_secret_123'}
            )
            
            # The response should be either success or rate limited
            self.assertIn(response.status_code, [
                status.HTTP_200_OK,
                status.HTTP_401_UNAUTHORIZED,  # Due to missing/invalid secret
                status.HTTP_429_TOO_MANY_REQUESTS  # If rate limiting is enabled
            ])


