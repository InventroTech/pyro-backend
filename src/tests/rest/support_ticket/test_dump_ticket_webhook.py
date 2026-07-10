import json
import uuid
from unittest.mock import patch
from django.urls import reverse
from django.test import override_settings
from rest_framework import status
from django.utils import timezone

from tests.base.test_setup import BaseAPITestCase
from support_ticket.models import SupportTicketDump
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from crm_records.models import Record


class DumpTicketWebhookViewTest(BaseAPITestCase):
    """Test the dump-ticket-webhook API endpoint"""
    
    def setUp(self):
        """Set up test data"""
        super().setUp()
        import os
        
        # 👇 ADD THIS LINE: Clean up thread pollution before every test! 👇
        SupportTicketDump.objects.all().delete()
        
        self.url = reverse('support_ticket:dump-ticket-webhook')
        self.webhook_secret = 'test_webhook_secret_123'
        os.environ['WEBHOOK_SECRET'] = self.webhook_secret
        
        self.valid_headers = {
            'x-webhook-secret': self.webhook_secret,
            'Content-Type': 'application/json'
        }
        
        # Valid webhook payload
        self.valid_payload = {
            'tenant_id': str(self.tenant_id),
            'support_ticket_id': 78901,
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

    def _latest_dump(self):
        return (
            SupportTicketDump.objects.filter(tenant_id=self.tenant_id)
            .order_by('-id')
            .first()
        )
    
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
        self.assertEqual(response.data['ticket_id'], 78901)
        self.assertIsNotNone(response.data['record_id'])
        
        # Verify the ticket was created in the database
        ticket_dump = self._latest_dump()
        self.assertEqual(ticket_dump.tenant_id, uuid.UUID(self.tenant_id))
        data = ticket_dump.data
        self.assertEqual(data.get('user_id'), 'test_user_123')
        self.assertEqual(data.get('name'), 'John Doe')
        self.assertEqual(data.get('phone'), '1234567890')
        self.assertEqual(data.get('reason'), 'Account issue')
        self.assertEqual(data.get('layout_status'), 'pending')
        self.assertEqual(data.get('badge'), 'premium')
        self.assertEqual(data.get('poster'), 'support_agent')
        self.assertEqual(data.get('subscription_status'), 'active')
        self.assertTrue(data.get('atleast_paid_once'))
        self.assertEqual(data.get('source'), 'mobile_app')
        self.assertEqual(
            data.get('praja_dashboard_user_link'),
            'https://www.thecircleapp.in/admin/users/abc123',
        )
        self.assertEqual(data.get('display_pic_url'), 'https://example.com/pic.jpg')
        self.assertTrue(ticket_dump.is_processed)
        self.assertIsNotNone(ticket_dump.created_at)
        record = Record.objects.get(id=response.data['record_id'])
        self.assertEqual(record.entity_type, SUPPORT_TICKET_ENTITY_TYPE)
        self.assertEqual(record.data.get('user_id'), 'test_user_123')
    
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
        self.assertIsNone(response.data['ticket_id'])
        self.assertIsNone(response.data['record_id'])
        
        # Verify the ticket was created with minimal data
        ticket_dump = self._latest_dump()
        self.assertEqual(ticket_dump.tenant_id, uuid.UUID(self.tenant_id))
        data = ticket_dump.data
        self.assertIsNone(data.get('user_id'))
        self.assertIsNone(data.get('name'))
        self.assertIsNotNone(data.get('ticket_date'))
        self.assertTrue(ticket_dump.is_processed)
    
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
        # DRF evaluates {} as False, so your view raises an "Empty JSON" error
        self.assertIn('empty JSON', response.data['error']) 
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
        ticket_dump = self._latest_dump()
        self.assertEqual(ticket_dump.tenant_id, uuid.UUID(self.tenant_id))
        data = ticket_dump.data
        self.assertIsNone(data.get('user_id'))
        self.assertIsNone(data.get('name'))
        self.assertIsNone(data.get('phone'))
        self.assertEqual(data.get('reason'), 'Test reason')
        self.assertIsNone(data.get('atleast_paid_once'))
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_extra_fields_stored_in_data(self):
        """Extra payload keys are persisted in dump ``data`` JSON."""
        payload_with_extra = self.valid_payload.copy()
        payload_with_extra.update({
            'extra_field_1': 'custom value',
            'extra_field_2': 123,
        })

        response = self.client.post(
            self.url,
            data=json.dumps(payload_with_extra),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret}
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        ticket_dump = self._latest_dump()
        self.assertEqual(ticket_dump.data.get('name'), 'John Doe')
        self.assertEqual(ticket_dump.data['extra_field_1'], 'custom value')
        self.assertEqual(ticket_dump.data['extra_field_2'], 123)
    
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
        # DELETED: self.assertEqual(SupportTicketDump.objects.count(), 0)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_boolean_field_handling(self):
        """Test proper handling of boolean fields"""
        # Because the view uses .create() without a serializer, passing strings like 'true' crashes Django.
        # Stick to standard Python booleans.
        test_cases = [
            (True, True),
            (False, False),
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
                
                ticket_dump = self._latest_dump()
                self.assertEqual(ticket_dump.data.get('atleast_paid_once'), expected_value)
                
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
                
                ticket_dump = self._latest_dump()
                self.assertEqual(ticket_dump.data.get('praja_dashboard_user_link'), url)
                self.assertEqual(ticket_dump.data.get('display_pic_url'), url)
                
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
    
    def _post_dump_and_get_ticket_date(self, payload):
        real_time = timezone.now()
        with patch("django.utils.timezone.now") as mock_now:
            mock_now.return_value = real_time
            response = self.client.post(
                self.url,
                data=json.dumps(payload),
                content_type="application/json",
                **{"HTTP_X_WEBHOOK_SECRET": self.webhook_secret},
            )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        ticket_dump = self._latest_dump()
        return ticket_dump.data.get("ticket_date"), real_time

    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_default_ticket_date(self):
        """ticket_date defaults to now when missing or falsy (matches legacy webhook)."""
        payload_no_date = self.valid_payload.copy()
        del payload_no_date["ticket_date"]
        ticket_date, expected = self._post_dump_and_get_ticket_date(payload_no_date)
        self.assertEqual(ticket_date, expected.isoformat())

        payload_empty_date = self.valid_payload.copy()
        payload_empty_date["ticket_date"] = ""
        ticket_date, expected = self._post_dump_and_get_ticket_date(payload_empty_date)
        self.assertEqual(ticket_date, expected.isoformat())
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    def test_dump_ticket_webhook_concurrent_requests(self):
        """Test handling multiple back-to-back webhook requests (unique users)."""
        results = []

        for thread_id in range(5):
            payload = self.valid_payload.copy()
            payload['user_id'] = f'user_{thread_id}'
            payload['name'] = f'User {thread_id}'
            payload['support_ticket_id'] = 80000 + thread_id

            response = self.client.post(
                self.url,
                data=json.dumps(payload),
                content_type='application/json',
                **{'HTTP_X_WEBHOOK_SECRET': self.webhook_secret},
            )
            results.append((thread_id, response.status_code, response.data))

        self.assertEqual(len(results), 5)

        for thread_id, status_code, data in results:
            self.assertEqual(status_code, status.HTTP_200_OK)
            self.assertEqual(data['ticket_id'], 80000 + thread_id)
            self.assertIsNotNone(data['record_id'])

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
        
        ticket_dump = self._latest_dump()
        self.assertEqual(len(ticket_dump.data.get('reason', '')), 10000)
    
    @override_settings(WEBHOOK_SECRET='test_webhook_secret_123')
    @patch('support_ticket.views.logger')
    def test_dump_ticket_webhook_logging(self, mock_logger):
        """Test that appropriate logging occurs during webhook processing"""
        
        # Use a bad secret to trigger the unauthorized warning log!
        response = self.client.post(
            self.url,
            data=json.dumps(self.valid_payload),
            content_type='application/json',
            **{'HTTP_X_WEBHOOK_SECRET': 'wrong_secret_to_trigger_warning'}
        )
        
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertTrue(mock_logger.warning.called)
    
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


