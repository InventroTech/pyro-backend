import uuid
from datetime import datetime, timedelta
from django.urls import reverse
from rest_framework import status
from django.utils import timezone

from accounts.models import SupabaseAuthUser
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_factory import SupportTicketFactory
from support_ticket.models import SupportTicket


class SupportTicketUpdateViewTest(BaseAPITestCase):
    """Test the support ticket update API endpoint"""

    def setUp(self):
        """Set up test data"""
        super().setUp()
        
        # Create a support ticket for testing
        self.support_ticket = SupportTicketFactory.create(
            user_id="test_user_123",
            name="Test User",
            phone="1234567890",
            tenant_id=self.tenant_id,
            assigned_to=None,
            resolution_status=None,
            cse_name=None,
            cse_remarks=None,
            call_status=None,
            layout_status="pending"
        )
        
        self.url = reverse('support_ticket:update-ticket')
        
        # Replace the cse_user creation with this:
        self.cse_user = SupabaseAuthUser.objects.create(
            id=uuid.uuid4(), 
            email="cse@example.com"
        )
        self.cse_uuid = str(self.cse_user.id)
        
    def test_update_assigned_to_success(self):
        """Test successful ticket assignment to a CSE"""
        data = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': self.cse_uuid
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        # Adding response.data will print the exact server error in your terminal!
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(response.data['success'])
        self.assertEqual(response.data['message'], 'Ticket updated successfully')
        self.assertEqual(response.data['updated_by'], self.email)
        self.assertIn('assigned_to_id', response.data['updated_fields'])
        
        # Verify ticket was updated
        updated_ticket = SupportTicket.objects.get(id=self.support_ticket.id)
        self.assertEqual(str(updated_ticket.assigned_to.id), self.cse_uuid)
        
    def test_update_multiple_fields_success(self):
        """Test successful update of multiple fields"""
        data = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': self.cse_uuid,
            'cse_name': 'john.doe@company.com',
            'resolution_status': 'In Progress',
            'cse_remarks': 'Working on this issue',
            'call_status': 'In Progress',
            'layout_status': 'assigned'
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['success'])
        self.assertEqual(len(response.data['updated_fields']), 6)
        
        # Verify all fields were updated
        updated_ticket = SupportTicket.objects.get(id=self.support_ticket.id)
        self.assertEqual(str(updated_ticket.assigned_to.id), self.cse_uuid)
        self.assertEqual(updated_ticket.cse_name, 'john.doe@company.com')
        self.assertEqual(updated_ticket.resolution_status, 'In Progress')
        self.assertEqual(updated_ticket.cse_remarks, 'Working on this issue')
        self.assertEqual(updated_ticket.call_status, 'In Progress')
        self.assertEqual(updated_ticket.layout_status, 'assigned')
        
    def test_update_snooze_until_success(self):
        """Test successful update of snooze_until field"""
        future_time = timezone.now() + timedelta(hours=2)
        
        data = {
            'ticket_id': self.support_ticket.id,
            'snooze_until': future_time.isoformat()
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify snooze_until was updated
        updated_ticket = SupportTicket.objects.get(id=self.support_ticket.id)
        self.assertIsNotNone(updated_ticket.snooze_until)
        
    def test_update_assigned_to_null(self):
        """Test updating assigned_to to null (unassigning ticket)"""
        # Change the assignment to use a real SupabaseAuthUser
        dummy_user = SupabaseAuthUser.objects.create(id=uuid.uuid4(), email="dummy1@example.com")
        self.support_ticket.assigned_to = dummy_user
        self.support_ticket.save()
        
        data = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': None
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify ticket was unassigned
        updated_ticket = SupportTicket.objects.get(id=self.support_ticket.id)
        self.assertIsNone(updated_ticket.assigned_to)
        
    def test_ticket_not_found(self):
        """Test handling of non-existent ticket"""
        data = {
            'ticket_id': 99999,  # Non-existent ticket
            'assigned_to': self.cse_uuid
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('Ticket not found', str(response.data))
        
    def test_missing_ticket_id(self):
        """Test handling of missing ticket_id"""
        data = {
            'assigned_to': self.cse_uuid
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('Invalid request data', str(response.data))
        self.assertIn('ticket_id', str(response.data))
        
    def test_no_fields_provided(self):
        """Test handling when no update fields are provided"""
        data = {
            'ticket_id': self.support_ticket.id
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('At least one field must be provided for update', str(response.data))
        
    def test_invalid_uuid_format(self):
        """Test handling of invalid UUID format for assigned_to"""
        data = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': 'invalid-uuid-format'
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('Invalid request data', str(response.data))
        
    def test_unauthorized_request(self):
        """Test handling of unauthorized requests"""
        data = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': self.cse_uuid
        }
        
        # Make request without auth headers
        response = self.client.patch(self.url, data, format='json')
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        
    def test_invalid_auth_header(self):
        """Test handling of invalid auth header"""
        data = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': self.cse_uuid
        }
        
        # Make request with invalid auth header
        invalid_headers = {
            'HTTP_AUTHORIZATION': 'Bearer invalid_token',
            'HTTP_X_TENANT_ID': self.tenant_id
        }
        
        response = self.client.patch(self.url, data, format='json', **invalid_headers)
        
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        
    def test_options_request(self):
        """Test CORS preflight OPTIONS request"""
        # Add HTTP_ORIGIN to trick Django into returning CORS headers
        response = self.client.options(
            self.url, 
            HTTP_ORIGIN='http://localhost:3000', 
            **self.auth_headers
        )
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response['Access-Control-Allow-Origin'], '*')
        self.assertIn('PATCH', response['Access-Control-Allow-Methods'])
        
    def test_empty_string_fields(self):
        """Test handling of empty string values"""
        data = {
            'ticket_id': self.support_ticket.id,
            'cse_name': '',
            'cse_remarks': '',
            'call_status': ''
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify empty strings were saved
        updated_ticket = SupportTicket.objects.get(id=self.support_ticket.id)
        self.assertEqual(updated_ticket.cse_name, '')
        self.assertEqual(updated_ticket.cse_remarks, '')
        self.assertEqual(updated_ticket.call_status, '')
        
    def test_long_field_values(self):
        """Test handling of long field values"""
        long_string = 'x' * 1000  # 1000 characters
        
        data = {
            'ticket_id': self.support_ticket.id,
            'cse_remarks': long_string
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify long string was saved
        updated_ticket = SupportTicket.objects.get(id=self.support_ticket.id)
        self.assertEqual(updated_ticket.cse_remarks, long_string)
        
    def test_update_response_structure(self):
        """Test that response has correct structure"""
        data = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': self.cse_uuid,
            'cse_name': 'test@example.com'
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Check response structure
        response_data = response.data
        self.assertIn('success', response_data)
        self.assertIn('message', response_data)
        self.assertIn('updated_ticket', response_data)
        self.assertIn('updated_by', response_data)
        self.assertIn('updated_fields', response_data)
        
        # Check updated_ticket structure
        updated_ticket = response_data['updated_ticket']
        self.assertIn('id', updated_ticket)
        self.assertIn('assigned_to', updated_ticket)
        self.assertIn('cse_name', updated_ticket)
        
        # Verify updated fields list
        self.assertIn('assigned_to_id', response_data['updated_fields'])
        self.assertIn('cse_name', response_data['updated_fields'])
        
    def test_concurrent_updates(self):
        """Test handling of concurrent updates to the same ticket"""
        # 1. Create real users first
        user1 = SupabaseAuthUser.objects.create(id=uuid.uuid4(), email="user1@example.com")
        user2 = SupabaseAuthUser.objects.create(id=uuid.uuid4(), email="user2@example.com")

        # 2. Use their real IDs in the payloads
        data1 = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': str(user1.id), # <--- Use real ID
            'cse_name': 'user1@example.com'
        }
        
        data2 = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': str(user2.id), # <--- Use real ID
            'cse_remarks': 'Updated by user 2'
        }
        
        # Make both requests
        response1 = self.client.patch(self.url, data1, format='json', **self.auth_headers)
        response2 = self.client.patch(self.url, data2, format='json', **self.auth_headers)
        
        # Both should succeed
        self.assertEqual(response1.status_code, status.HTTP_200_OK)
        self.assertEqual(response2.status_code, status.HTTP_200_OK)
        
        # Verify final state (last update should win)
        updated_ticket = SupportTicket.objects.get(id=self.support_ticket.id)
        self.assertEqual(str(updated_ticket.assigned_to.id), data2['assigned_to'])
        self.assertEqual(updated_ticket.cse_name, data1['cse_name'])  # From first update
        self.assertEqual(updated_ticket.cse_remarks, data2['cse_remarks'])  # From second update
        
    def test_update_with_existing_assigned_ticket(self):
        """Test updating a ticket that's already assigned to someone else"""
        
        # 1. Create a real user and assign ticket to them first
        original_cse_user = SupabaseAuthUser.objects.create(id=uuid.uuid4(), email="original@example.com")
        self.support_ticket.assigned_to = original_cse_user
        self.support_ticket.cse_name = 'original@example.com'
        self.support_ticket.save()
        
        # 👇 2. CREATE A REAL USER FOR THE NEW ASSIGNEE 👇
        new_cse_user = SupabaseAuthUser.objects.create(id=uuid.uuid4(), email="new@example.com")
        
        # 3. Use the REAL user's ID in the payload, not a random string
        data = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': str(new_cse_user.id), 
            'cse_name': 'new@example.com'
        }
        
        response = self.client.patch(self.url, data, format='json', **self.auth_headers)
        
        # Should succeed (admin can reassign)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify reassignment (Note: add .id to assigned_to here to compare strings properly)
        updated_ticket = SupportTicket.objects.get(id=self.support_ticket.id)
        self.assertEqual(str(updated_ticket.assigned_to.id), str(new_cse_user.id))
        self.assertEqual(updated_ticket.cse_name, 'new@example.com')


class SupportTicketUpdateSerializerTest(BaseAPITestCase):
    """Test the SupportTicketUpdateSerializer independently"""
    
    def setUp(self):
        """Set up test data"""
        super().setUp()
        self.support_ticket = SupportTicketFactory.create(
            tenant_id=self.tenant_id
        )
        
    def test_valid_data(self):
        """Test serializer with valid data"""
        from support_ticket.serializers import SupportTicketUpdateSerializer
        
        data = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': str(uuid.uuid4()),
            'cse_name': 'test@example.com'
        }
        
        serializer = SupportTicketUpdateSerializer(data=data)
        self.assertTrue(serializer.is_valid())
        
    def test_invalid_ticket_id(self):
        """Test serializer with invalid ticket_id"""
        from support_ticket.serializers import SupportTicketUpdateSerializer
        
        data = {
            'ticket_id': 99999,  # Non-existent
            'assigned_to': str(uuid.uuid4())
        }
        
        serializer = SupportTicketUpdateSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('ticket_id', serializer.errors)
        
    def test_no_update_fields(self):
        """Test serializer with no fields to update"""
        from support_ticket.serializers import SupportTicketUpdateSerializer
        
        data = {
            'ticket_id': self.support_ticket.id
        }
        
        serializer = SupportTicketUpdateSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('non_field_errors', serializer.errors)
        
    def test_invalid_uuid(self):
        """Test serializer with invalid UUID"""
        from support_ticket.serializers import SupportTicketUpdateSerializer
        
        data = {
            'ticket_id': self.support_ticket.id,
            'assigned_to': 'invalid-uuid'
        }
        
        serializer = SupportTicketUpdateSerializer(data=data)
        self.assertFalse(serializer.is_valid())
        self.assertIn('assigned_to', serializer.errors)
