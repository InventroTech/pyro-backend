import pytest
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient
from unittest.mock import patch
from uuid import uuid4

from core.models import Tenant
from support_ticket.models import SupportTicket
from tests.factories.core_factory import TenantFactory
from tests.factories.support_ticket_factory import SupportTicketFactory

# Use the Django DB marker to ensure each test runs with a clean database.
@pytest.mark.django_db
class TestTakeBreakView:

    @pytest.fixture(autouse=True)
    def bypass_tenant_auth(self):
        """
        Automatically bypasses the IsTenantAuthenticated permission check for all tests,
        but still correctly rejects unauthenticated users (for the 403 test).
        """
        def flexible_has_permission(*args, **kwargs):
            # Look through the arguments to find the 'request' object
            for arg in args:
                if hasattr(arg, 'user'):
                    return bool(arg.user and arg.user.is_authenticated)
            return False

        with patch('support_ticket.views.IsTenantAuthenticated.has_permission') as mock_perm:
            mock_perm.side_effect = flexible_has_permission
            yield

    @pytest.fixture
    def authenticated_client(self):
        """
        A pytest fixture to provide an authenticated API client.
        Using DRF's force_authenticate to bypass complex JWT mocking.
        """
        client = APIClient()
        user_id = str(uuid4())
        user_email = "test_user@example.com"
        
        # Create a dummy user that DRF will fully accept
        mock_user = type('MockUser', (object,), {
            'supabase_uid': user_id, 
            'email': user_email, 
            'is_authenticated': True,
            'is_active': True
        })()
        
        # This dictionary simulates the decoded JWT payload
        jwt_payload = {'sub': user_id, 'email': user_email}
        
        # Force the authentication
        client.force_authenticate(user=mock_user, token=jwt_payload)
        
        return client, user_id, user_email

    @pytest.fixture
    def test_ticket(self):
        """
        A pytest fixture to create a SupportTicket instance for testing.
        """
        # Use the factory to automatically generate a unique slug and ID
        tenant = TenantFactory() 
        
        return SupportTicketFactory(
            tenant=tenant,
            cse_name="existing_cse_name",
            assigned_to=None,
        )

    def test_take_break_unassigns_ticket_successfully(self, authenticated_client, test_ticket):
        client, user_id, user_email = authenticated_client
        url = reverse('support_ticket:take-break')
        
        # Set initial state to a non-WIP status to test unassignment.
        test_ticket.resolution_status = "Resolved"
        test_ticket.save()
        
        payload = {
            'ticketId': test_ticket.id,
            'resolutionStatus': 'Resolved'
        }
        
        response = client.post(url, payload, format='json')
        
        assert response.status_code == status.HTTP_200_OK
        assert response.json()['success'] is True
        assert "Ticket unassigned" in response.json()['message']
        assert response.json()['ticketUnassigned'] is True
        assert response.json()['userId'] == user_id
        assert response.json()['userEmail'] == user_email
        
        test_ticket.refresh_from_db()
        assert test_ticket.assigned_to is None
        assert test_ticket.cse_name is None
        assert test_ticket.resolution_status == "Resolved" 

    def test_take_break_does_not_unassign_wip_ticket(self, authenticated_client, test_ticket):
        client, user_id, user_email = authenticated_client
        url = reverse('support_ticket:take-break')
        
        test_ticket.resolution_status = "WIP"
        test_ticket.save()

        initial_assigned_to = test_ticket.assigned_to
        initial_cse_name = test_ticket.cse_name
        
        payload = {
            'ticketId': test_ticket.id,
            'resolutionStatus': 'WIP'
        }
        
        response = client.post(url, payload, format='json')
        
        assert response.status_code == status.HTTP_200_OK
        assert response.json()['success'] is True
        assert "in progress" in response.json()['message']
        assert response.json()['ticketUnassigned'] is False
        
        test_ticket.refresh_from_db()
        assert test_ticket.assigned_to == initial_assigned_to
        assert test_ticket.cse_name == initial_cse_name
        assert test_ticket.resolution_status == "WIP"

    def test_take_break_with_payload_wip_status(self, authenticated_client, test_ticket):
        client, user_id, user_email = authenticated_client
        url = reverse('support_ticket:take-break')
        
        test_ticket.resolution_status = "New"
        test_ticket.save()
        initial_assigned_to = test_ticket.assigned_to
        
        payload = {
            'ticketId': test_ticket.id,
            'resolutionStatus': 'WIP'
        }
        
        response = client.post(url, payload, format='json')
        
        assert response.status_code == status.HTTP_200_OK
        assert response.json()['success'] is True
        assert "in progress" in response.json()['message']
        assert response.json()['ticketUnassigned'] is False
        
        test_ticket.refresh_from_db()
        assert test_ticket.assigned_to == initial_assigned_to
        
    def test_take_break_with_non_existent_ticket(self, authenticated_client):
        client, _, _ = authenticated_client
        url = reverse('support_ticket:take-break')
        
        payload = {
            'ticketId': str(uuid4()), # <--- Gives a perfectly valid UUID that doesn't exist!
            'resolutionStatus': 'Resolved'
        }
        
        response = client.post(url, payload, format='json')
        
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "Ticket not found" in response.json()['error']

    def test_take_break_with_missing_ticket_id(self, authenticated_client):
        client, _, _ = authenticated_client
        url = reverse('support_ticket:take-break')
        
        payload = {
            'resolutionStatus': 'Resolved'
        }
        
        response = client.post(url, payload, format='json')
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid request data" in response.json()['error']
        assert "ticketId" in response.json()['details']

    def test_take_break_without_authentication(self, test_ticket):
        client = APIClient()
        url = reverse('support_ticket:take-break')
        
        payload = {
            'ticketId': test_ticket.id,
            'resolutionStatus': 'Resolved'
        }
        
        response = client.post(url, payload, format='json')
        
        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_take_break_with_no_user_id_in_token(self, test_ticket):
        client = APIClient()
        url = reverse('support_ticket:take-break')

        mock_user = type('MockUser', (object,), {
            'is_authenticated': True,
            'is_active': True,
            'email': 'test@example.com',
            'supabase_uid': None  # Explicitly None to trigger the 400 error in the view
        })()
        
        bad_jwt_payload = {'email': "test@example.com"}
        client.force_authenticate(user=mock_user, token=bad_jwt_payload)
            
        payload = {
            'ticketId': test_ticket.id,
            'resolutionStatus': 'Resolved'
        }
        
        response = client.post(url, payload, format='json')
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "No user id in JWT" in response.json()['error']