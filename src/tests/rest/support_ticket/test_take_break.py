import pytest
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient
from unittest.mock import patch
from model_bakery import baker
from uuid import uuid4

from analytics.models import SupportTicket

# Use the Django DB marker to ensure each test runs with a clean database.
@pytest.mark.django_db
class TestTakeBreakView:

    @pytest.fixture
    def authenticated_client(self):
        """
        A pytest fixture to provide an authenticated API client.
        We mock the SupabaseJWTAuthentication backend to simulate a successful login
        without needing a real JWT token.
        """
        client = APIClient()
        user_id = str(uuid4())
        user_email = "test_user@example.com"
        
        # We need to patch the authentication backend to bypass actual JWT validation.
        # This is a standard practice in unit testing to isolate the code being tested.
        with patch('config.supabase_auth.SupabaseJWTAuthentication.authenticate') as mock_authenticate:
            # The mock should return a tuple of (user, jwt_claims)
            mock_user = type('MockUser', (object,), {'supabase_uid': user_id, 'email': user_email})()
            mock_authenticate.return_value = (mock_user, {'sub': user_id, 'email': user_email})
            client.credentials(HTTP_AUTHORIZATION=f'Bearer fake-jwt-token')
            return client, user_id, user_email

    @pytest.fixture
    def test_ticket(self):
        """
        A pytest fixture to create a SupportTicket instance for testing.
        """
        # Create a SupportTicket using baker, ensuring it has all the necessary fields.
        ticket = baker.make(
            SupportTicket,
            assigned_to=uuid4(),
            cse_name="existing_cse_name"
        )
        return ticket

    def test_take_break_unassigns_ticket_successfully(self, authenticated_client, test_ticket):
        """
        Test that the API successfully unassigns a ticket when the resolution status
        is not "WIP".
        """
        client, user_id, user_email = authenticated_client
        url = reverse('support_ticket:take-break')
        
        # Set initial state to a non-WIP status to test unassignment.
        test_ticket.resolution_status = "Resolved"
        test_ticket.save()
        
        payload = {
            'ticketId': test_ticket.id,
            'resolutionStatus': 'Resolved' # Or any status other than "WIP"
        }
        
        response = client.post(url, payload, format='json')
        
        # Assert the HTTP status code is 200 OK.
        assert response.status_code == status.HTTP_200_OK
        
        # Assert the response payload content.
        assert response.json()['success'] is True
        assert "Ticket unassigned" in response.json()['message']
        assert response.json()['ticketUnassigned'] is True
        assert response.json()['userId'] == user_id
        assert response.json()['userEmail'] == user_email
        
        # Fetch the ticket from the database to confirm the update.
        test_ticket.refresh_from_db()
        assert test_ticket.assigned_to is None
        assert test_ticket.cse_name is None
        # Resolution status should not change from this API call
        assert test_ticket.resolution_status == "Resolved" 

    def test_take_break_does_not_unassign_wip_ticket(self, authenticated_client, test_ticket):
        """
        Test that the API does not unassign a ticket when its current status is "WIP".
        """
        client, user_id, user_email = authenticated_client
        url = reverse('support_ticket:take-break')
        
        # Set the initial ticket status to "WIP".
        test_ticket.resolution_status = "WIP"
        test_ticket.save()

        # Save the initial assigned values for comparison.
        initial_assigned_to = test_ticket.assigned_to
        initial_cse_name = test_ticket.cse_name
        
        payload = {
            'ticketId': test_ticket.id,
            'resolutionStatus': 'WIP'
        }
        
        response = client.post(url, payload, format='json')
        
        # Assert the HTTP status code is 200 OK.
        assert response.status_code == status.HTTP_200_OK
        
        # Assert the response payload content.
        assert response.json()['success'] is True
        assert "in progress" in response.json()['message']
        assert response.json()['ticketUnassigned'] is False
        
        # Fetch the ticket from the database to confirm it was NOT updated.
        test_ticket.refresh_from_db()
        assert test_ticket.assigned_to == initial_assigned_to
        assert test_ticket.cse_name == initial_cse_name
        assert test_ticket.resolution_status == "WIP"

    def test_take_break_with_payload_wip_status(self, authenticated_client, test_ticket):
        """
        Test that the API does not unassign a ticket when the payload sends "WIP" status,
        even if the ticket's current status is not "WIP".
        """
        client, user_id, user_email = authenticated_client
        url = reverse('support_ticket:take-break')
        
        # Set initial status to "New", then send a payload with "WIP"
        test_ticket.resolution_status = "New"
        test_ticket.save()
        initial_assigned_to = test_ticket.assigned_to
        
        payload = {
            'ticketId': test_ticket.id,
            'resolutionStatus': 'WIP'
        }
        
        response = client.post(url, payload, format='json')
        
        # Assert the HTTP status code is 200 OK.
        assert response.status_code == status.HTTP_200_OK
        
        # Assert the response payload content.
        assert response.json()['success'] is True
        assert "in progress" in response.json()['message']
        assert response.json()['ticketUnassigned'] is False
        
        # Confirm that the ticket remains assigned.
        test_ticket.refresh_from_db()
        assert test_ticket.assigned_to == initial_assigned_to
        
    def test_take_break_with_non_existent_ticket(self, authenticated_client):
        """
        Test that the API returns a 404 Not Found error for a non-existent ticket.
        """
        client, _, _ = authenticated_client
        url = reverse('support_ticket:take-break')
        
        payload = {
            'ticketId': 9999, # An ID that does not exist
            'resolutionStatus': 'Resolved'
        }
        
        response = client.post(url, payload, format='json')
        
        # Assert the HTTP status code is 404 Not Found.
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "Ticket not found" in response.json()['error']

    def test_take_break_with_missing_ticket_id(self, authenticated_client):
        """
        Test that the API returns a 400 Bad Request if the ticketId is missing.
        """
        client, _, _ = authenticated_client
        url = reverse('support_ticket:take-break')
        
        payload = {
            'resolutionStatus': 'Resolved'
        }
        
        response = client.post(url, payload, format='json')
        
        # Assert the HTTP status code is 400 Bad Request.
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid request data" in response.json()['error']
        assert "ticketId" in response.json()['details']

    def test_take_break_without_authentication(self, test_ticket):
        """
        Test that an unauthenticated request is rejected with 401 Unauthorized.
        """
        client = APIClient()
        url = reverse('support_ticket:take-break')
        
        payload = {
            'ticketId': test_ticket.id,
            'resolutionStatus': 'Resolved'
        }
        
        response = client.post(url, payload, format='json')
        
        # Assert the HTTP status code is 401 Unauthorized.
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        assert "Missing or invalid auth header" in response.json()['error']

    def test_take_break_with_no_user_id_in_token(self, test_ticket):
        """
        Test that the API returns a 400 Bad Request if the JWT payload
        is missing the 'sub' (user ID) field.
        """
        client = APIClient()
        url = reverse('support_ticket:take-break')

        # Mock the authentication to provide a JWT payload without 'sub'
        with patch('config.supabase_auth.SupabaseJWTAuthentication.authenticate') as mock_authenticate:
            mock_authenticate.return_value = (None, {'email': "test@example.com"})
            client.credentials(HTTP_AUTHORIZATION='Bearer fake-jwt-token')
            
            payload = {
                'ticketId': test_ticket.id,
                'resolutionStatus': 'Resolved'
            }
            
            response = client.post(url, payload, format='json')
            
            # Assert the HTTP status code is 400 Bad Request.
            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert "No user id in JWT" in response.json()['error']