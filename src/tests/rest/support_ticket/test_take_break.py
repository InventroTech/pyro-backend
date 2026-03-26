import pytest
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient
from uuid import uuid4
from unittest.mock import patch

from core.models import Tenant
from support_ticket.models import SupportTicket
from tests.factories.user_factory import UserFactory
from tests.factories.core_factory import TenantFactory
from tests.factories.support_ticket_factory import SupportTicketFactory

@pytest.mark.django_db
class TestTakeBreakView:

    @pytest.fixture(autouse=True)
    def mock_membership_info(self):
        """
        Simulates the middleware data extraction for the permission class.
        Returns None for unauthenticated users so 403 tests still pass!
        """
        def fake_get_membership_info(request):
            if not request.user or not request.user.is_authenticated:
                return None
            return {'role_key': 'CSE'}

        with patch('authz.permissions._get_membership_info', side_effect=fake_get_membership_info):
            yield

    @pytest.fixture
    def authenticated_client(self):
        from authz.models import Role, TenantMembership
        
        # 1. Establish the Tenant
        self.test_tenant = TenantFactory.create(slug=f"tenant-{uuid4().hex[:6]}")
        
    @pytest.fixture(autouse=True)
    def inject_tenant_context(self):
        """
        2. Intercept DRF's request initialization to simulate Middleware.
        This attaches the tenant BEFORE the real permission class checks it.
        """
        from support_ticket.views import TakeBreakView
        from rest_framework.views import APIView
        
        original_initial = APIView.initial

        def mock_initial(view_instance, request, *args, **kwargs):
            # Attach the tenant exactly like your production middleware does
            request.tenant = self.test_tenant
            
            # Now run the REAL DRF initialization (which includes check_permissions)
            return original_initial(view_instance, request, *args, **kwargs)

        # Patching 'initial' avoids ALL the signature TypeErrors of check_permissions
        with patch('support_ticket.views.TakeBreakView.initial', new=mock_initial):
            yield

    @pytest.fixture
    def authenticated_client(self):
        from authz.models import Role, TenantMembership
        client = APIClient()
        
        # 1. Establish the Tenant
        self.test_tenant = TenantFactory.create(slug=f"tenant-{uuid4().hex[:6]}")
        
        # 2. Establish the Role (Customer Support Executive)
        role_obj, _ = Role.objects.get_or_create(
            tenant=self.test_tenant, 
            key="CSE", 
            defaults={"name": "Customer Support Executive"}
        )
        
        # 3. Establish the User
        user = UserFactory.create(
            email=f"user-{uuid4().hex[:6]}@example.com",
            supabase_uid=str(uuid4()),
            is_active=True
        )
        # Custom auth backends sometimes attach the tenant directly to the user object
        user.tenant = self.test_tenant
        user.tenant_id = self.test_tenant.id

        # 4. Create the DB Membership
        TenantMembership.objects.update_or_create(
            tenant=self.test_tenant,
            user_id=user.supabase_uid,
            defaults={'role': role_obj}
        )

        # 5. The "Kitchen Sink" JWT Token
        # Multi-tenant apps often store the tenant_id inside the JWT claims
        mock_token = {
            'email': user.email, 
            'sub': user.supabase_uid,
            'role': 'authenticated',
            'app_metadata': {'tenant_id': str(self.test_tenant.id)},
            'user_metadata': {'tenant_id': str(self.test_tenant.id)},
            'tenant_id': str(self.test_tenant.id),
        }
        
        # 6. Authenticate
        client.force_authenticate(user=user, token=mock_token)
        
        # 7. Global Client Headers
        # This guarantees every request fired by this client has the headers attached
        client.defaults['HTTP_X_TENANT_ID'] = str(self.test_tenant.id)
        client.defaults['HTTP_TENANT_ID'] = str(self.test_tenant.id)
        client.defaults['HTTP_X_TENANT_SLUG'] = self.test_tenant.slug
        
        return client, user.supabase_uid, user.email

    @pytest.fixture
    def test_ticket(self, authenticated_client):
        return SupportTicketFactory(
            tenant=self.test_tenant,
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
            'ticketId': 99999, # Back to an integer that doesn't exist
            'resolutionStatus': 'Resolved'
        }
        
        response = client.post(url, payload, format='json')
        
        # Change these assertions to expect the 400 Bad Request the serializer throws
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid request data" in response.json()['error']

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

        # 1. Create a perfectly valid user in the DB (No IntegrityErrors!)
        user = UserFactory.create(email='test@example.com', is_active=True)
        
        # 2. 👇 The Magic Trick: Wipe the UID from the object in memory!
        user.supabase_uid = None 
        
        # 3. Authenticate with our tampered user
        bad_jwt_payload = {'email': "test@example.com"} 
        client.force_authenticate(user=user, token=bad_jwt_payload)
            
        payload = {
            'ticketId': test_ticket.id,
            'resolutionStatus': 'Resolved'
        }
        
        response = client.post(url, payload, format='json')
        
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "No user id in JWT" in response.json()['error']