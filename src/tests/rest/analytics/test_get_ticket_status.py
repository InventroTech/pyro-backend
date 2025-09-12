from django.urls import reverse
from django.utils import timezone
from django.conf import settings
from datetime import datetime, timedelta
import jwt
from tests.base.test_setup import BaseAPITestCase
from tests.factories.support_ticket_factory import SupportTicketFactory
from analytics.models import SupportTicket


class TestGetTicketStatusView(BaseAPITestCase):
    """Test cases for the get-ticket-status API endpoint."""

    def setUp(self):
        super().setUp()
        # Create test tickets with different statuses and assignments
        self.create_test_tickets()

    def create_test_tickets(self):
        """Create various test tickets for different scenarios."""
        today = timezone.now().date()
        yesterday = today - timedelta(days=1)
        
        # Tickets assigned to current user - resolved today
        SupportTicketFactory.create_batch(
            3,
            assigned_to=self.supabase_uid,
            resolution_status='Resolved',
            completed_at=datetime.combine(today, datetime.min.time()),
            tenant_id=self.tenant_id
        )
        
        # Tickets assigned to current user - resolved yesterday (should not count)
        SupportTicketFactory.create_batch(
            2,
            assigned_to=self.supabase_uid,
            resolution_status='Resolved',
            completed_at=datetime.combine(yesterday, datetime.min.time()),
            tenant_id=self.tenant_id
        )
        
        # Tickets assigned to current user - WIP status
        SupportTicketFactory.create_batch(
            2,
            assigned_to=self.supabase_uid,
            resolution_status='WIP',
            tenant_id=self.tenant_id
        )
        
        # Tickets assigned to current user - Can't Resolve today
        SupportTicketFactory.create_batch(
            1,
            assigned_to=self.supabase_uid,
            resolution_status="Can't Resolve",
            completed_at=datetime.combine(today, datetime.min.time()),
            tenant_id=self.tenant_id
        )
        
        # Pending tickets (resolution_status is null) - not assigned to current user
        SupportTicketFactory.create_batch(
            5,
            resolution_status=None,
            poster='paid',
            tenant_id=self.tenant_id
        )
        
        # More pending tickets with different poster
        SupportTicketFactory.create_batch(
            3,
            resolution_status=None,
            poster='in_trial',
            tenant_id=self.tenant_id
        )
        
        # Tickets assigned to other users (should not count in user-specific stats)
        other_user_uid = "other-user-uid"
        SupportTicketFactory.create_batch(
            4,
            assigned_to=other_user_uid,
            resolution_status='Resolved',
            completed_at=datetime.combine(today, datetime.min.time()),
            tenant_id=self.tenant_id
        )

    def test_get_ticket_status_success(self):
        """Test successful API response with correct ticket counts."""
        url = reverse("analytics:get-ticket-status")
        response = self.client.get(url, **self.auth_headers)
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        # Check response structure
        self.assertTrue(data['success'])
        self.assertIn('ticketStats', data)
        self.assertIn('dateRange', data)
        
        # Check ticket statistics
        stats = data['ticketStats']
        self.assertEqual(stats['resolvedByYouToday'], 3)  # Only today's resolved tickets
        self.assertEqual(stats['totalPendingTickets'], 8)  # All pending tickets
        self.assertEqual(stats['wipTickets'], 2)  # WIP tickets assigned to user
        self.assertEqual(stats['cantResolveToday'], 1)  # Can't resolve today
        self.assertEqual(stats['totalTickets'], 20)  # All tickets created
        
        # Check pending by poster breakdown
        pending_by_poster = stats['pendingByPoster']
        self.assertEqual(len(pending_by_poster), 2)
        
        # Should be sorted by count (descending)
        self.assertEqual(pending_by_poster[0]['poster'], 'paid')
        self.assertEqual(pending_by_poster[0]['count'], 5)
        self.assertEqual(pending_by_poster[1]['poster'], 'in_trial')
        self.assertEqual(pending_by_poster[1]['count'], 3)

    def test_get_ticket_status_date_range(self):
        """Test that date range is correctly formatted."""
        url = reverse("analytics:get-ticket-status")
        response = self.client.get(url, **self.auth_headers)
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        # Check date range format
        date_range = data['dateRange']
        self.assertIn('startOfDay', date_range)
        self.assertIn('endOfDay', date_range)
        
        # Verify date range is for today
        start_of_day = datetime.fromisoformat(date_range['startOfDay'].replace('Z', '+00:00'))
        end_of_day = datetime.fromisoformat(date_range['endOfDay'].replace('Z', '+00:00'))
        
        today = timezone.now().date()
        self.assertEqual(start_of_day.date(), today)
        self.assertEqual(end_of_day.date(), today)

    def test_get_ticket_status_no_tickets(self):
        """Test API response when no tickets exist."""
        # Clear all tickets
        SupportTicket.objects.all().delete()
        
        url = reverse("analytics:get-ticket-status")
        response = self.client.get(url, **self.auth_headers)
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        stats = data['ticketStats']
        self.assertEqual(stats['resolvedByYouToday'], 0)
        self.assertEqual(stats['totalPendingTickets'], 0)
        self.assertEqual(stats['wipTickets'], 0)
        self.assertEqual(stats['cantResolveToday'], 0)
        self.assertEqual(stats['totalTickets'], 0)
        self.assertEqual(stats['pendingByPoster'], [])

    def test_get_ticket_status_no_user_tickets(self):
        """Test API response when user has no assigned tickets."""
        # Delete tickets assigned to current user
        SupportTicket.objects.filter(assigned_to=self.supabase_uid).delete()
        
        url = reverse("analytics:get-ticket-status")
        response = self.client.get(url, **self.auth_headers)
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        stats = data['ticketStats']
        self.assertEqual(stats['resolvedByYouToday'], 0)
        self.assertEqual(stats['wipTickets'], 0)
        self.assertEqual(stats['cantResolveToday'], 0)
        
        # Total pending and total tickets should still show
        self.assertEqual(stats['totalPendingTickets'], 8)
        self.assertEqual(stats['totalTickets'], 8)

    def test_get_ticket_status_authentication_required(self):
        """Test that authentication is required."""
        url = reverse("analytics:get-ticket-status")
        response = self.client.get(url)  # No auth headers
        
        self.assertEqual(response.status_code, 401)

    def test_get_ticket_status_invalid_token(self):
        """Test API response with invalid token."""
        url = reverse("analytics:get-ticket-status")
        response = self.client.get(url, HTTP_AUTHORIZATION="Bearer invalid-token")
        
        self.assertEqual(response.status_code, 401)

    def test_get_ticket_status_user_without_supabase_uid(self):
        """Test API response when user has no supabase_uid."""
        # Create user without supabase_uid
        user_without_uid = self.create_test_user(supabase_uid=None)
        token = self.generate_supabase_jwt_for_user(user_without_uid)
        
        url = reverse("analytics:get-ticket-status")
        response = self.client.get(url, HTTP_AUTHORIZATION=f"Bearer {token}")
        
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertIn('error', data)
        self.assertIn('supabase_uid not found', data['error'])

    def test_get_ticket_status_pending_by_poster_edge_cases(self):
        """Test pending by poster with edge cases."""
        # Clear existing tickets
        SupportTicket.objects.all().delete()
        
        # Create tickets with null poster
        SupportTicketFactory.create_batch(
            2,
            resolution_status=None,
            poster=None,
            tenant_id=self.tenant_id
        )
        
        # Create tickets with empty poster
        SupportTicketFactory.create_batch(
            1,
            resolution_status=None,
            poster='',
            tenant_id=self.tenant_id
        )
        
        url = reverse("analytics:get-ticket-status")
        response = self.client.get(url, **self.auth_headers)
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        stats = data['ticketStats']
        # Should only include tickets with non-null, non-empty poster
        self.assertEqual(len(stats['pendingByPoster']), 0)
        self.assertEqual(stats['totalPendingTickets'], 3)

    def test_get_ticket_status_different_tenant_isolation(self):
        """Test that tickets from different tenants are isolated."""
        other_tenant_id = "other-tenant-id"
        
        # Create tickets for other tenant
        SupportTicketFactory.create_batch(
            5,
            assigned_to=self.supabase_uid,
            resolution_status='Resolved',
            tenant_id=other_tenant_id
        )
        
        url = reverse("analytics:get-ticket-status")
        response = self.client.get(url, **self.auth_headers)
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        
        stats = data['ticketStats']
        # Should only count tickets from current tenant
        self.assertEqual(stats['resolvedByYouToday'], 3)  # Original tickets
        self.assertEqual(stats['totalTickets'], 20)  # Original tickets

    def generate_supabase_jwt_for_user(self, user):
        """Generate JWT token for a specific user."""
        payload = {
            "sub": user.supabase_uid,
            "email": user.email,
            "tenant_id": user.tenant_id,
            "role": user.role,
            "aud": "authenticated"
        }
        token = jwt.encode(payload, settings.SUPABASE_JWT_SECRET, algorithm="HS256")
        if isinstance(token, bytes):
            token = token.decode("utf-8")
        return token
