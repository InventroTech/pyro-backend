from django.test import TestCase
from django.db import transaction
from unittest.mock import patch
from authz.service import link_user_uid_and_activate
from authz.models import TenantMembership, Role, Permission
from core.models import Tenant


class LinkUserUidTestCase(TestCase):
    def setUp(self):
        # Create a test tenant
        self.tenant = Tenant.objects.create(
            id='550e8400-e29b-41d4-a716-446655440000',
            name='Test Tenant',
            slug='test-tenant'
        )
        
        # Create a test role
        self.role = Role.objects.create(
            tenant=self.tenant,
            key='AGENT',
            name='Agent',
            description='Test agent role'
        )
        
        # Create a test tenant membership (no LegacyUser; link targets TenantMembership only)
        self.membership = TenantMembership.objects.create(
            tenant=self.tenant,
            email='test@example.com',
            role=self.role,
            is_active=False
        )

    def test_link_user_uid_success(self):
        """Test successful UID linking"""
        uid = '123e4567-e89b-12d3-a456-426614174000'
        
        result = link_user_uid_and_activate('test@example.com', uid)
        
        self.assertTrue(result['success'])
        self.assertEqual(result['uid'], uid)
        self.assertEqual(result['activated_memberships'], 1)
        self.assertIn('authz_tenantmembership', result['tables_updated'])
        
        # Verify membership was activated and linked
        self.membership.refresh_from_db()
        self.assertEqual(self.membership.user_id, uid)
        self.assertTrue(self.membership.is_active)

    def test_link_user_uid_user_not_found(self):
        """Test when email has no TenantMembership"""
        uid = '123e4567-e89b-12d3-a456-426614174000'
        
        result = link_user_uid_and_activate('nonexistent@example.com', uid)
        
        self.assertTrue(result['success'])
        self.assertEqual(result['activated_memberships'], 0)
        self.assertTrue(result.get('no_tenant_membership') or 'not found' in result.get('message', '').lower())

    def test_link_user_uid_no_memberships(self):
        """Test when user exists but has no memberships"""
        uid = '123e4567-e89b-12d3-a456-426614174000'
        
        # Delete the membership
        self.membership.delete()
        
        result = link_user_uid_and_activate('test@example.com', uid)
        
        self.assertTrue(result['success'])
        self.assertEqual(result['activated_memberships'], 0)
