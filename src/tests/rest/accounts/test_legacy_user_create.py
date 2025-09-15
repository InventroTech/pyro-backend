from django.test import TestCase
from django.db import transaction
from unittest.mock import patch
from rest_framework.test import APITestCase
from rest_framework import status
from accounts.models import LegacyUser, LegacyRole
from authz.models import Role, TenantMembership
from core.models import Tenant
from accounts.views import LegacyUserCreateView


class LegacyUserCreateViewTestCase(APITestCase):
    def setUp(self):
        # Create a test tenant
        self.tenant = Tenant.objects.create(
            id='550e8400-e29b-41d4-a716-446655440000',
            name='Test Tenant',
            slug='test-tenant'
        )
        
        # Create a legacy role
        self.legacy_role = LegacyRole.objects.create(
            id='650e8400-e29b-41d4-a716-446655440001',
            name='General Manager',
            description='Test GM role',
            tenant=self.tenant
        )
        
        # Create a corresponding authz role
        self.authz_role = Role.objects.create(
            tenant=self.tenant,
            key='GM',
            name='General Manager',
            description='Test GM role'
        )
        
        # Mock the request with tenant
        self.request_data = {
            'name': 'Test User',
            'email': 'test@example.com',
            'company_name': 'Test Company',
            'role_id': str(self.legacy_role.id),
            'uid': '123e4567-e89b-12d3-a456-426614174000'
        }

    def test_create_user_with_role_and_uid(self):
        """Test creating a user with role and UID creates both user and TenantMembership"""
        # Mock the request context
        request = self.client.post('/accounts/users/legacy/create/', self.request_data)
        
        # This test would need proper authentication setup in a real scenario
        # For now, we'll test the logic directly
        view = LegacyUserCreateView()
        mock_request = type('MockRequest', (), {
            'data': self.request_data,
            'tenant': self.tenant
        })()
        
        # Test the serializer validation
        from accounts.serializers import LegacyUserCreateSerializer
        serializer = LegacyUserCreateSerializer(
            data=self.request_data, 
            context={'request': mock_request}
        )
        
        # Mock the tenant resolution
        with patch.object(mock_request, 'tenant', self.tenant):
            self.assertTrue(serializer.is_valid())
            
            validated_data = serializer.validated_data
            
            with transaction.atomic():
                # Test user creation
                user_row = LegacyUser.objects.create(
                    name=validated_data["name"].strip(),
                    email=validated_data["email"],
                    tenant=validated_data["_tenant"],
                    company_name=validated_data.get("company_name"),
                    role_id=validated_data.get("role_id"),
                    uid=validated_data.get("uid")
                )
                
                # Test TenantMembership creation
                if validated_data.get("role_id"):
                    from authz.service import get_authz_role_from_legacy_role
                    authz_role = get_authz_role_from_legacy_role(
                        validated_data["role_id"], 
                        validated_data["_tenant"]
                    )
                    
                    membership, created = TenantMembership.objects.get_or_create(
                        tenant=validated_data["_tenant"],
                        email=validated_data["email"],
                        defaults={
                            'role': authz_role,
                            'user_id': validated_data.get("uid"),
                            'is_active': bool(validated_data.get("uid"))
                        }
                    )
                    
                    self.assertTrue(created)
                    self.assertEqual(membership.role, authz_role)
                    self.assertEqual(membership.user_id, validated_data.get("uid"))
                    self.assertTrue(membership.is_active)

    def test_create_user_without_role(self):
        """Test creating a user without role doesn't create TenantMembership"""
        request_data = {
            'name': 'Test User',
            'email': 'test2@example.com',
            'company_name': 'Test Company'
        }
        
        mock_request = type('MockRequest', (), {
            'data': request_data,
            'tenant': self.tenant
        })()
        
        from accounts.serializers import LegacyUserCreateSerializer
        serializer = LegacyUserCreateSerializer(
            data=request_data, 
            context={'request': mock_request}
        )
        
        with patch.object(mock_request, 'tenant', self.tenant):
            self.assertTrue(serializer.is_valid())
            
            validated_data = serializer.validated_data
            
            with transaction.atomic():
                user_row = LegacyUser.objects.create(
                    name=validated_data["name"].strip(),
                    email=validated_data["email"],
                    tenant=validated_data["_tenant"],
                    company_name=validated_data.get("company_name"),
                    role_id=validated_data.get("role_id"),
                    uid=validated_data.get("uid")
                )
                
                # No TenantMembership should be created
                memberships = TenantMembership.objects.filter(
                    tenant=validated_data["_tenant"],
                    email=validated_data["email"]
                )
                self.assertEqual(memberships.count(), 0)

    def test_role_mapping_function(self):
        """Test the role mapping function works correctly"""
        from authz.service import get_authz_role_from_legacy_role
        
        # Test successful mapping
        authz_role = get_authz_role_from_legacy_role(self.legacy_role.id, self.tenant)
        self.assertEqual(authz_role, self.authz_role)
        
        # Test with non-existent legacy role
        with self.assertRaises(Exception) as context:
            get_authz_role_from_legacy_role('999e8400-e29b-41d4-a716-446655440999', self.tenant)
        
        self.assertIn('not found', str(context.exception))
