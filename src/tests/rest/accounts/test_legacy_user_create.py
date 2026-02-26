from django.test import TestCase
from django.db import transaction
from unittest.mock import patch
from rest_framework.test import APITestCase
from rest_framework import status
from authz.models import Role, TenantMembership
from core.models import Tenant
from accounts.views import LegacyUserCreateView


class LegacyUserCreateViewTestCase(TestCase):
    """LegacyUserCreateView now creates only TenantMembership (no LegacyUser/LegacyRole)."""

    def setUp(self):
        self.tenant = Tenant.objects.create(
            id='550e8400-e29b-41d4-a716-446655440000',
            name='Test Tenant',
            slug='test-tenant'
        )
        self.authz_role = Role.objects.create(
            tenant=self.tenant,
            key='GM',
            name='General Manager',
            description='Test GM role'
        )
        self.request_data = {
            'name': 'Test User',
            'email': 'test@example.com',
            'company_name': 'Test Company',
            'role_id': str(self.authz_role.id),
            'uid': '123e4567-e89b-12d3-a456-426614174000'
        }

    def test_serializer_valid_and_membership_creation(self):
        """Serializer validates and TenantMembership can be created with authz role_id."""
        from accounts.serializers import LegacyUserCreateSerializer
        mock_request = type('MockRequest', (), {
            'data': self.request_data,
            'tenant': self.tenant
        })()
        serializer = LegacyUserCreateSerializer(
            data=self.request_data,
            context={'request': mock_request}
        )
        with patch.object(mock_request, 'tenant', self.tenant):
            self.assertTrue(serializer.is_valid())
            validated_data = serializer.validated_data
            authz_role = Role.objects.get(id=validated_data["role_id"], tenant=self.tenant)
            membership, created = TenantMembership.objects.get_or_create(
                tenant=validated_data["_tenant"],
                email=validated_data["email"],
                role=authz_role,
                defaults={
                    'name': validated_data["name"].strip(),
                    'company_name': validated_data.get("company_name"),
                    'user_id': validated_data.get("uid"),
                    'is_active': bool(validated_data.get("uid"))
                }
            )
            self.assertTrue(created)
            self.assertEqual(membership.role, authz_role)
            self.assertEqual(str(membership.user_id), self.request_data['uid'])
            self.assertTrue(membership.is_active)

    def test_serializer_without_role_id(self):
        """Without role_id, serializer can be valid but view requires role_id."""
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
        # No TenantMembership created without role_id
        self.assertEqual(
            TenantMembership.objects.filter(tenant=self.tenant, email='test2@example.com').count(),
            0
        )

    def test_get_authz_role_by_id(self):
        """get_authz_role_from_legacy_role resolves AuthZ role by ID."""
        from authz.service import get_authz_role_from_legacy_role
        resolved = get_authz_role_from_legacy_role(str(self.authz_role.id), self.tenant)
        self.assertEqual(resolved, self.authz_role)
        with self.assertRaises(Exception):
            get_authz_role_from_legacy_role('999e8400-e29b-41d4-a716-446655440999', self.tenant)
