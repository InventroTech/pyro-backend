from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from django.shortcuts import get_object_or_404
from django.db import transaction

from authz.permissions import IsTenantAuthenticated, HasTenantRole
from .models import UserSettings
from .serializers import (
    UserSettingsSerializer, 
    UserSettingsCreateSerializer, 
    LeadTypeAssignmentSerializer
)
from accounts.models import LegacyUser


class UserSettingsListView(APIView):
    """List and create user settings for the current tenant"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        """Get all user settings for the current tenant"""
        tenant = request.tenant
        settings = UserSettings.objects.filter(tenant=tenant)
        serializer = UserSettingsSerializer(settings, many=True)
        return Response(serializer.data)

    def post(self, request):
        """Create a new user setting"""
        tenant = request.tenant
        serializer = UserSettingsCreateSerializer(data=request.data)
        
        if serializer.is_valid():
            # Check if setting already exists
            existing_setting = UserSettings.objects.filter(
                tenant=tenant,
                user_id=serializer.validated_data['user_id'],
                key=serializer.validated_data['key']
            ).first()
            
            if existing_setting:
                # Update existing setting
                existing_setting.value = serializer.validated_data['value']
                existing_setting.save()
                response_serializer = UserSettingsSerializer(existing_setting)
                return Response(response_serializer.data, status=status.HTTP_200_OK)
            else:
                # Create new setting
                setting = serializer.save(tenant=tenant)
                response_serializer = UserSettingsSerializer(setting)
                return Response(response_serializer.data, status=status.HTTP_201_CREATED)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserSettingsDetailView(APIView):
    """Retrieve, update or delete a specific user setting"""
    permission_classes = [IsTenantAuthenticated]

    def get_object(self, tenant, user_id, key):
        """Get a specific user setting"""
        return get_object_or_404(
            UserSettings,
            tenant=tenant,
            user_id=user_id,
            key=key
        )

    def get(self, request, user_id, key):
        """Get a specific user setting"""
        tenant = request.tenant
        setting = self.get_object(tenant, user_id, key)
        serializer = UserSettingsSerializer(setting)
        return Response(serializer.data)

    def put(self, request, user_id, key):
        """Update a specific user setting"""
        tenant = request.tenant
        setting = self.get_object(tenant, user_id, key)
        serializer = UserSettingsSerializer(setting, data=request.data, partial=True)
        
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, user_id, key):
        """Delete a specific user setting"""
        tenant = request.tenant
        setting = self.get_object(tenant, user_id, key)
        setting.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class LeadTypeAssignmentView(APIView):
    """Manage lead type assignments for users (GM functionality)"""
    permission_classes = [IsTenantAuthenticated, HasTenantRole("GM")]

    def get(self, request):
        """Get all lead type assignments for the tenant"""
        tenant = request.tenant
        
        # Get all users with RM role
        rm_users = LegacyUser.objects.filter(
            tenant=tenant,
            role__name__iexact='RM'
        ).select_related('role')
        
        assignments = []
        for user in rm_users:
            # Get lead type assignment for this user
            try:
                setting = UserSettings.objects.get(
                    tenant=tenant,
                    user_id=user.uid or user.id,
                    key='LEAD_TYPE_ASSIGNMENT'
                )
                lead_types = setting.value if isinstance(setting.value, list) else []
            except UserSettings.DoesNotExist:
                lead_types = []
            
            assignments.append({
                'user_id': str(user.uid or user.id),
                'user_name': user.name,
                'user_email': user.email,
                'lead_types': lead_types
            })
        
        return Response(assignments)

    def post(self, request):
        """Assign lead types to a user"""
        tenant = request.tenant
        serializer = LeadTypeAssignmentSerializer(data=request.data)
        
        if serializer.is_valid():
            user_id = serializer.validated_data['user_id']
            lead_types = serializer.validated_data['lead_types']
            
            # Verify user exists and has RM role
            try:
                user = LegacyUser.objects.get(
                    tenant=tenant,
                    uid=user_id
                )
                if not user.role or user.role.name.upper() != 'RM':
                    return Response(
                        {'error': 'User must have RM role to assign lead types'},
                        status=status.HTTP_400_BAD_REQUEST
                    )
            except LegacyUser.DoesNotExist:
                return Response(
                    {'error': 'User not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            
            # Create or update the setting
            setting, created = UserSettings.objects.get_or_create(
                tenant=tenant,
                user_id=user_id,
                key='LEAD_TYPE_ASSIGNMENT',
                defaults={'value': lead_types}
            )
            
            if not created:
                setting.value = lead_types
                setting.save()
            
            return Response({
                'user_id': str(user_id),
                'user_name': user.name,
                'lead_types': lead_types,
                'created': created
            }, status=status.HTTP_201_CREATED if created else status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserLeadTypesView(APIView):
    """Get lead types assigned to a specific user"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request, user_id):
        """Get lead types assigned to a specific user"""
        tenant = request.tenant
        
        try:
            setting = UserSettings.objects.get(
                tenant=tenant,
                user_id=user_id,
                key='LEAD_TYPE_ASSIGNMENT'
            )
            lead_types = setting.value if isinstance(setting.value, list) else []
        except UserSettings.DoesNotExist:
            lead_types = []
        
        return Response({
            'user_id': str(user_id),
            'lead_types': lead_types
        })