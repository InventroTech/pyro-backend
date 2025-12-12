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
from crm_records.models import Record
from django.db.models import Q


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
                if 'daily_target' in serializer.validated_data:
                    existing_setting.daily_target = serializer.validated_data['daily_target']
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
    
    def patch(self, request, user_id, key):
        """Partially update a specific user setting"""
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
        # LegacyUser has role_id (UUID), so we need to filter via LegacyRole
        from accounts.models import LegacyRole
        rm_role_ids = LegacyRole.objects.filter(
            tenant=tenant,
            name__iexact='RM'
        ).values_list('id', flat=True)
        
        rm_users = LegacyUser.objects.filter(
            tenant=tenant,
            role_id__in=rm_role_ids
        )
        
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
            
            # Get daily_target from any user setting (this is a user-level field)
            user_setting = UserSettings.objects.filter(
                tenant=tenant,
                user_id=user.uid or user.id
            ).first()
            daily_target = user_setting.daily_target if user_setting else None
            
            # Always use uid (UUID) if available, as that's what the serializer expects
            user_id_value = str(user.uid) if user.uid else None
            if not user_id_value:
                # Skip users without uid - they can't be assigned lead types via API
                continue
            
            assignments.append({
                'user_id': user_id_value,
                'user_name': user.name,
                'user_email': user.email,
                'lead_types': lead_types,
                'daily_target': daily_target
            })
        
        return Response(assignments)

    def post(self, request):
        """Assign lead types to a user"""
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"LeadTypeAssignmentView.post called with data: {request.data}")
        
        tenant = request.tenant
        serializer = LeadTypeAssignmentSerializer(data=request.data)
        
        logger.info(f"Serializer is_valid: {serializer.is_valid()}, errors: {serializer.errors if not serializer.is_valid() else 'None'}")
        
        if serializer.is_valid():
            user_id = serializer.validated_data['user_id']
            lead_types = serializer.validated_data['lead_types']
            daily_target = serializer.validated_data.get('daily_target', None)
            
            # Verify user exists and has RM role
            # user_id could be a UUID string or integer ID string
            logger.info(f"Looking up user with tenant={tenant.id}, user_id={user_id} (type: {type(user_id)})")
            
            user = None
            try:
                import uuid
                # Try to parse as UUID first
                is_uuid_format = False
                potential_int_id = None
                
                try:
                    user_uuid = uuid.UUID(str(user_id))
                    is_uuid_format = True
                    
                    # Check if this UUID looks like a converted integer (00000000-0000-0000-0000-XXXXXXXX)
                    # where XXXX is a small integer - check the last segment
                    uuid_parts = str(user_uuid).split('-')
                    if uuid_parts[0] == '00000000' and uuid_parts[1] == '0000' and uuid_parts[2] == '0000' and uuid_parts[3] == '0000':
                        # This looks like a converted integer, extract from last segment
                        try:
                            potential_int_id = int(uuid_parts[4], 16)  # Last segment as hex integer
                            if potential_int_id > 0 and potential_int_id < 1000000:  # Reasonable ID range
                                logger.info(f"UUID appears to be converted integer: {potential_int_id}")
                        except (ValueError, TypeError):
                            pass
                    
                    # First try to find by uid (UUID field)
                    user = LegacyUser.objects.filter(
                        tenant=tenant,
                        uid=user_uuid
                    ).first()
                    if user:
                        logger.info(f"Found user by uid: {user.name}, uid={user.uid}, id={user.id}")
                except (ValueError, AttributeError):
                    pass
                
                # If not found by uid and we have a potential integer ID from UUID conversion
                if not user and potential_int_id:
                    user = LegacyUser.objects.filter(
                        tenant=tenant,
                        id=potential_int_id
                    ).first()
                    if user:
                        logger.info(f"Found user by converted integer id: {user.name}, uid={user.uid}, id={user.id}")
                
                # If still not found and user_id is a plain integer string, try by integer id
                if not user and not is_uuid_format:
                    try:
                        user_int_id = int(user_id)
                        user = LegacyUser.objects.filter(
                            tenant=tenant,
                            id=user_int_id
                        ).first()
                        if user:
                            logger.info(f"Found user by integer id: {user.name}, uid={user.uid}, id={user.id}")
                    except (ValueError, TypeError):
                        pass
                
                if not user:
                    raise LegacyUser.DoesNotExist(f"User not found with id={user_id}")
                    
            except LegacyUser.DoesNotExist as e:
                logger.warning(f"User not found with tenant={tenant.id}, user_id={user_id}: {e}")
                return Response(
                    {'error': f'User not found with id: {user_id}. Please use a valid user UUID or ID.'},
                    status=status.HTTP_404_NOT_FOUND
                )
            
            # Continue with user found
            if user:
                # Check if user has RM role via role_id
                from accounts.models import LegacyRole
                if not user.role_id:
                    return Response(
                        {'error': 'User does not have a role assigned'},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                user_role = LegacyRole.objects.filter(
                    id=user.role_id,
                    tenant=tenant,
                    name__iexact='RM'
                ).first()
                if not user_role:
                    return Response(
                        {'error': 'User must have RM role to assign lead types'},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                
                # Use uid if available, otherwise fall back to id
                actual_user_id_for_setting = user.uid if user.uid else user.id
            
            # Create or update the setting
            # Use the actual user identifier (uid if available, otherwise id)
            setting, created = UserSettings.objects.get_or_create(
                tenant=tenant,
                user_id=actual_user_id_for_setting,
                key='LEAD_TYPE_ASSIGNMENT',
                defaults={
                    'value': lead_types,
                    'daily_target': daily_target
                }
            )
            
            if not created:
                setting.value = lead_types
                if daily_target is not None:
                    setting.daily_target = daily_target
                setting.save()
            
            # Update daily_target across all user settings (since it's user-level, not key-specific)
            if daily_target is not None:
                UserSettings.objects.filter(
                    tenant=tenant,
                    user_id=actual_user_id_for_setting
                ).exclude(id=setting.id).update(daily_target=daily_target)
            
            return Response({
                'user_id': str(actual_user_id_for_setting),
                'user_name': user.name,
                'lead_types': lead_types,
                'daily_target': setting.daily_target,
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


class LeadTypesListView(APIView):
    """Get all unique lead types (affiliated_party values) from records for the current tenant"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        """Get all unique lead types from records' affiliated_party field"""
        tenant = request.tenant
        
        if not tenant:
            return Response({
                'lead_types': []
            }, status=status.HTTP_200_OK)
        
        # Extract unique affiliated_party values using database-level query for better performance
        # Using raw SQL for efficient JSONB querying
        from django.db import connection
        
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT data->>'affiliated_party' as affiliated_party
                FROM records
                WHERE tenant_id = %s
                  AND entity_type = 'lead'
                  AND data->>'affiliated_party' IS NOT NULL
                  AND data->>'affiliated_party' != ''
                  AND data->>'affiliated_party' != 'null'
                ORDER BY affiliated_party
            """, [tenant.id])
            
            lead_types_list = [row[0].strip() for row in cursor.fetchall() if row[0] and row[0].strip()]
        
        return Response({
            'lead_types': lead_types_list
        }, status=status.HTTP_200_OK)