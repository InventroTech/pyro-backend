from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from django.shortcuts import get_object_or_404
from django.db import transaction
import uuid

from authz.permissions import IsTenantAuthenticated, HasTenantRole

from authz.models import TenantMembership
from .models import UserSettings, RoutingRule

from .serializers import (
    UserSettingsSerializer,
    UserSettingsCreateSerializer,
    LeadTypeAssignmentSerializer,
    RoutingRuleSerializer,
)
from accounts.models import LegacyUser
from crm_records.models import Record
from django.db.models import Q


def get_tenant_membership_by_user_id(tenant, user_id, user=None):
    """
    Helper function to get TenantMembership by tenant and user_id (UUID).
    Falls back to email lookup if user_id lookup fails and user is provided.
    Returns None if not found.
    """
    # First try to find by user_id (UUID)
    try:
        user_uuid = uuid.UUID(str(user_id))
        tenant_membership = TenantMembership.objects.filter(
            tenant=tenant,
            user_id=user_uuid
        ).first()
        if tenant_membership:
            return tenant_membership
    except (ValueError, AttributeError, TypeError):
        pass
    
    # If not found by user_id and user object is provided, try by email
    if user and hasattr(user, 'email') and user.email:
        tenant_membership = TenantMembership.objects.filter(
            tenant=tenant,
            email__iexact=user.email.lower().strip()
        ).first()
        if tenant_membership:
            # If TenantMembership has null user_id and user has a uid, update it
            # This helps fix data inconsistencies where TenantMembership exists but user_id is null
            if not tenant_membership.user_id and hasattr(user, 'uid') and user.uid:
                try:
                    tenant_membership.user_id = user.uid
                    tenant_membership.save(update_fields=['user_id'])
                except Exception:
                    # If update fails (e.g., constraint violation), continue with existing membership
                    pass
            return tenant_membership
    
    return None


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
                tenant_membership=serializer.validated_data['tenant_membership'],
                key=serializer.validated_data['key']
            ).first()
            
            if existing_setting:
                # Update existing setting
                existing_setting.value = serializer.validated_data['value']
                if 'daily_target' in serializer.validated_data:
                    existing_setting.daily_target = serializer.validated_data['daily_target']
                if 'daily_limit' in serializer.validated_data:
                    existing_setting.daily_limit = serializer.validated_data['daily_limit']
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
        tenant_membership = get_tenant_membership_by_user_id(tenant, user_id)
        if not tenant_membership:
            from django.http import Http404
            raise Http404("TenantMembership not found for this user")
        return get_object_or_404(
            UserSettings,
            tenant=tenant,
            tenant_membership=tenant_membership,
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
        ).select_related()  # Optimize user queries
        
        # Collect all user UIDs upfront
        user_uid_map = {}  # Maps user_id (str) to user object
        user_uids = []
        for user in rm_users:
            user_id_value = str(user.uid) if user.uid else None
            if user_id_value:
                user_uid_map[user_id_value] = user
                user_uids.append(user.uid)
        
        if not user_uids:
            return Response([])
        
        # Fetch all TenantMemberships in one query
        tenant_memberships = TenantMembership.objects.filter(
            tenant=tenant,
            user_id__in=user_uids
        )
        # Build lookup map: user_id (str) -> tenant_membership
        membership_map = {str(tm.user_id): tm for tm in tenant_memberships}
        
        # Fetch all UserSettings in one query for all tenant_memberships
        tenant_membership_ids = [tm.id for tm in tenant_memberships]
        user_settings = UserSettings.objects.filter(
            tenant=tenant,
            tenant_membership_id__in=tenant_membership_ids
        )
        
        # Build lookup maps: tenant_membership_id -> settings
        settings_by_membership = {}  # Maps membership_id -> list of settings
        lead_type_settings = {}  # Maps membership_id -> LEAD_TYPE_ASSIGNMENT setting
        for setting in user_settings:
            membership_id = setting.tenant_membership_id
            if membership_id not in settings_by_membership:
                settings_by_membership[membership_id] = []
            settings_by_membership[membership_id].append(setting)
            
            # Track LEAD_TYPE_ASSIGNMENT separately
            if setting.key == 'LEAD_TYPE_ASSIGNMENT':
                lead_type_settings[membership_id] = setting
        
        # Build assignments using the pre-fetched data
        assignments = []
        for user_id_value, user in user_uid_map.items():
            tenant_membership = membership_map.get(user_id_value)
            if not tenant_membership:
                # Skip users without TenantMembership
                continue
            
            # Get lead type assignment
            lead_type_setting = lead_type_settings.get(tenant_membership.id)
            lead_types = lead_type_setting.value if lead_type_setting and isinstance(lead_type_setting.value, list) else []
            
            # Get daily_target and daily_limit from any user setting
            membership_settings = settings_by_membership.get(tenant_membership.id, [])
            user_setting = membership_settings[0] if membership_settings else None
            daily_target = user_setting.daily_target if user_setting else None
            daily_limit = user_setting.daily_limit if user_setting else None
            
            assignments.append({
                'user_id': user_id_value,
                'user_name': user.name,
                'user_email': user.email,
                'lead_types': lead_types,
                'daily_target': daily_target,
                'daily_limit': daily_limit,
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
            daily_limit = serializer.validated_data.get('daily_limit', None)
            
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
                
                # Find TenantMembership for this user (with email fallback)
                tenant_membership = get_tenant_membership_by_user_id(tenant, actual_user_id_for_setting, user=user)
                if not tenant_membership:
                    return Response(
                        {'error': 'TenantMembership not found for this user. Please ensure the user has a TenantMembership record with matching email or user_id.'},
                        status=status.HTTP_404_NOT_FOUND
                    )
            
            # Create or update the setting
            # Use tenant_membership instead of user_id
            setting, created = UserSettings.objects.get_or_create(
                tenant=tenant,
                tenant_membership=tenant_membership,
                key='LEAD_TYPE_ASSIGNMENT',
                defaults={
                    'value': lead_types,
                    'daily_target': daily_target,
                    'daily_limit': daily_limit,
                }
            )
            
            if not created:
                setting.value = lead_types
                if daily_target is not None:
                    setting.daily_target = daily_target
                if daily_limit is not None:
                    setting.daily_limit = daily_limit
                setting.save()
            
            # Update daily_target across all user settings (since it's user-level, not key-specific)
            if daily_target is not None:
                UserSettings.objects.filter(
                    tenant=tenant,
                    tenant_membership=tenant_membership
                ).exclude(id=setting.id).update(daily_target=daily_target)

            # Update daily_limit across all user settings (since it's user-level, not key-specific)
            if daily_limit is not None:
                UserSettings.objects.filter(
                    tenant=tenant,
                    tenant_membership=tenant_membership
                ).exclude(id=setting.id).update(daily_limit=daily_limit)
            
            return Response({
                'user_id': str(actual_user_id_for_setting),
                'user_name': user.name,
                'lead_types': lead_types,
                'daily_target': setting.daily_target,
                'daily_limit': setting.daily_limit,
                'created': created
            }, status=status.HTTP_201_CREATED if created else status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UserLeadTypesView(APIView):
    """Get lead types assigned to a specific user"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request, user_id):
        """Get lead types assigned to a specific user"""
        tenant = request.tenant
        
        # Find TenantMembership for this user
        tenant_membership = get_tenant_membership_by_user_id(tenant, user_id)
        if not tenant_membership:
            return Response({
                'user_id': str(user_id),
                'lead_types': []
            })
        
        try:
            setting = UserSettings.objects.get(
                tenant=tenant,
                tenant_membership=tenant_membership,
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


class RoutingRuleListCreateView(APIView):
    """
    List and upsert simple per-user routing rules for tickets and leads.

    v1: one active rule per (tenant, user_id, queue_type).
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant = request.tenant
        rules = RoutingRule.objects.filter(tenant=tenant).order_by("queue_type", "user_id", "id")
        serializer = RoutingRuleSerializer(rules, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def post(self, request):
        """
        Upsert a routing rule for a user + queue_type within the current tenant.

        If a rule already exists for (tenant, user_id, queue_type), it is updated.
        Otherwise a new rule is created.
        """
        tenant = request.tenant

        serializer = RoutingRuleSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        validated = serializer.validated_data
        user_id = validated.get("user_id")
        queue_type = validated.get("queue_type")

        with transaction.atomic():
            rule, created = RoutingRule.objects.select_for_update().get_or_create(
                tenant=tenant,
                user_id=user_id,
                queue_type=queue_type,
                defaults={
                    "is_active": validated.get("is_active", True),
                    "conditions": validated.get("conditions", {}),
                    "name": validated.get("name"),
                    "description": validated.get("description"),
                },
            )

            if not created:
                # Update existing rule in-place
                for field in ["is_active", "conditions", "name", "description"]:
                    if field in validated:
                        setattr(rule, field, validated[field])
                rule.save()

        response_serializer = RoutingRuleSerializer(rule)
        return Response(
            response_serializer.data,
            status=status.HTTP_201_CREATED if created else status.HTTP_200_OK,
        )


class RoutingRuleDetailView(APIView):
    """
    Retrieve, update, or delete a specific routing rule by ID.
    """

    permission_classes = [IsTenantAuthenticated, HasTenantRole("GM")]

    def get_object(self, tenant, pk: int) -> RoutingRule:
        return get_object_or_404(RoutingRule, tenant=tenant, pk=pk)

    def get(self, request, pk: int):
        tenant = request.tenant
        rule = self.get_object(tenant, pk)
        serializer = RoutingRuleSerializer(rule)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def put(self, request, pk: int):
        tenant = request.tenant
        rule = self.get_object(tenant, pk)
        serializer = RoutingRuleSerializer(rule, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, pk: int):
        tenant = request.tenant
        rule = self.get_object(tenant, pk)
        rule.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
