from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from django.shortcuts import get_object_or_404
from django.db import transaction
import uuid

from authz.permissions import IsTenantAuthenticated

from authz.models import TenantMembership
from .models import UserSettings, RoutingRule

from .serializers import (
    UserSettingsSerializer,
    UserSettingsCreateSerializer,
    LeadTypeAssignmentSerializer,
    RoutingRuleSerializer,
)
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

    """Manage lead type assignments for users (tenant-authenticated)"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        """Get all lead type assignments for the tenant"""
        tenant = request.tenant
        
        # Get RM role from authz
        from authz.models import Role
        rm_role = Role.objects.filter(
            tenant=tenant,
            name__iexact='RM'
        ).first()
        
        if not rm_role:
            return Response([])
        
        # Get ALL TenantMemberships with RM role (not just those with UserSettings)
        tenant_memberships = TenantMembership.objects.filter(
            tenant=tenant,
            role=rm_role
        ).select_related('role')
        
        # Fetch UserSettings for LEAD_TYPE_ASSIGNMENT (lead_sources stored in same row's lead_sources column)
        tenant_membership_ids = [tm.id for tm in tenant_memberships]
        user_settings_map = {}
        if tenant_membership_ids:
            # Check if lead_statuses column exists in database schema
            from django.db import connection
            column_exists = False
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name='user_settings' AND column_name='lead_statuses'
                    """)
                    column_exists = cursor.fetchone() is not None
            except Exception:
                column_exists = False
            
            # Query based on whether column exists
            if column_exists:
                # Column exists, query normally
                user_settings = UserSettings.objects.filter(
                    tenant=tenant,
                    tenant_membership_id__in=tenant_membership_ids,
                    key='LEAD_TYPE_ASSIGNMENT'
                )
            else:
                # Column doesn't exist, use only() to select only existing fields
                import logging
                logger = logging.getLogger(__name__)
                logger.warning("lead_statuses column doesn't exist in database yet, querying without it")
                user_settings = UserSettings.objects.filter(
                    tenant=tenant,
                    tenant_membership_id__in=tenant_membership_ids,
                    key='LEAD_TYPE_ASSIGNMENT'
                ).only('id', 'tenant_id', 'tenant_membership_id', 'key', 'value', 'daily_target', 'daily_limit', 'lead_sources', 'created_at', 'updated_at')
            
            for setting in user_settings:
                user_settings_map[setting.tenant_membership_id] = setting
        
        assignments = []
        user_identifiers = []  # For counting leads
        tm_identifier_map = {}  # Map TenantMembership ID to identifiers for counting
        
        # Process all RM TenantMemberships (including those without settings)
        for tm in tenant_memberships:
            # Collect identifiers for lead counting
            tm_identifiers = []
            if tm.user_id:
                uuid_str = str(tm.user_id)
                user_identifiers.append(uuid_str)
                tm_identifiers.append(uuid_str)
            if tm.email:
                email_lower = tm.email.lower().strip()
                user_identifiers.append(email_lower)
                tm_identifiers.append(email_lower)
            
            # Store mapping: TenantMembership ID -> list of identifiers (UUID and/or email)
            tm_identifier_map[tm.id] = tm_identifiers
            
            # Get UserSettings if exists, otherwise use defaults (lead_sources and lead_statuses from same row's column)
            setting = user_settings_map.get(tm.id)
            lead_types = setting.value if setting and isinstance(setting.value, list) else []
            daily_target = setting.daily_target if setting else None
            daily_limit = setting.daily_limit if setting else None
            lead_sources = setting.lead_sources if setting and isinstance(getattr(setting, 'lead_sources', None), list) else []
            # Safely access lead_statuses - wrap in try-except in case migration hasn't been run yet
            lead_statuses = []
            if setting:
                try:
                    lead_statuses_value = setting.lead_statuses
                    if isinstance(lead_statuses_value, list):
                        lead_statuses = lead_statuses_value
                except (AttributeError, Exception):
                    # Field doesn't exist in database yet (migration not run)
                    lead_statuses = []

            # Use TenantMembership id as the primary identifier
            user_id_value = str(tm.id)

            assignments.append({
                'user_id': user_id_value,  # Always use TenantMembership ID as primary identifier
                'user_name': tm.email.split('@')[0] if tm.email else '',  # Use email prefix as name
                'user_email': tm.email,
                'tenant_membership_id': tm.id,  # Explicitly include TenantMembership ID
                'lead_types': lead_types,
                'lead_sources': lead_sources,
                'lead_statuses': lead_statuses,
                'daily_target': daily_target,  # Can be None, 0, or any integer
                'daily_limit': daily_limit,  # Can be None, 0, or any integer
                'assigned_leads_count': 0,  # Will update after counting
            })
        
        # Count assigned leads in one batch query
        assigned_counts_map = {}
        if user_identifiers:
            from django.db import connection
            with connection.cursor() as cursor:
                placeholders = ','.join(['%s'] * len(user_identifiers))
                cursor.execute(f"""
                    SELECT data->>'assigned_to' as assigned_to, COUNT(*) as count
                    FROM records
                    WHERE tenant_id = %s
                      AND entity_type = 'lead'
                      AND data->>'assigned_to' IN ({placeholders})
                    GROUP BY data->>'assigned_to'
                """, [tenant.id] + user_identifiers)
                
                for row in cursor.fetchall():
                    assigned_to_value = row[0]
                    count = row[1]
                    if assigned_to_value:
                        assigned_counts_map[assigned_to_value] = count
        
        # Update assigned_leads_count in assignments
        for assignment in assignments:
            tm_id = assignment['tenant_membership_id']
            # Get identifiers for this TenantMembership (UUID and/or email)
            tm_identifiers = tm_identifier_map.get(tm_id, [])
            # Sum up counts for all identifiers (UUID and email) for this TenantMembership
            total_count = sum(assigned_counts_map.get(identifier, 0) for identifier in tm_identifiers)
            assignment['assigned_leads_count'] = total_count
        
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
            lead_sources = serializer.validated_data.get('lead_sources') or []
            lead_statuses = serializer.validated_data.get('lead_statuses') or []
            daily_target = serializer.validated_data.get('daily_target', None)
            daily_limit = serializer.validated_data.get('daily_limit', None)

            logger.info(f"LeadTypeAssignmentView.post - daily_target={daily_target}, daily_limit={daily_limit}, lead_sources={lead_sources}, lead_statuses={lead_statuses}")
            
            # user_id must be TenantMembership ID (integer, e.g., 147)
            logger.info(f"Looking up TenantMembership with tenant={tenant.id}, user_id={user_id} (type: {type(user_id)})")
            
            # Find TenantMembership directly by ID
            tenant_membership = None
            try:
                tm_id = int(user_id)
                tenant_membership = TenantMembership.objects.filter(
                    tenant=tenant,
                    id=tm_id
                ).first()
                if tenant_membership:
                    logger.info(f"Found TenantMembership by id: {tm_id}, email={tenant_membership.email}")
            except (ValueError, TypeError):
                pass
            
            if not tenant_membership:
                logger.warning(f"TenantMembership not found for user_id={user_id}")
                return Response(
                    {'error': f'TenantMembership not found with id: {user_id}. Please use a valid TenantMembership ID.'},
                    status=status.HTTP_404_NOT_FOUND
                )
            
            # Verify TenantMembership has RM role
            if tenant_membership.role.name.upper() != 'RM':
                return Response(
                    {'error': 'TenantMembership must have RM role to assign lead types'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Create or update the setting (lead_types in value, lead_sources and lead_statuses in their columns)
            setting, created = UserSettings.objects.get_or_create(
                tenant=tenant,
                tenant_membership=tenant_membership,
                key='LEAD_TYPE_ASSIGNMENT',
                defaults={
                    'value': lead_types,
                    'daily_target': daily_target,
                    'daily_limit': daily_limit,
                    'lead_sources': lead_sources,
                    **({'lead_statuses': lead_statuses} if hasattr(UserSettings, 'lead_statuses') else {}),
                }
            )
            if not created:
                setting.value = lead_types
                setting.lead_sources = lead_sources
                # Only set lead_statuses if the field exists (migration has been run)
                if hasattr(setting, 'lead_statuses'):
                    setting.lead_statuses = lead_statuses
                if 'daily_target' in serializer.validated_data:
                    setting.daily_target = daily_target
                    logger.info(f"Updating daily_target to {daily_target} for setting id={setting.id}")
                if 'daily_limit' in serializer.validated_data:
                    setting.daily_limit = daily_limit
                    logger.info(f"Updating daily_limit to {daily_limit} for setting id={setting.id}")
                setting.save()
                lead_statuses_log = getattr(setting, 'lead_statuses', 'N/A (migration not run)')
                logger.info(f"Saved setting id={setting.id}, daily_target={setting.daily_target}, daily_limit={setting.daily_limit}, lead_sources={setting.lead_sources}, lead_statuses={lead_statuses_log}")
            
            # Update daily_target across all user settings (since it's user-level, not key-specific)
            # Use 'in' check to handle both None and explicit values (including 0)
            if 'daily_target' in serializer.validated_data:
                UserSettings.objects.filter(
                    tenant=tenant,
                    tenant_membership=tenant_membership
                ).exclude(id=setting.id).update(daily_target=daily_target)

            # Update daily_limit across all user settings (since it's user-level, not key-specific)
            if 'daily_limit' in serializer.validated_data:
                UserSettings.objects.filter(
                    tenant=tenant,
                    tenant_membership=tenant_membership
                ).exclude(id=setting.id).update(daily_limit=daily_limit)
            
            # Refresh setting from DB to ensure we have the latest values
            setting.refresh_from_db()
            
            logger.info(f"LeadTypeAssignmentView.post - Returning response: daily_target={setting.daily_target}, daily_limit={setting.daily_limit}")
            
            # Return TenantMembership id as user_id (consistent with GET response)
            return Response({
                'user_id': str(tenant_membership.id),  # TenantMembership ID
                'user_name': tenant_membership.email.split('@')[0] if tenant_membership.email else '',
                'user_email': tenant_membership.email,
                'tenant_membership_id': tenant_membership.id,
                'lead_types': lead_types,
                'lead_sources': lead_sources,
                'lead_statuses': lead_statuses,
                'daily_target': setting.daily_target,  # Return the saved value
                'daily_limit': setting.daily_limit,  # Return the saved value
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


class UserLeadsCountView(APIView):
    """Get count of leads assigned to a specific user"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request, user_id):
        """Get count of leads assigned to a specific user"""
        tenant = request.tenant
        
        if not user_id:
            return Response({
                'error': 'user_id is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Count leads assigned to this user
        count = Record.objects.filter(
            tenant=tenant,
            entity_type='lead',
            data__assigned_to=str(user_id)
        ).count()
        
        return Response({
            'user_id': str(user_id),
            'assigned_leads_count': count
        }, status=status.HTTP_200_OK)


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


class LeadSourcesListView(APIView):
    """Get all unique lead sources (data.lead_source values) from records for the current tenant"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        """Get all unique lead sources from records' lead_source field"""
        tenant = request.tenant

        if not tenant:
            return Response({
                'lead_sources': []
            }, status=status.HTTP_200_OK)

        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT data->>'lead_source' as lead_source
                FROM records
                WHERE tenant_id = %s
                  AND entity_type = 'lead'
                  AND data->>'lead_source' IS NOT NULL
                  AND data->>'lead_source' != ''
                  AND data->>'lead_source' != 'null'
                ORDER BY lead_source
            """, [tenant.id])

            lead_sources_list = [row[0].strip() for row in cursor.fetchall() if row[0] and row[0].strip()]

        return Response({
            'lead_sources': lead_sources_list
        }, status=status.HTTP_200_OK)


class LeadStatusesListView(APIView):
    """Get all unique lead statuses (data.lead_status values) from records for the current tenant"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        """Get all unique lead statuses from records' lead_status field"""
        tenant = request.tenant

        if not tenant:
            return Response({
                'lead_statuses': []
            }, status=status.HTTP_200_OK)

        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT data->>'lead_status' as lead_status
                FROM records
                WHERE tenant_id = %s
                  AND entity_type = 'lead'
                  AND data->>'lead_status' IS NOT NULL
                  AND data->>'lead_status' != ''
                  AND data->>'lead_status' != 'null'
                ORDER BY lead_status
            """, [tenant.id])

            lead_statuses_list = [row[0].strip() for row in cursor.fetchall() if row[0] and row[0].strip()]

        return Response({
            'lead_statuses': lead_statuses_list
        }, status=status.HTTP_200_OK)


class RoutingRuleListCreateView(APIView):
    """
    List and upsert routing rules for tickets and leads.
    Rules are keyed by authz.TenantMembership id (not user UUID).
    Accepts user_id as TenantMembership id (integer) or as user UUID for backward compat.
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant = request.tenant
        rules = (
            RoutingRule.objects.filter(tenant=tenant)
            .select_related("tenant_membership")
            .order_by("queue_type", "tenant_membership_id", "id")
        )
        serializer = RoutingRuleSerializer(rules, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def post(self, request):
        """
        Upsert a routing rule by tenant_membership + queue_type.
        Request can send user_id as TenantMembership id (integer) or as user UUID;
        we resolve to TenantMembership and store that. Works even when membership has no linked auth user.
        """
        tenant = request.tenant
        raw_user_id = request.data.get("user_id")
        if not raw_user_id:
            return Response(
                {"user_id": ["This field is required (TenantMembership id or user UUID)."]},
                status=status.HTTP_400_BAD_REQUEST,
            )

        raw_str = str(raw_user_id).strip()
        tenant_membership = None

        # Prefer TenantMembership id (integer)
        try:
            tm_id = int(raw_str)
            tenant_membership = (
                TenantMembership.objects.filter(tenant=tenant, id=tm_id)
                .select_related("role")
                .first()
            )
        except (ValueError, TypeError):
            pass

        # Fallback: treat as user UUID and resolve to membership
        if not tenant_membership:
            try:
                user_uuid = uuid.UUID(raw_str)
                tenant_membership = (
                    TenantMembership.objects.filter(tenant=tenant, user_id=user_uuid)
                    .select_related("role")
                    .first()
                )
            except (ValueError, AttributeError, TypeError):
                pass

        if not tenant_membership:
            return Response(
                {
                    "user_id": [
                        "TenantMembership not found for this id or user UUID in this tenant."
                    ]
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        data = request.data.copy()
        data["tenant_membership"] = tenant_membership.id
        data["user_id"] = (
            str(tenant_membership.user_id) if tenant_membership.user_id else None
        )

        serializer = RoutingRuleSerializer(data=data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        validated = serializer.validated_data
        queue_type = validated["queue_type"]

        with transaction.atomic():
            rule, created = RoutingRule.objects.select_for_update().get_or_create(
                tenant=tenant,
                tenant_membership=tenant_membership,
                queue_type=queue_type,
                defaults={
                    "user_id": tenant_membership.user_id,
                    "is_active": validated.get("is_active", True),
                    "conditions": validated.get("conditions", {}),
                    "name": validated.get("name"),
                    "description": validated.get("description"),
                },
            )

            if not created:
                rule.user_id = tenant_membership.user_id
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

    permission_classes = [IsTenantAuthenticated]

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
