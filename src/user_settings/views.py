from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from django.shortcuts import get_object_or_404
from django.db import transaction
import uuid

from authz.permissions import IsTenantAuthenticated

from authz.models import TenantMembership
from .models import UserSettings, RoutingRule, Group, TenantMemberSetting

from .serializers import (
    UserSettingsSerializer,
    UserSettingsCreateSerializer,
    TenantMemberSettingSerializer,
    LeadTypeAssignmentSerializer,
    RoutingRuleSerializer,
    GroupSerializer,
)
from .services import (
    upsert_user_kv_settings,
    USER_KV_GROUP_ID_KEY,
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_DAILY_TARGET_KEY,
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
        
        # Fetch per-user KV rows for core settings and resolve group filters from Group table
        tenant_membership_ids = [tm.id for tm in tenant_memberships]
        core_kv_map = {}
        groups_by_id = {}
        if tenant_membership_ids:
            kv_rows = TenantMemberSetting.objects.filter(
                tenant=tenant,
                tenant_membership_id__in=tenant_membership_ids,
                key__in=[USER_KV_GROUP_ID_KEY, USER_KV_DAILY_TARGET_KEY, USER_KV_DAILY_LIMIT_KEY],
            )
            for row in kv_rows:
                core_kv_map.setdefault(row.tenant_membership_id, {})[row.key] = row.value
            group_ids = {
                kv.get(USER_KV_GROUP_ID_KEY)
                for kv in core_kv_map.values()
                if isinstance(kv.get(USER_KV_GROUP_ID_KEY), int)
            }
            if group_ids:
                groups_by_id = {
                    g.id: g
                    for g in Group.objects.filter(tenant=tenant, id__in=group_ids)
                }
        
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
            
            tm_core = core_kv_map.get(tm.id, {})
            group = groups_by_id.get(tm_core.get(USER_KV_GROUP_ID_KEY))
            group_data = group.group_data if group and isinstance(group.group_data, dict) else {}
            lead_types = group_data.get("party") if isinstance(group_data.get("party"), list) else []
            lead_sources = group_data.get("lead_sources") if isinstance(group_data.get("lead_sources"), list) else []
            lead_statuses = group_data.get("lead_statuses") if isinstance(group_data.get("lead_statuses"), list) else []
            daily_target = tm_core.get(USER_KV_DAILY_TARGET_KEY) if isinstance(tm_core.get(USER_KV_DAILY_TARGET_KEY), int) else None
            daily_limit = tm_core.get(USER_KV_DAILY_LIMIT_KEY) if isinstance(tm_core.get(USER_KV_DAILY_LIMIT_KEY), int) else None

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
            assignment_value = {
                "lead_types": lead_types,
                "lead_sources": lead_sources,
                "lead_statuses": lead_statuses,
                "daily_target": daily_target,
                "daily_limit": daily_limit,
            }

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
            
            # Create or update the setting in UserSettings
            setting, created = UserSettings.objects.get_or_create(
                tenant=tenant,
                tenant_membership=tenant_membership,
                key='LEAD_TYPE_ASSIGNMENT',
                defaults={
                    'value': assignment_value,
                    'daily_target': daily_target,
                    'daily_limit': daily_limit,
                }
            )
            if not created:
                setting.value = assignment_value
                setting.lead_sources = None
                if hasattr(setting, 'lead_statuses'):
                    setting.lead_statuses = None
                if 'daily_target' in serializer.validated_data:
                    setting.daily_target = daily_target
                if 'daily_limit' in serializer.validated_data:
                    setting.daily_limit = daily_limit
                setting.save()

            if 'daily_target' in serializer.validated_data:
                UserSettings.objects.filter(
                    tenant=tenant,
                    tenant_membership=tenant_membership
                ).exclude(id=setting.id).update(daily_target=daily_target)
            if 'daily_limit' in serializer.validated_data:
                UserSettings.objects.filter(
                    tenant=tenant,
                    tenant_membership=tenant_membership
                ).exclude(id=setting.id).update(daily_limit=daily_limit)
            upsert_user_kv_settings(
                tenant=tenant,
                tenant_membership=tenant_membership,
                group_id=getattr(setting, "group_id", None),
                daily_target=setting.daily_target,
                daily_limit=setting.daily_limit,
            )
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
        
        kv_rows = TenantMemberSetting.objects.filter(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key__in=[USER_KV_GROUP_ID_KEY],
        )
        kv_map = {row.key: row.value for row in kv_rows}
        group = None
        group_id = kv_map.get(USER_KV_GROUP_ID_KEY)
        if isinstance(group_id, int):
            group = Group.objects.filter(tenant=tenant, id=group_id).first()
        group_data = group.group_data if group and isinstance(group.group_data, dict) else {}
        lead_types = group_data.get("party") if isinstance(group_data.get("party"), list) else []
        
        return Response({
            'user_id': str(user_id),
            'lead_types': lead_types
        })


class UserCoreKVSettingsView(APIView):
    """
    Returns per-user key/value settings for core fields:
    - GROUP
    - DAILY_TARGET
    - DAILY_LIMIT
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request, user_id):
        tenant = request.tenant

        tenant_membership = None
        try:
            tenant_membership = TenantMembership.objects.filter(tenant=tenant, id=int(user_id)).first()
        except (ValueError, TypeError):
            tenant_membership = get_tenant_membership_by_user_id(tenant, user_id)

        if not tenant_membership:
            return Response(
                {"error": f"TenantMembership not found for user_id={user_id}"},
                status=status.HTTP_404_NOT_FOUND,
            )

        qs = TenantMemberSetting.objects.filter(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key__in=[USER_KV_GROUP_ID_KEY, USER_KV_DAILY_TARGET_KEY, USER_KV_DAILY_LIMIT_KEY],
        ).order_by("key")
        return Response(TenantMemberSettingSerializer(qs, many=True).data, status=status.HTTP_200_OK)


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


class LeadStatesListView(APIView):
    """Get all unique lead states (data.state values) from records for the current tenant"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant = request.tenant

        if not tenant:
            return Response({
                'lead_states': []
            }, status=status.HTTP_200_OK)

        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT data->>'state' as state
                FROM records
                WHERE tenant_id = %s
                  AND entity_type = 'lead'
                  AND data->>'state' IS NOT NULL
                  AND data->>'state' != ''
                  AND data->>'state' != 'null'
                ORDER BY state
            """, [tenant.id])

            lead_states_list = [row[0].strip() for row in cursor.fetchall() if row[0] and row[0].strip()]

        return Response({
            'lead_states': lead_states_list
        }, status=status.HTTP_200_OK)


class QueueTypesListView(APIView):
    """Get supported queue types for tenant routing/grouping."""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        return Response(
            {
                "queue_types": ["lead", "ticket"]
            },
            status=status.HTTP_200_OK,
        )


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


class GroupListCreateView(APIView):
    """List and create tenant groups."""

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant = request.tenant
        groups = Group.objects.filter(tenant=tenant).order_by("-created_at")
        serializer = GroupSerializer(groups, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def post(self, request):
        tenant = request.tenant
        serializer = GroupSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        group = serializer.save(tenant=tenant)
        return Response(GroupSerializer(group).data, status=status.HTTP_201_CREATED)


class GroupDetailView(APIView):
    """Retrieve/update/delete a tenant group."""

    permission_classes = [IsTenantAuthenticated]

    def get_object(self, tenant, pk: int) -> Group:
        return get_object_or_404(Group, tenant=tenant, pk=pk)

    def get(self, request, pk: int):
        tenant = request.tenant
        group = self.get_object(tenant, pk)
        return Response(GroupSerializer(group).data, status=status.HTTP_200_OK)

    def put(self, request, pk: int):
        tenant = request.tenant
        group = self.get_object(tenant, pk)
        serializer = GroupSerializer(group, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, pk: int):
        tenant = request.tenant
        group = self.get_object(tenant, pk)
        group.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
