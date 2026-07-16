from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.shortcuts import get_object_or_404
import uuid

from authz.permissions import IsTenantAuthenticated

from authz.models import TenantMembership
from .models import Group, TenantMemberSetting

from .serializers import (
    TenantMemberSettingSerializer,
    LeadTypeAssignmentSerializer,
    UserCoreKVSettingsPatchSerializer,
    GroupSerializer,
)
from .services import (
    coerce_kv_int,
    fresh_leads_counts_for_groups,
    get_lead_filter_options,
    upsert_user_kv_settings,
    upsert_user_lead_assignment_kv,
    upsert_support_daily_limit_kv,
    USER_KV_GROUP_ID_KEY,
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_DAILY_TARGET_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY,
    USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY,
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


class LeadTypeAssignmentView(APIView):

    """Manage lead type assignments for users (tenant-authenticated)"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        """Get all lead type assignments for the tenant"""
        tenant = request.tenant
        
        # Get RM/CSE roles from authz
        from authz.models import Role
        target_roles = Role.objects.filter(
            tenant=tenant,
        ).filter(Q(name__iexact='RM') | Q(name__iexact='CSE'))

        if not target_roles.exists():
            return Response([])

        # Get ALL TenantMemberships with RM/CSE roles
        tenant_memberships = TenantMembership.objects.filter(
            tenant=tenant,
            role__in=target_roles
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
            
            # Persist assignment via TenantMemberSetting KV only (Group holds filter config).
            kv_row = TenantMemberSetting.objects.filter(
                tenant=tenant,
                tenant_membership=tenant_membership,
                key=USER_KV_GROUP_ID_KEY,
            ).first()
            group_id = kv_row.value if kv_row and isinstance(kv_row.value, int) else None

            upsert_user_kv_settings(
                tenant=tenant,
                tenant_membership=tenant_membership,
                group_id=group_id,
                daily_target=daily_target,
                daily_limit=daily_limit,
            )
            upsert_user_lead_assignment_kv(
                tenant=tenant,
                tenant_membership=tenant_membership,
                assignment_value=assignment_value,
            )
            logger.info(
                "LeadTypeAssignmentView.post - Returning response: daily_target=%s, daily_limit=%s",
                daily_target,
                daily_limit,
            )

            return Response({
                'user_id': str(tenant_membership.id),
                'user_name': tenant_membership.email.split('@')[0] if tenant_membership.email else '',
                'user_email': tenant_membership.email,
                'tenant_membership_id': tenant_membership.id,
                'lead_types': lead_types,
                'lead_sources': lead_sources,
                'lead_statuses': lead_statuses,
                'daily_target': daily_target,
                'daily_limit': daily_limit,
                'created': True,
            }, status=status.HTTP_200_OK)
        
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


_CORE_KV_SETTING_KEYS = (
    USER_KV_GROUP_ID_KEY,
    USER_KV_DAILY_TARGET_KEY,
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY,
    USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY,
)


def _resolve_tenant_membership_for_settings(tenant, user_id):
    try:
        return TenantMembership.objects.filter(tenant=tenant, id=int(user_id)).first()
    except (ValueError, TypeError):
        return get_tenant_membership_by_user_id(tenant, user_id)


class UserCoreKVSettingsView(APIView):
    """
    Per-user core KV settings (GET + PATCH support daily limits / resolve-rate goal for CSE).

    GET returns GROUP, DAILY_TARGET, DAILY_LIMIT, and CSE support ST/other
    limit + resolve-rate goal keys when set.

    PATCH accepts any of:
    ``support_daily_limit_self_trial``, ``support_daily_limit_other``,
    ``support_resolve_rate_goal`` (null clears). Membership must have CSE role.
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request, user_id):
        tenant = request.tenant

        tenant_membership = _resolve_tenant_membership_for_settings(tenant, user_id)

        if not tenant_membership:
            return Response(
                {"error": f"TenantMembership not found for user_id={user_id}"},
                status=status.HTTP_404_NOT_FOUND,
            )

        qs = TenantMemberSetting.objects.filter(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key__in=_CORE_KV_SETTING_KEYS,
        ).order_by("key")
        return Response(TenantMemberSettingSerializer(qs, many=True).data, status=status.HTTP_200_OK)

    def patch(self, request, user_id):
        tenant = request.tenant
        tenant_membership = _resolve_tenant_membership_for_settings(tenant, user_id)
        if not tenant_membership:
            return Response(
                {"error": f"TenantMembership not found for user_id={user_id}"},
                status=status.HTTP_404_NOT_FOUND,
            )

        role_key = (tenant_membership.role.key or "").upper() if tenant_membership.role else ""
        if role_key != "CSE":
            return Response(
                {
                    "error": (
                        "TenantMembership must have CSE role to set support "
                        "daily limits / resolve-rate goals"
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = UserCoreKVSettingsPatchSerializer(data=request.data, partial=True)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        data = serializer.validated_data
        upsert_support_daily_limit_kv(
            tenant=tenant,
            tenant_membership=tenant_membership,
            self_trial_limit=data.get("support_daily_limit_self_trial"),
            other_limit=data.get("support_daily_limit_other"),
            update_self_trial="support_daily_limit_self_trial" in data,
            update_other="support_daily_limit_other" in data,
            resolve_rate_goal=data.get("support_resolve_rate_goal"),
            update_resolve_rate_goal="support_resolve_rate_goal" in data,
        )

        qs = TenantMemberSetting.objects.filter(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key__in=_CORE_KV_SETTING_KEYS,
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
            data__contains={"assigned_to": str(user_id)}
        ).count()
        
        return Response({
            'user_id': str(user_id),
            'assigned_leads_count': count
        }, status=status.HTTP_200_OK)


class LeadTypesListView(APIView):
    """Get all unique lead types (affiliated_party values) from records for the current tenant"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        options = get_lead_filter_options(request.tenant)
        return Response({"lead_types": options["lead_types"]}, status=status.HTTP_200_OK)


class LeadSourcesListView(APIView):
    """Get all unique lead sources (data.lead_source values) from records for the current tenant"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        options = get_lead_filter_options(request.tenant)
        return Response({"lead_sources": options["lead_sources"]}, status=status.HTTP_200_OK)


class LeadStatusesListView(APIView):
    """Get all unique lead statuses (data.lead_status values) from records for the current tenant"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        options = get_lead_filter_options(request.tenant)
        return Response({"lead_statuses": options["lead_statuses"]}, status=status.HTTP_200_OK)


class LeadStatesListView(APIView):
    """Get all unique lead states (data.state values) from records for the current tenant"""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        options = get_lead_filter_options(request.tenant)
        return Response({"lead_states": options["lead_states"]}, status=status.HTTP_200_OK)


class LeadFilterOptionsView(APIView):
    """All lead filter dropdown values in one response (single DB round trip)."""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        options = get_lead_filter_options(request.tenant)
        return Response(options, status=status.HTTP_200_OK)


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


class GroupListCreateView(APIView):
    """List and create tenant groups."""

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant = request.tenant
        groups = list(Group.objects.filter(tenant=tenant).order_by("-created_at"))
        fresh_counts = fresh_leads_counts_for_groups(tenant, groups)
        serializer = GroupSerializer(
            groups,
            many=True,
            context={"fresh_leads_counts": fresh_counts},
        )
        return Response(serializer.data, status=status.HTTP_200_OK)

    def post(self, request):
        tenant = request.tenant
        serializer = GroupSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        group = serializer.save(tenant=tenant)
        fresh_counts = fresh_leads_counts_for_groups(tenant, [group])
        return Response(
            GroupSerializer(group, context={"fresh_leads_counts": fresh_counts}).data,
            status=status.HTTP_201_CREATED,
        )


class MyLeadGroupSummaryView(APIView):
    """
    Return the authenticated user's assigned lead group and fresh-leads pool count.

    Used by the RM pending-leads page so it does not need to stitch membership +
    core-KV + group detail calls together.
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant = request.tenant
        user = request.user
        user_identifier = getattr(user, "supabase_uid", None) or getattr(user, "email", None)
        if not user_identifier:
            return Response(
                {"error": "Unable to resolve current user"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        membership = get_tenant_membership_by_user_id(tenant, user_identifier, user=user)
        if not membership:
            return Response(
                {
                    "tenant_membership_id": None,
                    "group_id": None,
                    "group_name": None,
                    "fresh_leads_count": None,
                    "daily_limit": None,
                    "daily_target": None,
                },
                status=status.HTTP_200_OK,
            )

        kv_rows = TenantMemberSetting.objects.filter(
            tenant=tenant,
            tenant_membership=membership,
            key__in=[USER_KV_GROUP_ID_KEY, USER_KV_DAILY_LIMIT_KEY, USER_KV_DAILY_TARGET_KEY],
        )
        kv_map = {row.key: row.value for row in kv_rows}
        group_id = coerce_kv_int(kv_map.get(USER_KV_GROUP_ID_KEY))
        daily_limit = coerce_kv_int(kv_map.get(USER_KV_DAILY_LIMIT_KEY))
        daily_target = coerce_kv_int(kv_map.get(USER_KV_DAILY_TARGET_KEY))

        group = None
        fresh_leads_count = None
        if group_id is not None:
            group = Group.objects.filter(tenant=tenant, id=group_id).first()
            if group:
                fresh_counts = fresh_leads_counts_for_groups(tenant, [group])
                fresh_leads_count = fresh_counts.get(group.id)

        return Response(
            {
                "tenant_membership_id": membership.id,
                "group_id": group.id if group else None,
                "group_name": group.name if group else None,
                "fresh_leads_count": fresh_leads_count,
                "daily_limit": daily_limit,
                "daily_target": daily_target,
            },
            status=status.HTTP_200_OK,
        )


class GroupDetailView(APIView):
    """Retrieve/update/delete a tenant group."""

    permission_classes = [IsTenantAuthenticated]

    def get_object(self, tenant, pk: int) -> Group:
        return get_object_or_404(Group, tenant=tenant, pk=pk)

    def get(self, request, pk: int):
        tenant = request.tenant
        group = self.get_object(tenant, pk)
        fresh_counts = fresh_leads_counts_for_groups(tenant, [group])
        return Response(
            GroupSerializer(group, context={"fresh_leads_counts": fresh_counts}).data,
            status=status.HTTP_200_OK,
        )

    def put(self, request, pk: int):
        tenant = request.tenant
        group = self.get_object(tenant, pk)
        serializer = GroupSerializer(group, data=request.data, partial=True)
        if serializer.is_valid():
            group = serializer.save()
            fresh_counts = fresh_leads_counts_for_groups(tenant, [group])
            return Response(
                GroupSerializer(group, context={"fresh_leads_counts": fresh_counts}).data,
                status=status.HTTP_200_OK,
            )
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, pk: int):
        tenant = request.tenant
        group = self.get_object(tenant, pk)
        group.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
