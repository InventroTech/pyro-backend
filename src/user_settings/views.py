from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import serializers, status
from django.db import DatabaseError, transaction
from django.shortcuts import get_object_or_404
from django.utils import timezone
import logging
import uuid

from accounts.models import DeletedUserLog
from accounts.serializers import DeleteUserEverywhereSerializer
from accounts.services.delete_user_everywhere import delete_user_everywhere
from authz.permissions import IsTenantAuthenticated

from authz.models import TenantMembership
from realtime.broadcast import broadcast_to_tenant
from .models import Group, TenantMemberSetting

from .serializers import (
    TenantMemberSettingSerializer,
    LeadTypeAssignmentSerializer,
    UserCoreKVSettingsPatchSerializer,
    GroupSerializer,
    InactiveEmailPolicySerializer,
)
from .services import (
    coerce_kv_int,
    count_available_fresh_leads_for_group,
    fresh_leads_counts_for_groups,
    get_lead_filter_options,
    upsert_user_kv_settings,
    upsert_user_lead_assignment_kv,
    upsert_support_daily_limit_kv,
    upsert_custom_field_kv,
    is_user_management_kv_key,
    USER_KV_GROUP_ID_KEY,
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_DAILY_TARGET_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
    USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY,
    USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY,
    USER_KV_STATE_KEY,
    USER_KV_DISTRICT_KEY,
    USER_KV_PARTY_KEY,
    USER_KV_RESERVED_KEYS,
)
from crm_records.models import Record
from django.db.models import Q
from .geo_party_catalog import catalog_options, load_geo_party_catalog

logger = logging.getLogger(__name__)


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
    USER_KV_STATE_KEY,
    USER_KV_DISTRICT_KEY,
    USER_KV_PARTY_KEY,
)

_SUPPORT_PATCH_KEYS = (
    "support_daily_limit_self_trial",
    "support_daily_limit_other",
    "support_resolve_rate_goal",
)


_CORE_KV_KEY_FILTER = Q(key__in=_CORE_KV_SETTING_KEYS) | ~Q(key__in=USER_KV_RESERVED_KEYS)


def _core_kv_queryset(tenant, tenant_membership):
    """Core keys plus bare User Management custom field keys (e.g. STATE)."""
    return Q(key__in=_CORE_KV_SETTING_KEYS) | ~Q(key__in=USER_KV_RESERVED_KEYS)


def _core_kv_queryset(tenant, tenant_membership):
    return (
        TenantMemberSetting.objects.filter(
            tenant=tenant,
            tenant_membership=tenant_membership,
        )
        .filter(_CORE_KV_KEY_FILTER)
        .order_by("key")
    )


CORE_KV_SETTINGS_BATCH_LIMIT = 60


class TenantCoreKVSettingsView(APIView):
    """
    Core KV settings for a batch of tenant members in one request.

    GET ?membership_ids=67,68,... (at most ``CORE_KV_SETTINGS_BATCH_LIMIT`` ids) returns
    ``{"results": {"67": {"DAILY_TARGET": 10, ...}, ...}}`` — the same keys
    ``UserCoreKVSettingsView`` returns per member, as a compact key -> value map. The
    Settings page loads all users in batches of this size instead of one request each.
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        raw_ids = request.query_params.get("membership_ids", "")
        try:
            membership_ids = sorted({int(part) for part in raw_ids.split(",") if part.strip()})
        except ValueError:
            return Response(
                {"error": "membership_ids must be a comma-separated list of integers"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if not membership_ids:
            return Response(
                {"error": "membership_ids is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if len(membership_ids) > CORE_KV_SETTINGS_BATCH_LIMIT:
            return Response(
                {"error": f"At most {CORE_KV_SETTINGS_BATCH_LIMIT} membership_ids per request"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        rows = (
            TenantMemberSetting.objects.filter(
                tenant=request.tenant,
                tenant_membership_id__in=membership_ids,
            )
            .filter(_CORE_KV_KEY_FILTER)
            .order_by("tenant_membership_id", "key")
            .values_list("tenant_membership_id", "key", "value")
        )
        results: dict[str, dict] = {}
        for membership_id, key, value in rows:
            results.setdefault(str(membership_id), {})[key] = value
        return Response({"results": results}, status=status.HTTP_200_OK)


def _resolve_tenant_membership_for_settings(tenant, user_id):
    try:
        return TenantMembership.objects.filter(tenant=tenant, id=int(user_id)).first()
    except (ValueError, TypeError):
        return get_tenant_membership_by_user_id(tenant, user_id)


class UserCoreKVSettingsView(APIView):
    """
    Per-user core KV settings (GET + PATCH).

    GET returns GROUP, DAILY_TARGET, DAILY_LIMIT, STATE, DISTRICT, CSE support
    keys, and any free-form custom field keys.

    PATCH accepts:
    - CSE support keys (``support_daily_limit_*``, ``support_resolve_rate_goal``)
      — requires CSE role on the target membership
    - Bare uppercase free-form custom keys for any role

    STATE / DISTRICT are core keys (like GROUP / DAILY_LIMIT) and are set via
    user create/update, not this free-form patch path.
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

        qs = _core_kv_queryset(tenant, tenant_membership)
        return Response(TenantMemberSettingSerializer(qs, many=True).data, status=status.HTTP_200_OK)

    def patch(self, request, user_id):
        tenant = request.tenant
        tenant_membership = _resolve_tenant_membership_for_settings(tenant, user_id)
        if not tenant_membership:
            return Response(
                {"error": f"TenantMembership not found for user_id={user_id}"},
                status=status.HTTP_404_NOT_FOUND,
            )

        raw = request.data
        if not isinstance(raw, dict):
            return Response(
                {"error": "Request body must be a JSON object"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        custom_fields = {
            key: value
            for key, value in raw.items()
            if is_user_management_kv_key(key)
        }
        reserved_collision = [
            key for key in raw if isinstance(key, str) and key in USER_KV_RESERVED_KEYS
        ]
        if reserved_collision:
            return Response(
                {
                    "error": (
                        f"Cannot set reserved key(s) via this patch: "
                        f"{', '.join(reserved_collision)}"
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        support_payload = {key: raw[key] for key in _SUPPORT_PATCH_KEYS if key in raw}

        if not custom_fields and not support_payload:
            return Response(
                {
                    "error": (
                        "Provide at least one of support_daily_limit_self_trial, "
                        "support_daily_limit_other, support_resolve_rate_goal, "
                        "or a custom field key (e.g. STATE, DISTRICT)"
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if support_payload:
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

            serializer = UserCoreKVSettingsPatchSerializer(data=support_payload, partial=True)
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

        if custom_fields:
            for key, value in custom_fields.items():
                if value is not None and not isinstance(value, (str, int, float, bool)):
                    return Response(
                        {"error": f"{key} must be a string, number, boolean, or null"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
            upsert_custom_field_kv(
                tenant=tenant,
                tenant_membership=tenant_membership,
                fields=custom_fields,
            )

        qs = _core_kv_queryset(tenant, tenant_membership)
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


class GeoPartyCatalogView(APIView):
    """
    Circle state / district / party ID→name catalog for User Management dropdowns.

    Labels are ``Name (id)``; values are Circle IDs. Optional ``?state_id=`` filters
    districts (and parties) to that state.
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        state_id = (request.query_params.get("state_id") or "").strip() or None
        catalog = load_geo_party_catalog()
        return Response(
            {
                "version": catalog.get("version"),
                "states": catalog_options("states"),
                "districts": catalog_options("districts", state_id=state_id),
                "parties": catalog_options("parties", state_id=state_id),
            },
            status=status.HTTP_200_OK,
        )


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
                fresh_leads_count = count_available_fresh_leads_for_group(tenant, group)

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


class InactiveEmailPolicyView(APIView):
    """
    Settings-page hour threshold for who counts as inactive. Default is 72 hours.
    This does not turn memberships off.
    Sign-in time shown on the user list is auth.users.last_sign_in_at, or
    authentication.User.last_login when AUTH_PROVIDER is django.
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        from user_settings.inactivity import inactive_email_policy

        return Response(inactive_email_policy(request.tenant))

    def patch(self, request):
        from user_settings.inactivity import inactive_email_policy, save_inactive_email_policy

        serializer = InactiveEmailPolicySerializer(data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        if not data:
            return Response(inactive_email_policy(request.tenant))
        return Response(
            save_inactive_email_policy(
                request.tenant,
                enabled=data.get("enabled"),
                inactive_after_hours=data.get("inactive_after_hours"),
            )
        )


# ---------------------------------------------------------------------------
# Deleted-users log: DELETE /accounts/delete-user-with-log/ snapshots the user,
# calls the unchanged delete_user_everywhere, then stores a DeletedUserLog row for
# each membership it soft-deleted. GET /accounts/deleted-users/ lists those rows.
# ---------------------------------------------------------------------------

# Realtime clients already relay "record_updated"; existing listeners filter on
# lead / support_ticket, so this entity type only reaches the deleted-users list.
DELETED_USER_REALTIME_ENTITY_TYPE = "tenant_membership"


def _broadcast_user_deleted(tenant_id, membership_ids: list[int]) -> None:
    for membership_id in membership_ids:
        broadcast_to_tenant(
            tenant_id,
            {
                "event": "record_updated",
                "entity_type": DELETED_USER_REALTIME_ENTITY_TYPE,
                "record_id": f"{DELETED_USER_REALTIME_ENTITY_TYPE}:{membership_id}",
                "created": False,
                "data": {"deleted": True},
            },
        )


def _actor(request) -> str:
    user = getattr(request, "user", None)
    return str(getattr(user, "email", None) or getattr(user, "supabase_uid", None) or "")


def _candidate_memberships(*, tenant, uid, email):
    """The live memberships delete_user_everywhere can remove for this request."""
    match = Q()
    if email:
        match |= Q(email=email)
    if uid:
        match |= Q(user_id=uid)
    if not match:
        return []
    return list(
        TenantMembership.objects.select_related("role", "user_parent_id").filter(tenant=tenant).filter(match)
    )


def _snapshot(membership) -> dict:
    manager = membership.user_parent_id
    return {
        "membership_id": membership.id,
        "name": membership.name or "",
        "email": membership.email,
        "role_id": membership.role_id,
        "role_name": membership.role.name if membership.role_id else "",
        "manager_name": (manager.name or "") if manager else "",
        "manager_email": manager.email if manager else "",
        "account_created_at": membership.created_at,
    }


def _save_delete_logs(rows: list[DeletedUserLog]) -> list[DeletedUserLog]:
    """
    Save the log rows in their own savepoint. If the log table is missing or the
    write fails, the delete still goes through and only the log is skipped.
    """
    if not rows:
        return []
    try:
        with transaction.atomic():
            return DeletedUserLog.objects.bulk_create(rows)
    except DatabaseError:
        logger.exception(
            "[DeleteUserWithLog] Could not save deleted-user log for %s; the user was still deleted",
            [row.email for row in rows],
        )
        return []


def _logged_deletions(tenant) -> list[DeletedUserLog]:
    """Log rows for the tenant, newest first, or none if the log table can't be read yet."""
    try:
        with transaction.atomic():
            return list(DeletedUserLog.objects.filter(tenant=tenant).order_by("-deleted_at", "-id"))
    except DatabaseError:
        logger.exception("[DeletedUsersList] Could not read deleted-user log; returning an empty list")
        return []


class DeleteUserWithLogView(APIView):
    """
    DELETE /accounts/delete-user-with-log/
    Body: same as /accounts/delete-user/.
    """

    permission_classes = [IsTenantAuthenticated]

    def delete(self, request):
        try:
            ser = DeleteUserEverywhereSerializer(data=request.data, context={"request": request})
            ser.is_valid(raise_exception=True)

            tenant = ser.validated_data["_tenant"]
            uid = ser.validated_data.get("uid")
            email = ser.validated_data.get("email")
            role_id = ser.validated_data.get("role_id")
            deleted_by = _actor(request)

            with transaction.atomic():
                snapshots = {m.id: _snapshot(m) for m in _candidate_memberships(tenant=tenant, uid=uid, email=email)}
                report = delete_user_everywhere(tenant=tenant, uid=uid, email=email, role_id=role_id)
                removed = TenantMembership.all_objects.filter(id__in=list(snapshots), is_deleted=True)
                now = timezone.now()
                logs = _save_delete_logs(
                    [
                        DeletedUserLog(
                            tenant=tenant,
                            deleted_at=membership.deleted_at or now,
                            deleted_by=deleted_by,
                            **snapshots[membership.id],
                        )
                        for membership in removed
                    ]
                )
                if logs:
                    deleted_ids = [log.membership_id for log in logs]
                    transaction.on_commit(lambda: _broadcast_user_deleted(tenant.id, deleted_ids))

            return Response(
                {
                    "success": True,
                    "matched_by": report.get("matched_by"),
                    "tenant_id": report.get("tenant_id"),
                    "resolved_uid": report.get("resolved_uid"),
                    "deleted": report.get("deleted"),
                    "memberships_deactivated": report.get("memberships_deactivated", 0),
                    "reports_reassigned": report.get("reports_reassigned", 0),
                    "sessions_revoked": report.get("sessions_revoked", []),
                    "notes": report.get("notes", []),
                    "logged": len(logs),
                },
                status=status.HTTP_200_OK,
            )
        except serializers.ValidationError as exc:
            return Response({"success": False, "error": exc.detail}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as exc:
            logger.error("DeleteUserWithLogView error: %s", exc, exc_info=True)
            return Response(
                {"success": False, "error": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


def _log_row(log: DeletedUserLog) -> dict:
    return {
        "id": f"log-{log.id}",
        "name": log.name,
        "email": log.email,
        "role_id": str(log.role_id) if log.role_id else None,
        "role_name": log.role_name or None,
        "manager_name": log.manager_name or None,
        "manager_email": log.manager_email or None,
        "created_at": log.account_created_at,
        "deleted_at": log.deleted_at,
        "deleted_by": log.deleted_by or None,
    }


class DeletedUsersListView(APIView):
    """
    GET /accounts/deleted-users/ — users removed through delete-user-with-log, newest first.

    Memberships soft-deleted before the log existed are not listed: many tenants have
    repeat deletes and re-added users there, which made the list look like everyone.
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        rows = [_log_row(log) for log in _logged_deletions(request.tenant)]
        return Response({"count": len(rows), "results": rows}, status=status.HTTP_200_OK)
