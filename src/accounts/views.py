import re
import uuid
from typing import Optional
from django.db import DatabaseError, transaction, connection, close_old_connections
from django.db.utils import InterfaceError, OperationalError
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, serializers
from django.conf import settings
from accounts.serializers import TenantMembershipCreateSerializer, TenantMembershipUpdateSerializer
from authz.permissions import IsTenantAuthenticated, HasTenantRole
from authz.service import get_authz_role_from_legacy_role  # DEPRECATED: Will be removed
from authz.models import TenantMembership, Role
from user_settings.models import Group
from user_settings.services import upsert_user_kv_settings
from rest_framework.permissions import IsAuthenticated, AllowAny
from core.models import Tenant

from django.db.models import Q, Subquery
from .models import DeletedUserLog
from .serializers import LinkUserUidSerializer, DeleteUserEverywhereSerializer
from authz.service import link_user_uid_and_activate, drop_permissions_cache
from accounts.services.delete_user_everywhere import delete_user_everywhere
from realtime.broadcast import broadcast_to_tenant
import logging

logger = logging.getLogger(__name__)


def _apply_group_and_assignment(
    tenant,
    membership: TenantMembership,
    lead_group_name: Optional[str],
    daily_target,
    daily_limit,
    state: Optional[int] = None,
    district: Optional[int] = None,
    party: Optional[int] = None,
    update_group: bool = True,
    update_daily_target: bool = True,
    update_daily_limit: bool = True,
    update_state: bool = False,
    update_district: bool = False,
    update_party: bool = False,
):
    """Bind user to a group and sync core TenantMemberSetting KV rows."""
    from user_settings.models import TenantMemberSetting
    from user_settings.services import USER_KV_GROUP_ID_KEY, coerce_kv_int

    group = None
    if update_group:
        normalized_group_name = (lead_group_name or "").strip()
        if normalized_group_name:
            group = Group.objects.filter(tenant=tenant, name__iexact=normalized_group_name).first()
            if not group:
                raise serializers.ValidationError({"lead_group_name": "Lead group not found for this tenant."})
    else:
        existing = TenantMemberSetting.objects.filter(
            tenant=tenant,
            tenant_membership=membership,
            key=USER_KV_GROUP_ID_KEY,
        ).first()
        existing_group_id = coerce_kv_int(existing.value) if existing else None
        if existing_group_id is not None:
            group = Group.objects.filter(tenant=tenant, id=existing_group_id).first()

    upsert_user_kv_settings(
        tenant=tenant,
        tenant_membership=membership,
        group_id=group.id if group else None,
        daily_target=daily_target,
        daily_limit=daily_limit,
        state=state,
        district=district,
        party=party,
        update_group=update_group,
        update_daily_target=update_daily_target,
        update_daily_limit=update_daily_limit,
        update_state=update_state,
        update_district=update_district,
        update_party=update_party,
    )

    return group


def _run_atomic_with_connection_retry(fn, *, label: str):
    """
    Run ``fn`` in a transaction; if the DB connection drops (e.g. the Supabase pooler
    closes it mid-request), open a fresh connection and run it once more. ``fn`` must be
    safe to repeat: the failed attempt is rolled back by Postgres.
    """
    try:
        with transaction.atomic():
            return fn()
    except (InterfaceError, OperationalError) as e:
        logger.warning(f"DB connection lost during {label}; retrying once: {e}")
        close_old_connections()
        with transaction.atomic():
            return fn()


def _resolve_manager_membership(tenant, manager_email: Optional[str], *, self_membership_id=None):
    """Map manager_email to a tenant membership, or None to clear the parent link."""
    if manager_email is None:
        return None
    normalized = (manager_email or "").strip().lower()
    if not normalized:
        return None
    parent = TenantMembership.objects.filter(tenant=tenant, email=normalized).first()
    if not parent:
        raise serializers.ValidationError(
            {"manager_email": f"Manager with email {normalized} not found in this tenant"}
        )
    if self_membership_id is not None and parent.id == self_membership_id:
        raise serializers.ValidationError({"manager_email": "A user cannot be their own manager"})
    return parent


def _read_geo_kv(tenant, membership) -> dict:
    """Return current STATE / DISTRICT / PARTY ints for API responses."""
    from user_settings.models import TenantMemberSetting
    from user_settings.services import (
        USER_KV_DISTRICT_KEY,
        USER_KV_PARTY_KEY,
        USER_KV_STATE_KEY,
        coerce_kv_int,
    )

    rows = TenantMemberSetting.objects.filter(
        tenant=tenant,
        tenant_membership=membership,
        key__in=[USER_KV_STATE_KEY, USER_KV_DISTRICT_KEY, USER_KV_PARTY_KEY],
    )
    by_key = {row.key: coerce_kv_int(row.value) for row in rows}
    return {
        "state": by_key.get(USER_KV_STATE_KEY),
        "district": by_key.get(USER_KV_DISTRICT_KEY),
        "party": by_key.get(USER_KV_PARTY_KEY),
    }

class TenantMembershipCreateView(APIView):
    """
    NEW: Creates TenantMembership directly (no longer creates LegacyUser).
    Body: { name, email, [company_name], [department], [role_id], [uid] }
    
    DEPRECATED: LegacyUser creation removed. This endpoint now only creates TenantMembership.
    """
    # permission_classes = [IsTenantAuthenticated, HasTenantRole("GM")]
    permission_classes = [IsTenantAuthenticated]
    def post(self, request):
        ser = TenantMembershipCreateSerializer(data = request.data, context={'request':request})
        ser.is_valid(raise_exception=True)
        tenant = request.tenant
        name = ser.validated_data["name"].strip()
        email = ser.validated_data["email"]
        company_name = ser.validated_data.get("company_name")
        department = (ser.validated_data.get("department") or "").strip() or None
        role_id = ser.validated_data.get("role_id")
        uid = ser.validated_data.get("uid")
        lead_group_name = ser.validated_data.get("lead_group_name")
        daily_target = ser.validated_data.get("daily_target")
        daily_limit = ser.validated_data.get("daily_limit")
        state = ser.validated_data.get("state")
        district = ser.validated_data.get("district")
        party = ser.validated_data.get("party")
        update_manager = "manager_email" in ser.validated_data
        manager_email = ser.validated_data.get("manager_email") if update_manager else None

        if not role_id:
            return Response({
                'error': 'role_id is required to create TenantMembership'
            }, status=status.HTTP_400_BAD_REQUEST)

        with transaction.atomic():
            try:
                # Get the authz role (role_id should be AuthZ role ID, not legacy)
                # Try to get by ID first (assume it's AuthZ role ID)
                try:
                    authz_role = Role.objects.get(id=role_id, tenant=tenant)
                except Role.DoesNotExist:
                    # Fallback: try legacy role mapping (for backward compatibility during transition)
                    try:
                        authz_role = get_authz_role_from_legacy_role(role_id, tenant)
                    except Exception as e:
                        logger.error(f"Failed to find role {role_id} for tenant {tenant.id}: {e}")
                        return Response({
                            'error': f'Role with ID {role_id} not found for this tenant'
                        }, status=status.HTTP_400_BAD_REQUEST)

                parent_membership = None
                if update_manager:
                    try:
                        parent_membership = _resolve_manager_membership(tenant, manager_email)
                    except serializers.ValidationError as ve:
                        return Response(ve.detail, status=status.HTTP_400_BAD_REQUEST)
                
                # Create or update TenantMembership directly (no LegacyUser)
                create_defaults = {
                    'name': name,
                    'company_name': company_name,
                    'department': department,
                    'user_id': uid,
                    'is_active': bool(uid),
                }
                if update_manager:
                    create_defaults['user_parent_id'] = parent_membership
                membership, created = TenantMembership.objects.get_or_create(
                    tenant=tenant,
                    email=email,
                    role=authz_role,
                    defaults=create_defaults,
                )
                
                # If membership already exists, update it
                if not created:
                    membership.name = name
                    if company_name is not None:
                        membership.company_name = company_name
                    if department is not None:
                        membership.department = department
                    membership.role = authz_role
                    if update_manager:
                        membership.user_parent_id = parent_membership
                    if uid:
                        membership.user_id = uid
                        membership.is_active = True
                    membership.save()

                group = _apply_group_and_assignment(
                    tenant=tenant,
                    membership=membership,
                    lead_group_name=lead_group_name,
                    daily_target=daily_target,
                    daily_limit=daily_limit,
                    state=state,
                    district=district,
                    party=party,
                    update_state=True,
                    update_district=True,
                    update_party=True,
                )
                
                # Invalidate permissions cache so newly updated role (e.g. GM) is seen immediately
                # without user having to re-login (permission checks use cached role with 10min TTL)
                if membership.user_id:
                    drop_permissions_cache(str(membership.user_id), membership.tenant)
                
                return Response({
                    'id': str(membership.id),
                    'name': membership.name,
                    'email': membership.email,
                    'tenant_id': str(tenant.id),
                    'company_name': membership.company_name,
                    'department': membership.department,
                    'role_id': str(membership.role.id),
                    'uid': str(membership.user_id) if membership.user_id else None,
                    'is_active': membership.is_active,
                    'lead_group_id': group.id if group else None,
                    'lead_group_name': group.name if group else None,
                    'state': state,
                    'district': district,
                    'party': party,
                    'manager_email': membership.user_parent_id.email if membership.user_parent_id else None,
                    'created': created
                }, status=status.HTTP_201_CREATED)
                        
            except Exception as e:
                logger.error(f"Failed to create TenantMembership for user {email}: {str(e)}", exc_info=True)
                return Response({
                    'error': f'Failed to create user: {str(e)}'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class TenantMembershipUpdateView(APIView):
    """
    Update existing TenantMembership identified by original_email + original_role_id.
    Body: { name, email, department, role_id, original_email, original_role_id }
    """
    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        ser = TenantMembershipUpdateSerializer(data=request.data, context={"request": request})
        ser.is_valid(raise_exception=True)

        tenant = request.tenant
        membership = ser.validated_data["_membership"]
        name = ser.validated_data["name"].strip()
        email = ser.validated_data["email"]
        department = (ser.validated_data.get("department") or "").strip() or None
        role_id = ser.validated_data["role_id"]
        lead_group_name = ser.validated_data.get("lead_group_name")
        daily_target = ser.validated_data.get("daily_target")
        daily_limit = ser.validated_data.get("daily_limit")
        state = ser.validated_data.get("state")
        district = ser.validated_data.get("district")
        party = ser.validated_data.get("party")
        # Only rewrite KV / hierarchy fields that the client actually sent.
        update_group = "lead_group_name" in ser.validated_data
        update_daily_target = "daily_target" in ser.validated_data
        update_daily_limit = "daily_limit" in ser.validated_data
        update_state = "state" in ser.validated_data
        update_district = "district" in ser.validated_data
        update_party = "party" in ser.validated_data
        update_manager = "manager_email" in ser.validated_data
        manager_email = ser.validated_data.get("manager_email") if update_manager else None

        def save_membership():
            try:
                authz_role = Role.objects.get(id=role_id, tenant=tenant)
            except Role.DoesNotExist:
                try:
                    authz_role = get_authz_role_from_legacy_role(role_id, tenant)
                except Exception as e:
                    logger.error(f"Failed to find role {role_id} for tenant {tenant.id}: {e}")
                    return Response({
                        "error": f"Role with ID {role_id} not found for this tenant"
                    }, status=status.HTTP_400_BAD_REQUEST)

            parent_membership = None
            if update_manager:
                try:
                    parent_membership = _resolve_manager_membership(
                        tenant,
                        manager_email,
                        self_membership_id=membership.id,
                    )
                except serializers.ValidationError as ve:
                    return Response(ve.detail, status=status.HTTP_400_BAD_REQUEST)

            membership.name = name
            membership.email = email
            membership.department = department
            membership.role = authz_role
            if update_manager:
                membership.user_parent_id = parent_membership
            membership.save()

            group = _apply_group_and_assignment(
                tenant=tenant,
                membership=membership,
                lead_group_name=lead_group_name,
                daily_target=daily_target,
                daily_limit=daily_limit,
                state=state,
                district=district,
                party=party,
                update_group=update_group,
                update_daily_target=update_daily_target,
                update_daily_limit=update_daily_limit,
                update_state=update_state,
                update_district=update_district,
                update_party=update_party,
            )

            if membership.user_id:
                drop_permissions_cache(str(membership.user_id), membership.tenant)

            geo = _read_geo_kv(tenant, membership)
            return Response({
                "id": str(membership.id),
                "name": membership.name,
                "email": membership.email,
                "tenant_id": str(tenant.id),
                "department": membership.department,
                "role_id": str(membership.role.id),
                "uid": str(membership.user_id) if membership.user_id else None,
                "is_active": membership.is_active,
                "lead_group_id": group.id if group else None,
                "lead_group_name": group.name if group else None,
                "state": geo["state"],
                "district": geo["district"],
                "party": geo["party"],
                "manager_email": membership.user_parent_id.email if membership.user_parent_id else None,
                "updated": True
            }, status=status.HTTP_200_OK)

        try:
            return _run_atomic_with_connection_retry(
                save_membership,
                label=f"update TenantMembership {membership.id}",
            )
        except Exception as e:
            logger.error(f"Failed to update TenantMembership {membership.id}: {str(e)}", exc_info=True)
            return Response({
                "error": f"Failed to update user: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    



class AssigneesByRoleView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        """
        NEW: Using TenantMembership instead of LegacyUser
        Returns assignees filtered by role key (e.g., 'CSE', 'RM')
        """
        role_key = (request.query_params.get('role') or '').strip().upper()
        tenant = request.tenant
        
        if not tenant:
            return Response({'error': "Unable to determine tenant for current user."}, status=403)
        
        # NEW: Query TenantMembership filtered by Role key
        try:
            if role_key:
                # Find role by key (case-insensitive)
                role = Role.objects.filter(tenant=tenant, key__iexact=role_key).first()
                if not role:
                    # Fallback: try to find by name for backward compatibility
                    role = Role.objects.filter(tenant=tenant, name__iexact=role_key).first()
                
                if role:
                    qs = TenantMembership.objects.filter(
                        tenant=tenant,
                        role=role,
                        is_active=True
                    ).select_related('role').order_by('email', 'id')
                else:
                    qs = TenantMembership.objects.none()
            else:
                # Return all active memberships if no role specified
                qs = TenantMembership.objects.filter(
                    tenant=tenant,
                    is_active=True
                ).select_related('role').order_by('email', 'id')
            
            # NEW: Serialize using TenantMembership.name directly (no LegacyUser fallback)
            data = []
            for membership in qs:
                # Use TenantMembership.name field (migrated from LegacyUser)
                name = membership.name or membership.email.split('@')[0] if membership.email else 'Unknown'
                
                data.append({
                    'id': str(membership.id),
                    'name': name,
                    'email': membership.email,
                    'company_name': membership.company_name,
                    'department': membership.department,
                    'uid': str(membership.user_id) if membership.user_id else None,
                    'role': {
                        'id': str(membership.role.id),
                        'key': membership.role.key,
                        'name': membership.role.name
                    }
                })
            
            return Response({'count': len(data), 'results': data})
            
        except Exception as e:
            logger.error(f"Error in AssigneesByRoleView: {e}", exc_info=True)
            # Return empty result on error (no legacy fallback)
            return Response({'count': 0, 'results': []}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


PYRO_ADMIN_ROLE_KEY = "pyro_admin"
PYRO_ADMIN_ROLE_NAME = "PYRO_ADMIN"


def _slugify_tenant_slug(raw: str) -> str:
    """Normalize to lowercase alphanumeric and hyphens (matches Tenant.slug validator)."""
    if not raw or not isinstance(raw, str):
        return ""
    s = re.sub(r"[^a-z0-9\-]", "", raw.lower().strip())
    return re.sub(r"-+", "-", s).strip("-") or ""


class TenantBySlugView(APIView):
    """
    Resolve a tenant from the application database by slug.

    The SPA used to read public.tenants through Supabase PostgREST. That table
    is not the AWS database this API uses.
    """

    permission_classes = [AllowAny]

    def get(self, request):
        slug = _slugify_tenant_slug(request.query_params.get("slug") or "")
        if not slug:
            return Response({"error": "slug is required"}, status=status.HTTP_400_BAD_REQUEST)
        tenant = Tenant.objects.filter(slug=slug).only("id", "slug", "name").first()
        if tenant is None:
            return Response({"error": "Tenant not found"}, status=status.HTTP_404_NOT_FOUND)
        return Response(
            {"id": str(tenant.id), "slug": tenant.slug, "name": tenant.name},
            status=status.HTTP_200_OK,
        )


class SetupNewTenantView(APIView):
    """
    Signup flow: create tenant → PYRO_ADMIN role → TenantMembership.
    POST with Supabase JWT. Body: { "tenant_slug": "...", "tenant_name": "..." (optional) }.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        tenant_slug = (request.data.get("tenant_slug") or "").strip()
        tenant_name = (request.data.get("tenant_name") or "").strip()
        if not tenant_slug:
            return Response({"error": "tenant_slug is required"}, status=status.HTTP_400_BAD_REQUEST)
        slug = _slugify_tenant_slug(tenant_slug)
        if not slug:
            return Response(
                {"error": "tenant_slug must contain at least one letter or number"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        email = (getattr(request.user, "email", None) or "").lower().strip()
        if not email:
            return Response({"error": "User email not found"}, status=status.HTTP_400_BAD_REQUEST)
        supabase_uid = getattr(request.user, "supabase_uid", None)
        if not supabase_uid:
            return Response({"error": "User supabase_uid not found"}, status=status.HTTP_400_BAD_REQUEST)

        existing = (
            TenantMembership.objects.filter(user_id=supabase_uid, is_active=True)
            .select_related("tenant", "role")
            .first()
        )
        if existing:
            return Response(
                {
                    "success": True,
                    "tenant_id": str(existing.tenant.id),
                    "tenant_slug": existing.tenant.slug,
                    "role_id": str(existing.role.id),
                    "role_key": existing.role.key,
                    "message": "Already set up",
                },
                status=status.HTTP_200_OK,
            )

        name = tenant_name or slug.replace("-", " ").title()
        with transaction.atomic():
            tenant, tenant_created = Tenant.objects.get_or_create(
                slug=slug,
                defaults={"id": uuid.uuid4(), "name": name, "created_at": timezone.now()},
            )
            if not tenant_created:
                return Response(
                    {"error": f"Organization slug '{slug}' is already taken"},
                    status=status.HTTP_409_CONFLICT,
                )
            role, _ = Role.objects.get_or_create(
                tenant=tenant,
                key=PYRO_ADMIN_ROLE_KEY,
                defaults={"name": PYRO_ADMIN_ROLE_NAME, "description": "Default admin role"},
            )
            membership, _ = TenantMembership.objects.get_or_create(
                tenant=tenant,
                email=email,
                role=role,
                defaults={
                    "user_id": supabase_uid,
                    "is_active": True,
                    "name": name or email.split("@")[0],
                },
            )
            if membership.user_id != supabase_uid:
                membership.user_id = supabase_uid
                membership.is_active = True
                membership.save(update_fields=["user_id", "is_active"])

        return Response(
            {
                "success": True,
                "tenant_id": str(tenant.id),
                "tenant_slug": tenant.slug,
                "role_id": str(role.id),
                "role_key": role.key,
                "message": "Tenant, PYRO_ADMIN role, and membership created",
            },
            status=status.HTTP_201_CREATED,
        )


class LinkUserUidView(APIView):
    """
    POST: Link Supabase UID to a user and activate tenant memberships.
    """
    permission_classes = [AllowAny]

    def post(self, request):
        try:
            serializer = LinkUserUidSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)

            email = serializer.validated_data["email"]
            uid = serializer.validated_data["uid"]

            result = link_user_uid_and_activate(email, uid)

            if result.get("success") or result.get("code") == "UID_CONFLICT":
                return Response(result, status=status.HTTP_200_OK)
            return Response(result, status=status.HTTP_400_BAD_REQUEST)

        except serializers.ValidationError as ve:
            return Response({"error": ve.detail}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Error in LinkUserUidView.post: {e}", exc_info=True)
            return Response(
                {"error": "Internal server error", "message": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )



class DeleteUserEverywhereView(APIView):
    """
    DELETE /api/accounts/delete-user
    Body:
      - Preferred: { "uid": "<uuid>" }
      - Or:        { "email": "user@example.com", "role_id": "<uuid>" }

    Behavior:
      - Requires authenticated, tenant-scoped caller.
      - GM/OWNER (or whichever role you prefer) can delete users in their tenant.
      - Deletes rows in:
          1) auth.users (by uid)  [cascades public.users via FK]
          2) public.users (any leftovers)
          3) public.authz_tenantmembership (scoped to tenant)
      - Idempotent; returns counts of actually deleted rows.
    """
    permission_classes = [IsTenantAuthenticated]  # adjust to your policy

    def delete(self, request):
        try:
            ser = DeleteUserEverywhereSerializer(data=request.data, context={"request": request})
            ser.is_valid(raise_exception=True)

            tenant = ser.validated_data["_tenant"]
            uid = ser.validated_data.get("uid")
            email = ser.validated_data.get("email")
            role_id = ser.validated_data.get("role_id")

            report = delete_user_everywhere(tenant=tenant, uid=uid, email=email, role_id=role_id)

            # 204 is also OK for delete
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
                },
                status=status.HTTP_200_OK,
            )

        except Exception as e:
            logger.error("DeleteUserEverywhereView error: %s", str(e), exc_info=True)
            return Response(
                {"success": False, "error": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
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