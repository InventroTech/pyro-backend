from typing import Dict
from datetime import timedelta
from django.utils import timezone
import logging
from authz.models import (
    TenantMembership, RolePermission, Permission,
    GroupMembership, GroupPermission, GroupRole, UserPermission
)
from authz.models import Role as AuthzRole

import uuid
from django.db import transaction, IntegrityError
from django.db.models import Q

_CACHE: Dict[str, dict] = {}
_TTL = timedelta(minutes=10)
logger = logging.getLogger(__name__)

def _normalize_tenant_id(tenant_or_id) -> str:
    """
    Accept either a tenant object or raw tenant id and return a stable cache key part.
    This avoids cache key mismatches when callers pass model instances.
    """
    if hasattr(tenant_or_id, "id"):
        return str(getattr(tenant_or_id, "id"))
    return str(tenant_or_id)

def _cache_key(user_uuid: str, tenant_or_id) -> str:
    return f"{str(user_uuid)}:{_normalize_tenant_id(tenant_or_id)}"

def drop_permissions_cache(user_uuid: str, tenant_id) -> None:
    _CACHE.pop(_cache_key(user_uuid, tenant_id), None)

def get_effective_permissions(user_uuid: str, tenant) -> dict:
    key = _cache_key(user_uuid, tenant)
    hit = _CACHE.get(key)
    if hit and hit['exp'] > timezone.now():
        return hit['val']

    member = TenantMembership.objects.filter(
        tenant=tenant, user_id=user_uuid, is_active=True
    ).select_related('role').first()
    if not member:
        val = {'role_key': None, 'perm_keys': set()}
        # Do not cache "no membership" results for long periods; this can
        # cause temporary 403s right after signup/link flows.
        return val

    role_perm_keys = Permission.objects.filter(
        id__in=RolePermission.objects.filter(role=member.role).values('permission_id')
    ).values_list('perm_key', flat=True)

    group_ids = GroupMembership.objects.filter(
        user_id=user_uuid, group__tenant=tenant
    ).values_list('group_id', flat=True)

    grp_perm_keys = Permission.objects.filter(
        id__in=GroupPermission.objects.filter(group_id__in=group_ids).values('permission_id')
    ).values_list('perm_key', flat=True)

    grp_role_perm_keys = Permission.objects.filter(
        id__in=RolePermission.objects.filter(
            role_id__in=GroupRole.objects.filter(group_id__in=group_ids).values('role_id')
        ).values('permission_id')
    ).values_list('perm_key', flat=True)

    allow_keys = set(role_perm_keys) | set(grp_perm_keys) | set(grp_role_perm_keys)

    # Per-user overrides are stored against TenantMembership; use the resolved member.
    user_allow = set()
    user_deny = set()
    if member:
        user_allow = set(Permission.objects.filter(
            id__in=UserPermission.objects.filter(
                membership=member, effect='allow'
            ).values('permission_id')
        ).values_list('perm_key', flat=True))

        user_deny = set(Permission.objects.filter(
            id__in=UserPermission.objects.filter(
                membership=member, effect='deny'
            ).values('permission_id')
        ).values_list('perm_key', flat=True))

    final = (allow_keys | user_allow) - user_deny
    val = {'role_key': member.role.key, 'perm_keys': final}
    _CACHE[key] = {'val': val, 'exp': timezone.now()+_TTL}
    return val


def get_authz_role_from_legacy_role(legacy_role_id: str, tenant):
    """
    Resolve role_id to AuthZ Role. role_id must be an AuthZ role ID for this tenant.
    """
    return AuthzRole.objects.get(id=legacy_role_id, tenant=tenant)


def link_user_uid_and_activate(email: str, uid: str) -> dict:
    """
    NEW: Link a Supabase UID to TenantMembership and activate user.
    No longer updates LegacyUser (public.users) - only TenantMembership.
    
    This function:
    1. Finds TenantMembership records by email
    2. Links the UID and activates the user
    
    Args:
        email: User's email address
        uid: Supabase user ID (UUID)
    
    Returns:
        dict: Result containing success status and message
    """
    try:
        with transaction.atomic():
            email_normalized = email.lower().strip()
            
            # IDEMPOTENT: Check if user is already linked with the SAME UID first
            existing_with_same_uid = TenantMembership.objects.filter(
                email=email_normalized,
                user_id=uid
            )
            
            if existing_with_same_uid.exists():
                # User is already linked with this UID - return success (idempotent)
                activated_count = existing_with_same_uid.filter(is_active=True).count()
                membership_ids = [str(m.id) for m in existing_with_same_uid]
                return {
                    'success': True,
                    'message': f'User already linked. {activated_count} tenant membership(s) active.',
                    'uid': uid,
                    'activated_memberships': activated_count,
                    'membership_ids': membership_ids,
                    'already_linked': True
                }
            
            # Check if user is linked with a DIFFERENT UID
            existing_with_different_uid = TenantMembership.objects.filter(
                email=email_normalized,
                user_id__isnull=False
            ).exclude(user_id=uid).exists()
            
            if existing_with_different_uid:
                # User is linked with different UID - return success but with warning (idempotent)
                # This allows the frontend to handle gracefully without 400 errors
                return {
                    'success': True,
                    'message': f'User with email {email} is already linked to a different Supabase account',
                    'uid': uid,
                    'activated_memberships': 0,
                    'membership_ids': [],
                    'already_linked_different_uid': True
                }
            
            # Find TenantMembership records that need linking (no user_id set)
            memberships = TenantMembership.objects.filter(
                email=email_normalized,
                user_id__isnull=True
            )
            
            if not memberships.exists():
                # No TenantMembership found - return success with info (idempotent)
                # This prevents 400 errors for users not yet added to tenants
                return {
                    'success': True,
                    'message': f'User with email {email} not found in TenantMembership table. Please ensure the user is added to a tenant first.',
                    'uid': uid,
                    'activated_memberships': 0,
                    'membership_ids': [],
                    'no_tenant_membership': True,
                    'code': 'NO_TENANT_MEMBERSHIP'
                }
            
            # Link the UID and activate memberships in one DB statement to reduce lock time.
            # We capture ids/tenant ids first for response and cache invalidation.
            memberships_snapshot = list(memberships.values_list("id", "tenant_id"))
            membership_ids = [str(membership_id) for membership_id, _ in memberships_snapshot]
            tenant_ids = {str(tenant_id) for _, tenant_id in memberships_snapshot}
            activated_count = memberships.update(user_id=uid, is_active=True)

            # Clear permissions cache for affected user-tenant combinations.
            for tenant_id in tenant_ids:
                drop_permissions_cache(uid, tenant_id)
            
            return {
                'success': True,
                'message': f'User linked successfully. Activated {activated_count} tenant membership(s).',
                'uid': uid,
                'activated_memberships': activated_count,
                'membership_ids': membership_ids,
                'tables_updated': ['authz_tenantmembership']
            }
            
    except Exception as e:
        logger.error(
            "Failed linking UID for email=%s uid=%s: %s",
            email,
            uid,
            str(e),
            exc_info=True,
        )
        return {
            'success': False,
            'error': str(e),
            'message': f'Failed to link user: {str(e)}'
        }



def create_or_sync_role(tenant, key: str, name: str, description: str = ""):
    """
    NEW: Idempotent, tenant-scoped role creation.
    - Case-insensitive uniqueness on key.
    - Creates ONLY in authz_role (no longer creates LegacyRole).
    - If already exists, returns the existing record.
    Returns: { created: bool, role: {...} }
    
    DEPRECATED: LegacyRole creation removed.
    """
    norm_key = (key or "").strip()
    norm_name = (name or "").strip()
    norm_desc = (description or "").strip() or None

    # Quick path: if exists (any case), return existing (no LegacyRole sync)
    existing = AuthzRole.objects.filter(tenant=tenant, key__iexact=norm_key).first()
    if existing:
        return {
            "created": False,
            "role": {
                "id": str(existing.id),
                "tenant_id": str(tenant.id),
                "key": existing.key,
                "name": existing.name,
                "description": existing.description,
            },
        }

    # Create new authz role (no LegacyRole)
    new_id = uuid.uuid4()
    try:
        with transaction.atomic():
            authz_role = AuthzRole.objects.create(
                id=new_id,
                tenant=tenant,
                key=norm_key,
                name=norm_name,
                description=norm_desc,
            )
        return {
            "created": True,
            "role": {
                "id": str(new_id),
                "tenant_id": str(tenant.id),
                "key": authz_role.key,
                "name": authz_role.name,
                "description": authz_role.description,
            },
        }
    except IntegrityError:
        # Another request created it concurrently. Re-fetch.
        winner = AuthzRole.objects.get(tenant=tenant, key__iexact=norm_key)
        return {
            "created": False,
            "role": {
                "id": str(winner.id),
                "tenant_id": str(tenant.id),
                "key": winner.key,
                "name": winner.name,
                "description": winner.description,
            },
        }
