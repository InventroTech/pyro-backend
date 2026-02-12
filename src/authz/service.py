from typing import Dict
from datetime import timedelta
from django.utils import timezone
from authz.models import (
    TenantMembership, RolePermission, Permission,
    GroupMembership, GroupPermission, GroupRole, UserPermission
)
from accounts.models import LegacyUser, LegacyRole

import uuid
from django.db import transaction, IntegrityError
from django.db.models import Q
from authz.models import Role as AuthzRole
from accounts.models import LegacyRole

_CACHE: Dict[str, dict] = {}
_TTL = timedelta(minutes=10)

def _cache_key(user_uuid: str, tenant_id) -> str:
    return f"{user_uuid}:{tenant_id}"

def drop_permissions_cache(user_uuid: str, tenant_id) -> None:
    _CACHE.pop(_cache_key(user_uuid, tenant_id), None)

def get_effective_permissions(user_uuid: str, tenant) -> dict:
    key = _cache_key(user_uuid, tenant.id)
    hit = _CACHE.get(key)
    if hit and hit['exp'] > timezone.now():
        return hit['val']

    member = TenantMembership.objects.filter(
        tenant=tenant, user_id=user_uuid, is_active=True
    ).select_related('role').first()
    if not member:
        val = {'role_key': None, 'perm_keys': set()}
        _CACHE[key] = {'val': val, 'exp': timezone.now()+_TTL}
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

    user_allow = set(Permission.objects.filter(
        id__in=UserPermission.objects.filter(
            tenant=tenant, user_id=user_uuid, effect='allow'
        ).values('permission_id')
    ).values_list('perm_key', flat=True))

    user_deny = set(Permission.objects.filter(
        id__in=UserPermission.objects.filter(
            tenant=tenant, user_id=user_uuid, effect='deny'
        ).values('permission_id')
    ).values_list('perm_key', flat=True))

    final = (allow_keys | user_allow) - user_deny
    val = {'role_key': member.role.key, 'perm_keys': final}
    _CACHE[key] = {'val': val, 'exp': timezone.now()+_TTL}
    return val


def get_authz_role_from_legacy_role(legacy_role_id: str, tenant):
    """
    DEPRECATED: Map a legacy role ID to the corresponding authz Role.
    This function is kept for backward compatibility during transition.
    
    NEW: Assumes role_id is already an AuthZ role ID (since LegacyRole is being phased out).
    Falls back to LegacyRole lookup only if AuthZ role not found.
    
    Args:
        legacy_role_id: UUID of the role (assumed to be AuthZ role ID)
        tenant: Tenant instance
    
    Returns:
        Role: The corresponding authz Role instance
        
    Raises:
        Exception: If role not found
    """
    # NEW: Try AuthZ role first (assume role_id is AuthZ role ID)
    try:
        return AuthzRole.objects.get(id=legacy_role_id, tenant=tenant)
    except AuthzRole.DoesNotExist:
        pass
    
    # DEPRECATED: Fallback to LegacyRole lookup (for backward compatibility)
    # This will be removed after migration complete
    try:
        legacy_role = LegacyRole.objects.get(id=legacy_role_id, tenant=tenant)
    except LegacyRole.DoesNotExist:
        raise Exception(f"Role with ID {legacy_role_id} not found in AuthZ or Legacy roles")
    
    # Map legacy role name to authz role key
    role_name_mapping = {
        'General Manager': 'GM',
        'GM': 'GM',
        'Owner': 'OWNER',
        'OWNER': 'OWNER',
        'Agent': 'AGENT',
        'AGENT': 'AGENT',
        'Manager': 'MANAGER',
        'MANAGER': 'MANAGER',
        'Admin': 'ADMIN',
        'ADMIN': 'ADMIN',
    }
    
    authz_role_key = role_name_mapping.get(legacy_role.name)
    if authz_role_key:
        try:
            return AuthzRole.objects.get(tenant=tenant, key__iexact=authz_role_key)
        except AuthzRole.DoesNotExist:
            pass
    
    # Try by name match
    try:
        return AuthzRole.objects.get(tenant=tenant, name__iexact=legacy_role.name)
    except AuthzRole.DoesNotExist:
        raise Exception(f"No corresponding authz role found for legacy role '{legacy_role.name}'")


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
            
            # NEW: Find TenantMembership records (no LegacyUser lookup)
            memberships = TenantMembership.objects.filter(
                email=email_normalized,
                user_id__isnull=True  # Only update memberships that don't have user_id set
            )
            
            if not memberships.exists():
                return {
                    'success': False,
                    'error': f'No TenantMembership found for email {email}',
                    'message': f'User with email {email} not found in TenantMembership table'
                }
            
            activated_count = 0
            membership_ids = []
            for membership in memberships:
                # Link the UID to authz_tenantmembership and activate the user
                membership.user_id = uid
                membership.is_active = True
                membership.save()
                activated_count += 1
                membership_ids.append(str(membership.id))
                
                # Clear permissions cache for this user-tenant combination
                drop_permissions_cache(uid, membership.tenant)
            
            return {
                'success': True,
                'message': f'User linked successfully. Activated {activated_count} tenant membership(s).',
                'uid': uid,
                'activated_memberships': activated_count,
                'membership_ids': membership_ids,
                'tables_updated': ['authz_tenantmembership']
            }
            
    except Exception as e:
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
