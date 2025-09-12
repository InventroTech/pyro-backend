from typing import Dict
from datetime import timedelta
from django.utils import timezone
from django.db import transaction
from authz.models import (
    TenantMembership, RolePermission, Permission,
    GroupMembership, GroupPermission, GroupRole, UserPermission
)
from accounts.models import LegacyUser, LegacyRole

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
    Map a legacy role ID to the corresponding authz Role.
    
    Args:
        legacy_role_id: UUID of the legacy role
        tenant: Tenant instance
    
    Returns:
        Role: The corresponding authz Role instance
        
    Raises:
        Exception: If legacy role not found or no corresponding authz role exists
    """
    # Get the legacy role
    try:
        legacy_role = LegacyRole.objects.get(id=legacy_role_id, tenant=tenant)
    except LegacyRole.DoesNotExist:
        raise Exception(f"Legacy role with ID {legacy_role_id} not found")
    
    # Map legacy role name to authz role key
    # This mapping can be customized based on your role naming conventions
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
    
    # Try to find authz role by key first, then by name
    authz_role_key = role_name_mapping.get(legacy_role.name)
    
    if authz_role_key:
        try:
            return LegacyRole.objects.get(tenant=tenant, key=authz_role_key)
        except LegacyRole.DoesNotExist:
            pass
    
    # If no direct mapping, try to find by name match
    try:
        return LegacyRole.objects.get(tenant=tenant, name__iexact=legacy_role.name).id
    except LegacyRole.DoesNotExist:
        raise Exception(f"No corresponding authz role found for legacy role '{legacy_role.name}'")


def link_user_uid_and_activate(email: str, uid: str) -> dict:
    """
    Link a Supabase UID to a user in the legacy users table and activate 
    their tenant membership. This replaces the functionality of the edge function.
    
    This function:
    1. Links the UID to the users table (public.users)
    2. Links the UID to the authz_tenantmembership table and activates the user
    
    Args:
        email: User's email address
        uid: Supabase user ID (UUID)
    
    Returns:
        dict: Result containing success status and message
        
    Raises:
        Exception: If user not found or linking fails
    """
    try:
        with transaction.atomic():
            email_normalized = email.lower().strip()
            
            # Step 1: Update the users table with the UID
            # Check if the email exists in public.users
            user = LegacyUser.objects.filter(email=email_normalized).first()
            if not user:
                raise Exception(f"User with email {email} not found in users table")
            
            # Link the UID to public.users (like the edge function does)
            user.uid = uid
            user.save()
            
            # Step 2: Find and activate tenant memberships for this user
            # Find all tenant memberships for this email that don't have a user_id yet
            memberships = TenantMembership.objects.filter(
                email=email_normalized,
                user_id__isnull=True  # Only update memberships that don't have user_id set
            )
            
            activated_count = 0
            for membership in memberships:
                # Link the UID to authz_tenantmembership and activate the user
                membership.user_id = uid
                membership.is_active = True
                membership.save()
                activated_count += 1
                
                # Clear permissions cache for this user-tenant combination
                drop_permissions_cache(uid, membership.tenant)
            
            return {
                'success': True,
                'message': f'User linked successfully. Updated users table and activated {activated_count} tenant memberships.',
                'user_id': str(user.id),
                'uid': uid,
                'activated_memberships': activated_count,
                'tables_updated': ['users', 'authz_tenantmembership']
            }
            
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': f'Failed to link user: {str(e)}'
        }
