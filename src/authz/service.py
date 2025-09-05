from typing import Dict
from datetime import timedelta
from django.utils import timezone
from authz.models import (
    TenantMembership, RolePermission, Permission,
    GroupMembership, GroupPermission, GroupRole, UserPermission
)

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
