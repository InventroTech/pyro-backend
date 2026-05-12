from rest_framework.permissions import BasePermission

from authz.models import TenantMembership
from authz.service import get_effective_permissions


def _get_membership_info(request):
    if not getattr(request, 'tenant', None) or not getattr(request, 'user', None) or not request.user.is_authenticated:
        return None
    supabase_uid = getattr(request.user, 'supabase_uid', None)
    if not supabase_uid:
        return None
    info = get_effective_permissions(supabase_uid, request.tenant)
    request.user.role_key = info['role_key']
    request.user.perms = info['perm_keys']
    return info

def _has_active_tenant_membership(request):
    """Direct membership check when effective-permissions path does not expose a role_key."""
    tenant = getattr(request, "tenant", None)
    user = getattr(request, "user", None)
    if not tenant or not user or not getattr(user, "is_authenticated", False):
        return False
    supabase_uid = getattr(user, "supabase_uid", None)
    if not supabase_uid:
        return False
    return TenantMembership.objects.filter(
        tenant=tenant,
        user_id=supabase_uid,
        is_active=True,
    ).exists()


class IsTenantAuthenticated(BasePermission):
    """
    Tenant-scoped APIs: user must have an active TenantMembership in request.tenant.

    Prefer resolving role/permissions via get_effective_permissions (cached).
    Fallback: if that yields no role_key but an active TenantMembership row exists,
    still allow — avoids false 403s after signup/link flows or edge cases where
    the effective-permissions path briefly disagrees with the membership table.
    """

    def has_permission(self, request, view):
        info = _get_membership_info(request)
        if info and info.get("role_key"):
            return True
        return _has_active_tenant_membership(request)




def HasTenantRole(role_key: str):
    class _HasTenantRole(BasePermission):
        def has_permission(self, request, view):
            info = _get_membership_info(request)
            return bool(info and info['role_key'] == role_key)
    _HasTenantRole.__name__ = f"HasTenantRole_{role_key}"
    return _HasTenantRole

def HasPermissionKey(perm_key: str):
    class _HasPermissionKey(BasePermission):
        def has_permission(self, request, view):
            info = _get_membership_info(request)
            return bool(info and perm_key in info['perm_keys'])
    _HasPermissionKey.__name__ = f"HasPermissionKey_{perm_key.replace(':','_')}"
    return _HasPermissionKey
