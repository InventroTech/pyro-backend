from rest_framework.permissions import BasePermission
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

class IsTenantAuthenticated(BasePermission):
    def has_permission(self, request, view):
        info = _get_membership_info(request)
        return bool(info and info['role_key'])
    



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
