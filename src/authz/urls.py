from django.urls import path
from .views_management import (
    ListTenantUsersView,
    RolesView,
    CurrentUserRoleView,
    UpdateUserHierarchyView,
    SpoofTenantUserTokenView,
)

urlpatterns = [
    path("users/", ListTenantUsersView.as_view(), name="authz_list_users"),
    path("users/hierarchy/", UpdateUserHierarchyView.as_view(), name="authz_update_user_hierarchy"),
    path("users/<int:membership_id>/spoof-token/", SpoofTenantUserTokenView.as_view(), name="authz_spoof_user_token"),
    path("roles/", RolesView.as_view(), name="authz_roles"),
    path("me/role/", CurrentUserRoleView.as_view(), name="authz_current_user_role"),
]
