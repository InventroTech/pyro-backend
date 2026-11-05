from django.urls import path
from .deleted_users_log import DeletedUsersListView, DeleteUserWithLogView
from .views import (
    LinkUserUidView,
    TenantMembershipCreateView,
    TenantMembershipUpdateView,
    AssigneesByRoleView,
    DeleteUserEverywhereView,
    SetupNewTenantView,
)

urlpatterns = [
    path("users/create/", TenantMembershipCreateView.as_view(), name="tenant-membership-create"),
    path("users/update/", TenantMembershipUpdateView.as_view(), name="tenant-membership-update"),
    path("users/assignees-by-role/", AssigneesByRoleView.as_view(), name="assignees-by-role"),
    path("link-user-uid/", LinkUserUidView.as_view(), name="link_user_uid"),
    path("delete-user/", DeleteUserEverywhereView.as_view(), name="delete-user-everywhere"),
    path("delete-user-with-log/", DeleteUserWithLogView.as_view(), name="delete-user-with-log"),
    path("deleted-users/", DeletedUsersListView.as_view(), name="deleted-users"),
    path("setup-new-tenant/", SetupNewTenantView.as_view(), name="setup-new-tenant"),
]
