from django.urls import path
from .views import (
    LinkUserUidView,
    LegacyUserCreateView,
    LegacyUserUpdateView,
    AssigneesByRoleView,
    DeleteUserEverywhereView,
    SetupNewTenantView,
)

urlpatterns = [
    path("users/legacy/create/", LegacyUserCreateView.as_view(), name="legacy-user-create"),
    path("users/legacy/update/", LegacyUserUpdateView.as_view(), name="legacy-user-update"),
    path("users/assignees-by-role/", AssigneesByRoleView.as_view(), name="assignees-by-role"),
    path("link-user-uid/", LinkUserUidView.as_view(), name="link_user_uid"),
    path("delete-user/", DeleteUserEverywhereView.as_view(), name="delete-user-everywhere"),
    path("setup-new-tenant/", SetupNewTenantView.as_view(), name="setup-new-tenant"),
]