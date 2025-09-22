from django.urls import path
from .views_management import (
    ListTenantUsersView,
    RolesView
)

urlpatterns = [
    path("users", ListTenantUsersView.as_view(), name="authz_list_users"),
    path("roles", RolesView.as_view(), name="authz_roles"),
]
