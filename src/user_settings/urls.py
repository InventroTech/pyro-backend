from django.urls import path
from .views import (
    UserSettingsListView,
    UserSettingsDetailView,
    LeadTypeAssignmentView,
    UserLeadTypesView,
    LeadTypesListView,
    RoutingRuleListCreateView,
    RoutingRuleDetailView,
)

urlpatterns = [
    # General user settings endpoints
    path("settings/", UserSettingsListView.as_view(), name="user-settings-list"),
    path(
        "settings/<uuid:user_id>/<str:key>/",
        UserSettingsDetailView.as_view(),
        name="user-settings-detail",
    ),
    # Lead type assignment endpoints
    path(
        "lead-type-assignments/",
        LeadTypeAssignmentView.as_view(),
        name="lead-type-assignments",
    ),
    path(
        "users/<uuid:user_id>/lead-types/",
        UserLeadTypesView.as_view(),
        name="user-lead-types",
    ),
    path("lead-types/", LeadTypesListView.as_view(), name="lead-types-list"),
    # Routing rules endpoints (GM / tenant admin)
    path(
        "routing-rules/",
        RoutingRuleListCreateView.as_view(),
        name="routing-rules-list-create",
    ),
    path(
        "routing-rules/<int:pk>/",
        RoutingRuleDetailView.as_view(),
        name="routing-rules-detail",
    ),
]

