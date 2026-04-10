from django.urls import path
from .views import (
    UserSettingsListView,
    UserSettingsDetailView,
    LeadTypeAssignmentView,
    UserLeadTypesView,
    UserLeadsCountView,
    LeadTypesListView,
    LeadSourcesListView,
    LeadStatusesListView,
    LeadStatesListView,
    QueueTypesListView,
    RoutingRuleListCreateView,
    RoutingRuleDetailView,
    GroupListCreateView,
    GroupDetailView,
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
    path(
        "users/<uuid:user_id>/leads-count/",
        UserLeadsCountView.as_view(),
        name="user-leads-count",
    ),
    path("lead-types/", LeadTypesListView.as_view(), name="lead-types-list"),
    path("lead-sources/", LeadSourcesListView.as_view(), name="lead-sources-list"),
    path("lead-statuses/", LeadStatusesListView.as_view(), name="lead-statuses-list"),
    path("lead-states/", LeadStatesListView.as_view(), name="lead-states-list"),
    path("queue-types/", QueueTypesListView.as_view(), name="queue-types-list"),
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
    path("groups/", GroupListCreateView.as_view(), name="groups-list-create"),
    path("groups/<int:pk>/", GroupDetailView.as_view(), name="groups-detail"),
]

