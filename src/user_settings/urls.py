from django.urls import path
from .views import (
    LeadTypeAssignmentView,
    UserLeadTypesView,
    UserCoreKVSettingsView,
    UserLeadsCountView,
    LeadTypesListView,
    LeadSourcesListView,
    LeadStatusesListView,
    LeadStatesListView,
    LeadFilterOptionsView,
    QueueTypesListView,
    GroupListCreateView,
    GroupDetailView,
)

urlpatterns = [
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
        "users/<str:user_id>/core-kv-settings/",
        UserCoreKVSettingsView.as_view(),
        name="user-core-kv-settings",
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
    path("lead-filter-options/", LeadFilterOptionsView.as_view(), name="lead-filter-options"),
    path("queue-types/", QueueTypesListView.as_view(), name="queue-types-list"),
    path("groups/", GroupListCreateView.as_view(), name="groups-list-create"),
    path("groups/<int:pk>/", GroupDetailView.as_view(), name="groups-detail"),
]
