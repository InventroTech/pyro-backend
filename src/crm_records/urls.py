from django.urls import path
from .views import RecordListCreateView, RecordDetailView, EntityProxyView, RecordEventView, EventLogListView, GetNextLeadView, LeadStatsView, PrajaLeadsAPIView, EntityTypeSchemaListCreateView, EntityTypeSchemaDetailView, EntityTypeSchemaByTypeView, EntityTypeAttributesView, LeadScoringView, TrialActivationView, TrialActivationStatsView, GetMyCurrentLeadView
from .admin_views import RuleSetListCreateView, RuleExecutionLogListView
from .public_views import PublicJobsView, PublicJobApplicationView


urlpatterns = [
    # Universal endpoint - supports entity_type filtering
    path("records/", RecordListCreateView.as_view(), name="record-list"),
    path("records/<int:pk>/", RecordListCreateView.as_view(), name="record-update"),  # For PUT with ID in URL
    path("records/detail/", RecordDetailView.as_view(), name="record-detail"),
    path("records/events/", RecordEventView.as_view(), name="record-events"),
    # Event logging endpoints (admin only)
    path("events/", EventLogListView.as_view(), name="event-log-list"),
    
    # Rule management endpoints (admin only)
    path("rules/", RuleSetListCreateView.as_view(), name="rule-list"),
    path("rule-logs/", RuleExecutionLogListView.as_view(), name="rule-execution-log-list"),
    
    # Entity-specific aliases (friendly URLs)
    path("leads/", EntityProxyView.as_view(entity_type="lead"), name="lead-list"),
    
    # Get next lead endpoint
    path("leads/next/", GetNextLeadView.as_view(), name="get-next-lead"),
    
    # Get my current assigned lead (always from DB, no cache)
    path("leads/current/", GetMyCurrentLeadView.as_view(), name="get-my-current-lead"),
    
    # Lead statistics
    path("leads/stats/", LeadStatsView.as_view(), name="lead-stats"),
    
    # Lead scoring endpoint
    path("leads/score/", LeadScoringView.as_view(), name="lead-scoring"),

    # Trial activation endpoints
    path("trials/activations/", TrialActivationView.as_view(), name="trial-activation"),
    path("trials/activations/today/", TrialActivationStatsView.as_view(), name="trial-activation-today"),
    
    # Public endpoints - NO authentication required
    path("public/jobs/", PublicJobsView.as_view(), name="public-jobs"),
    path("public/applications/", PublicJobApplicationView.as_view(), name="public-applications"),
    
    # Entity Type Schema endpoints
    path("entity-schemas/", EntityTypeSchemaListCreateView.as_view(), name="entity-schema-list-create"),
    path("entity-schemas/<int:pk>/", EntityTypeSchemaDetailView.as_view(), name="entity-schema-detail"),
    path("entity-schemas/by-type/", EntityTypeSchemaByTypeView.as_view(), name="entity-schema-by-type"),
    path("entity-attributes/", EntityTypeAttributesView.as_view(), name="entity-attributes"),
    
]
