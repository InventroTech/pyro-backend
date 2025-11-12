from django.urls import path
from .views import RecordListCreateView, RecordDetailView, EntityProxyView, RecordEventView, EventLogListView, GetNextLeadView, LeadStatsView, PrajaLeadsAPIView, RecordListCreatePublicView
from .admin_views import RuleSetListCreateView, RuleExecutionLogListView
from .public_views import PublicJobsView, PublicJobApplicationView


urlpatterns = [
    # Universal endpoint - supports entity_type filtering
    path("records/", RecordListCreateView.as_view(), name="record-list"),
    path("records/detail/", RecordDetailView.as_view(), name="record-detail"),
    path("records/events/", RecordEventView.as_view(), name="record-events"),
    path("records/public/", RecordListCreatePublicView.as_view(), name="record-list-public"),
    # Event logging endpoints (admin only)
    path("events/", EventLogListView.as_view(), name="event-log-list"),
    
    # Rule management endpoints (admin only)
    path("rules/", RuleSetListCreateView.as_view(), name="rule-list"),
    path("rule-logs/", RuleExecutionLogListView.as_view(), name="rule-execution-log-list"),
    
    # Entity-specific aliases (friendly URLs)
    path("leads/", EntityProxyView.as_view(entity_type="lead"), name="lead-list"),
    
    # Get next lead endpoint
    path("leads/next/", GetNextLeadView.as_view(), name="get-next-lead"),
    
    # Lead statistics
    path("leads/stats/", LeadStatsView.as_view(), name="lead-stats"),
    
    # External API endpoint - Single endpoint for all CRUD operations
    # POST: CREATE, GET: READ, PATCH: UPDATE score, DELETE: DELETE
    path("external/leads/", PrajaLeadsAPIView.as_view(), name="external-leads"),
    
    # Public endpoints - NO authentication required
    path("public/jobs/", PublicJobsView.as_view(), name="public-jobs"),
    path("public/applications/", PublicJobApplicationView.as_view(), name="public-applications"),
    
]
