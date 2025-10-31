from django.urls import path
from .views import RecordListCreateView, RecordDetailView, EntityProxyView, RecordEventView, EventLogListView, GetNextLeadView, LeadStatsView, PrajaLeadsAPIView
from .admin_views import RuleSetListCreateView, RuleExecutionLogListView


urlpatterns = [
    # Universal endpoint - supports entity_type filtering
    path("records/", RecordListCreateView.as_view(), name="record-list"),
    path("records/<int:pk>/", RecordDetailView.as_view(), name="record-detail"),
    path("records/<int:pk>/events/", RecordEventView.as_view(), name="record-events"),
    
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
    
    # Praja API endpoint - Single endpoint for all CRUD operations
    # POST: CREATE, GET: READ, PATCH: UPDATE score, DELETE: DELETE
    path("praja/leads/", PrajaLeadsAPIView.as_view(), name="praja-leads"),
    
]
