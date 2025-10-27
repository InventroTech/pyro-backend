from django.urls import path
from .views import RecordListCreateView, RecordDetailView, EntityProxyView, RecordEventView, EventLogListView
from .admin_views import RuleSetListCreateView, RuleExecutionLogListView


urlpatterns = [
    # Universal endpoint - supports entity_type filtering
    path("records/", RecordListCreateView.as_view(), name="record-list"),
    path("records/detail/", RecordDetailView.as_view(), name="record-detail"),
    path("records/events/", RecordEventView.as_view(), name="record-events"),
    
    # Event logging endpoints (admin only)
    path("events/", EventLogListView.as_view(), name="event-log-list"),
    
    # Rule management endpoints (admin only)
    path("rules/", RuleSetListCreateView.as_view(), name="rule-list"),
    path("rule-logs/", RuleExecutionLogListView.as_view(), name="rule-execution-log-list"),
    
    # Entity-specific aliases (friendly URLs)
    path("leads/", EntityProxyView.as_view(entity_type="lead"), name="lead-list"),
    
]
