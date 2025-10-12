from django.urls import path
from .views import RecordListCreateView, RecordDetailView, EntityProxyView, RecordEventView, EventLogListView


urlpatterns = [
    # Universal endpoint - supports entity_type filtering
    path("records/", RecordListCreateView.as_view(), name="record-list"),
    path("records/<int:pk>/", RecordDetailView.as_view(), name="record-detail"),
    path("records/<int:pk>/events/", RecordEventView.as_view(), name="record-events"),
    
    # Event logging endpoints (admin only)
    path("events/", EventLogListView.as_view(), name="event-log-list"),
    
    # Entity-specific aliases (friendly URLs)
    path("leads/", EntityProxyView.as_view(entity_type="lead"), name="lead-list"),
    
]
