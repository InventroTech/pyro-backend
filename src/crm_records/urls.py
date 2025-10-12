from django.urls import path
from .views import RecordListCreateView, RecordDetailView, EntityProxyView


urlpatterns = [
    # Universal endpoint - supports entity_type filtering
    path("records/", RecordListCreateView.as_view(), name="record-list"),
    path("records/<int:pk>/", RecordDetailView.as_view(), name="record-detail"),
    
    # Entity-specific aliases (friendly URLs)
    path("leads/", EntityProxyView.as_view(entity_type="lead"), name="lead-list"),
    
]
