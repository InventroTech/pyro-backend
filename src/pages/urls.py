from django.urls import path
from .views import (
    PageListCreateView, 
    PageDetailView,
    CustomIconListCreateView,
    CustomIconDetailView
)

urlpatterns = [
    path('', PageListCreateView.as_view(), name='page-list-create'),
    path('<uuid:pk>/', PageDetailView.as_view(), name='page-detail'),
    
    # New Custom Icon routes
    path('custom-icons/', CustomIconListCreateView.as_view(), name='custom-icon-list-create'),
    path('custom-icons/<uuid:pk>/', CustomIconDetailView.as_view(), name='custom-icon-detail'),
]
