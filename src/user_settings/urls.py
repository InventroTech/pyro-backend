from django.urls import path
from .views import (
    UserSettingsListView,
    UserSettingsDetailView,
    LeadTypeAssignmentView,
    UserLeadTypesView
)

urlpatterns = [
    # General user settings endpoints
    path('settings/', UserSettingsListView.as_view(), name='user-settings-list'),
    path('settings/<uuid:user_id>/<str:key>/', UserSettingsDetailView.as_view(), name='user-settings-detail'),
    
    # Lead type assignment endpoints
    path('lead-type-assignments/', LeadTypeAssignmentView.as_view(), name='lead-type-assignments'),
    path('users/<uuid:user_id>/lead-types/', UserLeadTypesView.as_view(), name='user-lead-types'),
]
