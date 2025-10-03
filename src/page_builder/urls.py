from django.urls import path
from .views import (
    CustomTablesView, CustomTableDetailView,
    CustomColumnsView, CustomColumnDetailView, ReorderCustomColumnsView,
    CustomRowsView, CustomRowDetailView,
    PagesView, PageDetailView,
    CardsView, CardDetailView,
    TenantUsersView, TenantView,
    RolesView, UsersView, UserByEmailView, RemoveTenantUserView,
    LeadsView, LeadDetailView, CheckExistingLeadsView,
    CreateLeadsView, CreateLeadsTableRecordView, UpdateLeadsTableRecordView
)

app_name = 'page_builder'

urlpatterns = [
    # Custom Tables
    path('custom-tables/', CustomTablesView.as_view(), name='custom-tables'),
    path('custom-tables/<uuid:pk>/', CustomTableDetailView.as_view(), name='custom-table-detail'),
    
    # Custom Columns
    path('custom-columns/', CustomColumnsView.as_view(), name='custom-columns'),
    path('custom-columns/<uuid:pk>/', CustomColumnDetailView.as_view(), name='custom-column-detail'),
    path('custom-columns/reorder/', ReorderCustomColumnsView.as_view(), name='custom-column-reorder'),
    
    # Custom Rows
    path('custom-rows/', CustomRowsView.as_view(), name='custom-rows'),
    path('custom-rows/<uuid:pk>/', CustomRowDetailView.as_view(), name='custom-row-detail'),
    
    # Pages
    path('pages/', PagesView.as_view(), name='pages'),
    path('pages/<uuid:pk>/', PageDetailView.as_view(), name='page-detail'),
    
    # Cards
    path('cards/', CardsView.as_view(), name='cards'),
    path('cards/<uuid:pk>/', CardDetailView.as_view(), name='card-detail'),
    
    # Tenant info
    path('tenant-users/', TenantUsersView.as_view(), name='tenant-users'),
    path('tenant/', TenantView.as_view(), name='tenant'),
    
    # Roles and Users
    path('roles/', RolesView.as_view(), name='roles'),
    path('users/', UsersView.as_view(), name='users'),
    path('users/by-email/', UserByEmailView.as_view(), name='user-by-email'),
    path('tenant-users/remove/', RemoveTenantUserView.as_view(), name='remove-tenant-user'),
    
    # Leads
    path('leads/', LeadsView.as_view(), name='leads'),
    path('leads/<uuid:lead_id>/', LeadDetailView.as_view(), name='lead-detail'),
    path('leads/check-existing/', CheckExistingLeadsView.as_view(), name='check-existing-leads'),
    path('leads/create/', CreateLeadsView.as_view(), name='create-leads'),
    path('leads/create-record/', CreateLeadsTableRecordView.as_view(), name='create-leads-table-record'),
    path('leads/update-record/<uuid:pk>/', UpdateLeadsTableRecordView.as_view(), name='update-leads-table-record'),
]
