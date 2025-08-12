from django.urls import path
from . import views

app_name = 'crm'

urlpatterns = [
    path('leads/', views.get_all_leads, name='get_all_leads'),
    path('leads/create/', views.create_lead, name='create_lead'),
    path('leads/<int:lead_id>/', views.get_lead_by_id, name='get_lead_by_id'),
]
