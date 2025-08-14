from django.urls import path
from .views import WIPLeadsView, AllMyLeadsView, LeadStatsView, GetNextLead

app_name = 'crm'

urlpatterns = [
    path('leads/wip/', WIPLeadsView.as_view(), name='wip-leads'),
    path('leads/', AllMyLeadsView.as_view(), name='all-my-leads'),
    path('leads/stats/', LeadStatsView.as_view(), name='stats-leads'),
    path("leads/get-next-lead/", GetNextLead.as_view(), name="get-next-lead"),
]