from django.urls import path
from .views import (
    LeadPushWebhookView,
    WIPLeadsView,
    AllMyLeadsView,
    LeadStatsView,
    GetNextLead,
    SaveAndContinueLeadView,
    AllLeadsView,
    TakeBreakLeadView,
    LeadDetailUpdateView,
    LeadScoreUpdateView
    
)
from .views_cron import CronFixLeadsView
from .views_calls import LeadCallOutcomeView

app_name = 'crm'

urlpatterns = [
    path('leads/wip/', WIPLeadsView.as_view(), name='wip-leads'),
    path('my-leads/', AllMyLeadsView.as_view(), name='all-my-leads'),
    path('leads/stats/', LeadStatsView.as_view(), name='stats-leads'),
    path("leads/get-next-lead/", GetNextLead.as_view(), name="get-next-lead"),
    path("leads/save-and-continue/", SaveAndContinueLeadView.as_view(), name="save-and-continue-lead"),
    path("leads/", AllLeadsView.as_view(), name="all-leads"),
    path('leads/take-break/', TakeBreakLeadView.as_view(), name='take-break-lead'),
    path("leads/<int:lead_id>/call-outcome/", LeadCallOutcomeView.as_view(), name="lead-call-outcome"),
    path("internal/cron/fix-leads/", CronFixLeadsView.as_view(), name="cron-fix-leads"),
    path("leads/<int:pk>/", LeadDetailUpdateView.as_view(), name="lead-detail-update"),
    path("leads/update-score/", LeadScoreUpdateView.as_view(), name="lead-score-update"),
    path("leads/push-webhook/", LeadPushWebhookView.as_view(), name="lead-push-webhook"),
]