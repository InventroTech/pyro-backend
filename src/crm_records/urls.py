from django.urls import path
from .views import RecordListCreateView, RecordDetailView, EntityProxyView, RecordEventView, EventLogListView, EventLogCountView, GetNextLeadView, LeadStatsView, PrajaLeadsAPIView, EntityTypeSchemaListCreateView, EntityTypeSchemaDetailView, EntityTypeSchemaByTypeView, EntityTypeAttributesView, LeadScoringView, GetMyCurrentLeadView, PartnerEventsView, PartnerLeadView, CallAttemptMatrixListCreateView, CallAttemptMatrixDetailView, CallAttemptMatrixByLeadTypeView, LeadAssignmentWebhookProxyView, RMAssignedMixpanelView, ScoringRuleListCreateView, ScoringRuleDetailView, ApiSecretKeySetView, ApiSecretKeyUpdateView, LeadFollowupListView
from .admin_views import RuleSetListCreateView, RuleExecutionLogListView
from .public_views import PublicJobsView, PublicJobApplicationView


app_name = "crm_records"

urlpatterns = [
    # Universal endpoint - supports entity_type filtering
    path("records/", RecordListCreateView.as_view(), name="record-list"),
    # Detail endpoint - supports retrieve/update by ID (including PATCH)
    path("records/<int:pk>/", RecordDetailView.as_view(), name="record-update"),
    path("records/detail/", RecordDetailView.as_view(), name="record-detail"),
    path("records/events/", RecordEventView.as_view(), name="record-events"),
    # Event logging endpoints (admin only)
    path("events/", EventLogListView.as_view(), name="event-log-list"),
    path("events/count/", EventLogCountView.as_view(), name="event-log-count"),
    
    # Rule management endpoints (admin only)
    path("rules/", RuleSetListCreateView.as_view(), name="rule-list"),
    path("rule-logs/", RuleExecutionLogListView.as_view(), name="rule-execution-log-list"),
    
    # Entity-specific aliases (friendly URLs)
    path("leads/", EntityProxyView.as_view(entity_type="lead"), name="lead-list"),
    
    # Get next lead endpoint
    path("leads/next/", GetNextLeadView.as_view(), name="get-next-lead"),
    # Follow-up notifications for RM (due/soon next_call_at)
    path("leads/followups/", LeadFollowupListView.as_view(), name="lead-followups"),
    
    # Get my current assigned lead (always from DB, no cache)
    path("leads/current/", GetMyCurrentLeadView.as_view(), name="get-my-current-lead"),
    # Partner-assigned lead (e.g. Halocom)
    path("leads/partner/<str:partner_slug>/", PartnerLeadView.as_view(), name="partner-lead"),
    # Partner events webhook (X-Secret-Pyro)
    path("partner/events/", PartnerEventsView.as_view(), name="partner-events"),
    # Lead statistics
    path("leads/stats/", LeadStatsView.as_view(), name="lead-stats"),
    
    # Lead scoring endpoint
    path("leads/score/", LeadScoringView.as_view(), name="lead-scoring"),
    
    # Public endpoints - NO authentication required
    path("public/jobs/", PublicJobsView.as_view(), name="public-jobs"),
    path("public/applications/", PublicJobApplicationView.as_view(), name="public-applications"),
    
    # Entity Type Schema endpoints
    path("entity-schemas/", EntityTypeSchemaListCreateView.as_view(), name="entity-schema-list-create"),
    path("entity-schemas/<int:pk>/", EntityTypeSchemaDetailView.as_view(), name="entity-schema-detail"),
    path("entity-schemas/by-type/", EntityTypeSchemaByTypeView.as_view(), name="entity-schema-by-type"),
    path("entity-attributes/", EntityTypeAttributesView.as_view(), name="entity-attributes"),
    
    # Call Attempt Matrix endpoints
    path("call-attempt-matrix/", CallAttemptMatrixListCreateView.as_view(), name="call-attempt-matrix-list-create"),
    path("call-attempt-matrix/<int:pk>/", CallAttemptMatrixDetailView.as_view(), name="call-attempt-matrix-detail"),
    path("call-attempt-matrix/by-lead-type/", CallAttemptMatrixByLeadTypeView.as_view(), name="call-attempt-matrix-by-lead-type"),
    
    # Webhook proxy endpoints
    path("webhooks/lead-assigned/", LeadAssignmentWebhookProxyView.as_view(), name="lead-assignment-webhook-proxy"),
    
    # Mixpanel endpoints
    path("mixpanel/rm-assigned/", RMAssignedMixpanelView.as_view(), name="rm-assigned-mixpanel"),
    
    # Scoring Rules CRUD endpoints
    path("scoring-rules/", ScoringRuleListCreateView.as_view(), name="scoring-rule-list-create"),
    path("scoring-rules/<int:pk>/", ScoringRuleDetailView.as_view(), name="scoring-rule-detail"),
    # API secret for /entity/ (X-Secret-Pyro): POST create, PUT update by id
    path("api-secret-keys/", ApiSecretKeySetView.as_view(), name="api-secret-key-set"),
    path("api-secret-keys/<int:pk>/", ApiSecretKeyUpdateView.as_view(), name="api-secret-key-update"),
]
