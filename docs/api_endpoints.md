# Backend API list

All 146 API routes registered in Django (generated from the URL config on branch `feat-make-close-leads`, based on `PROD`).

The **Route** column is exactly the `endpoint` label used in Grafana (`http_request_duration_seconds{endpoint="..."}`).

## `/crm-records/` (39)

| Route | Methods | View |
|---|---|---|
| `/crm-records/api-secret-keys/` | POST | `ApiSecretKeySetView` |
| `/crm-records/api-secret-keys/<int:pk>/` | PUT | `ApiSecretKeyUpdateView` |
| `/crm-records/call-attempt-matrix/` | GET,POST | `CallAttemptMatrixListCreateView` |
| `/crm-records/call-attempt-matrix/<int:pk>/` | GET,PUT,PATCH,DELETE | `CallAttemptMatrixDetailView` |
| `/crm-records/call-attempt-matrix/by-lead-type/` | GET | `CallAttemptMatrixByLeadTypeView` |
| `/crm-records/entity-attributes/` | GET | `EntityTypeAttributesView` |
| `/crm-records/entity-schemas/` | GET,POST | `EntityTypeSchemaListCreateView` |
| `/crm-records/entity-schemas/<int:pk>/` | GET,PUT,PATCH,DELETE | `EntityTypeSchemaDetailView` |
| `/crm-records/entity-schemas/by-type/` | GET,POST,PUT,PATCH | `EntityTypeSchemaByTypeView` |
| `/crm-records/entity-types/` | GET | `TenantEntityTypeListView` |
| `/crm-records/events/` | GET | `EventLogListView` |
| `/crm-records/events/count/` | GET | `EventLogCountView` |
| `/crm-records/leads/` | GET,POST | `EntityProxyView` |
| `/crm-records/leads/current/` | GET | `GetMyCurrentLeadView` |
| `/crm-records/leads/make-closed/` | POST | `MakeLeadsClosedView` |
| `/crm-records/leads/make-fresh/` | POST | `MakeLeadsFreshView` |
| `/crm-records/leads/next/` | GET | `GetNextLeadView` |
| `/crm-records/leads/partner/<str:partner_slug>/` | GET | `PartnerLeadView` |
| `/crm-records/leads/score/` | GET,POST | `LeadScoringView` |
| `/crm-records/leads/stats/` | GET | `LeadStatsView` |
| `/crm-records/mixpanel/rm-assigned/` | POST | `RMAssignedMixpanelView` |
| `/crm-records/partner/events/` | POST | `PartnerEventsView` |
| `/crm-records/product-link-extract/` | POST | `ProductLinkExtractView` |
| `/crm-records/public/applications/` | POST | `PublicJobApplicationView` |
| `/crm-records/public/jobs/` | GET | `PublicJobsView` |
| `/crm-records/records/` | GET,POST,PUT,PATCH,DELETE | `RecordListCreateView` |
| `/crm-records/records/<int:pk>/` | GET,PUT,PATCH,DELETE | `RecordDetailView` |
| `/crm-records/records/<int:pk>/history/` | GET | `RecordHistoryView` |
| `/crm-records/records/detail/` | GET,PUT,PATCH,DELETE | `RecordDetailView` |
| `/crm-records/records/distinct-values/` | GET | `RecordDistinctFieldValuesView` |
| `/crm-records/records/events/` | POST | `RecordEventView` |
| `/crm-records/rule-logs/` | GET | `RuleExecutionLogListView` |
| `/crm-records/rules/` | GET,POST | `RuleSetListCreateView` |
| `/crm-records/scoring-rules/` | GET,POST | `ScoringRuleListCreateView` |
| `/crm-records/scoring-rules/<int:pk>/` | GET,PUT,PATCH,DELETE | `ScoringRuleDetailView` |
| `/crm-records/shipment-couriers/` | GET | `ShipmentCouriersView` |
| `/crm-records/shipment-track/` | POST | `ShipmentTrackView` |
| `/crm-records/status-config/` | GET | `RequestStatusConfigView` |
| `/crm-records/webhooks/lead-assigned/` | POST | `LeadAssignmentWebhookProxyView` |

## `/analytics/` (28)

| Route | Methods | View |
|---|---|---|
| `/analytics/board/` | GET,POST | `AnalyticsBoardView` |
| `/analytics/board/<str:report_id>/` | PUT,DELETE | `AnalyticsBoardDetailView` |
| `/analytics/cse-average-resolution-time/` | GET | `CSEAverageResolutionTimeView` |
| `/analytics/cse/filter-options/` | GET | `CseFilterOptionsView` |
| `/analytics/cse/members/` | GET | `CseMembersView` |
| `/analytics/cse/overview/` | GET | `CseOverviewView` |
| `/analytics/cse/time-series/` | GET | `CseTimeSeriesView` |
| `/analytics/get-cse-stats/` | GET | `GetCseStatsView` |
| `/analytics/get-ticket-status/` | GET | `GetTicketStatusView` |
| `/analytics/query/` | POST | `AnalyticsQueryView` |
| `/analytics/rm-active-sessions/` | GET | `RmActiveSessionsView` |
| `/analytics/rm-activity-events/` | GET | `RmActivityEventListView` |
| `/analytics/rm-daily-targets/` | GET | `RmDailyTargetsView` |
| `/analytics/rm-filter-options/` | GET | `RmPrdFilterOptionsView` |
| `/analytics/sla-time/` | GET | `SLATimeView` |
| `/analytics/support-ticket-count/` | GET | `SupportTicketView` |
| `/analytics/support-ticket/` | GET | `SupportTicketListView` |
| `/analytics/support-tickets/filter-options/` | GET | `SupportTicketFilterOptionsView` |
| `/analytics/team/events/` | GET | `TeamEventsView` |
| `/analytics/team/members/` | GET | `TeamMembersView` |
| `/analytics/team/overview/` | GET | `TeamOverviewView` |
| `/analytics/team/time-series/` | GET | `TeamTimeSeriesView` |
| `/analytics/team/unassigned-leads/` | GET | `UnassignedLeadsBreakdownView` |
| `/analytics/ticket-close-time/` | GET | `TicketClosureTimeAnalytics` |
| `/analytics/tickets/daily-average/` | GET | `DailyAverageResolutionTimeView` |
| `/analytics/tickets/daily-percentile/` | GET | `DailyPercentileResolutionTimeView` |
| `/analytics/tickets/resolved/daily/` | GET | `DailyResolvedTicketsView` |
| `/analytics/tickets/stacked-bar-daily/` | GET | `StackedBarResolvedUnresolvedView` |

## `/user-settings/` (15)

| Route | Methods | View |
|---|---|---|
| `/user-settings/geo-party-catalog/` | GET | `GeoPartyCatalogView` |
| `/user-settings/groups/` | GET,POST | `GroupListCreateView` |
| `/user-settings/groups/<int:pk>/` | GET,PUT,DELETE | `GroupDetailView` |
| `/user-settings/inactive-email-policy/` | GET,PATCH | `InactiveEmailPolicyView` |
| `/user-settings/lead-filter-options/` | GET | `LeadFilterOptionsView` |
| `/user-settings/lead-sources/` | GET | `LeadSourcesListView` |
| `/user-settings/lead-states/` | GET | `LeadStatesListView` |
| `/user-settings/lead-statuses/` | GET | `LeadStatusesListView` |
| `/user-settings/lead-type-assignments/` | GET,POST | `LeadTypeAssignmentView` |
| `/user-settings/lead-types/` | GET | `LeadTypesListView` |
| `/user-settings/me/lead-group-summary/` | GET | `MyLeadGroupSummaryView` |
| `/user-settings/queue-types/` | GET | `QueueTypesListView` |
| `/user-settings/users/<str:user_id>/core-kv-settings/` | GET,PATCH | `UserCoreKVSettingsView` |
| `/user-settings/users/<uuid:user_id>/lead-types/` | GET | `UserLeadTypesView` |
| `/user-settings/users/<uuid:user_id>/leads-count/` | GET | `UserLeadsCountView` |

## `/support-ticket/` (10)

| Route | Methods | View |
|---|---|---|
| `/support-ticket/daily-progress/` | GET | `SupportDailyProgressView` |
| `/support-ticket/dump-ticket-webhook/` | POST | `DumpTicketWebhookView` |
| `/support-ticket/get-next-ticket/` | GET | `GetNextTicketView` |
| `/support-ticket/get-not-connected-tickets/` | GET | `GetNotConnectedTicketsView` |
| `/support-ticket/get-wip-tickets/` | GET | `GetWIPTicketsView` |
| `/support-ticket/process-dumped-tickets/` | POST | `ProcessDumpedTicketsView` |
| `/support-ticket/save-and-continue/` | POST | `SaveAndContinueView` |
| `/support-ticket/take-break/` | POST | `TakeBreakView` |
| `/support-ticket/update-call-status/` | POST | `UpdateCallStatusView` |
| `/support-ticket/update/` | PATCH | `SupportTicketUpdateView` |

## `/jobs/` (8)

| Route | Methods | View |
|---|---|---|
| `/jobs/<int:job_id>/` | GET | `JobDetailView` |
| `/jobs/<int:job_id>/rerun/` | POST | `RerunJobView` |
| `/jobs/<int:job_id>/retry/` | POST | `RetryJobView` |
| `/jobs/bulk-retry/` | POST | `BulkRetryJobsView` |
| `/jobs/enqueue/` | POST | `EnqueueJobView` |
| `/jobs/failed/` | GET | `FailedJobsView` |
| `/jobs/status/` | GET | `JobQueueStatusView` |
| `/jobs/types/` | GET | `JobTypesView` |

## `/accounts/` (6)

| Route | Methods | View |
|---|---|---|
| `/accounts/delete-user/` | DELETE | `DeleteUserEverywhereView` |
| `/accounts/link-user-uid/` | POST | `LinkUserUidView` |
| `/accounts/setup-new-tenant/` | POST | `SetupNewTenantView` |
| `/accounts/users/assignees-by-role/` | GET | `AssigneesByRoleView` |
| `/accounts/users/create/` | POST | `TenantMembershipCreateView` |
| `/accounts/users/update/` | POST | `TenantMembershipUpdateView` |

## `/email/` (6)

| Route | Methods | View |
|---|---|---|
| `/email/send/` | POST | `SendEmailView` |
| `/email/zoho/callback/` | GET | `ZohoMailCallbackView` |
| `/email/zoho/connect/` | GET | `ZohoMailConnectView` |
| `/email/zoho/disconnect/` | POST | `ZohoMailDisconnectView` |
| `/email/zoho/status/` | GET | `ZohoMailStatusView` |
| `/email/zoho/sync-now/` | POST | `ZohoMailSyncNowView` |

## `/membership/` (6)

| Route | Methods | View |
|---|---|---|
| `/membership/billing/` | GET | `TenantMembershipBillingView` |
| `/membership/me/role/` | GET | `CurrentUserRoleView` |
| `/membership/roles/` | GET,POST | `RolesView` |
| `/membership/users/` | GET | `ListTenantUsersView` |
| `/membership/users/<int:membership_id>/spoof-token/` | POST | `SpoofTenantUserTokenView` |
| `/membership/users/hierarchy/` | PATCH | `UpdateUserHierarchyView` |

## `/chat/` (4)

| Route | Methods | View |
|---|---|---|
| `/chat/ask/` | POST | `ChatQuickView` |
| `/chat/conversations/` | GET,POST | `ConversationListCreateView` |
| `/chat/conversations/<uuid:conversation_id>/` | GET | `ConversationDetailView` |
| `/chat/conversations/<uuid:conversation_id>/messages/` | GET,POST | `ConversationMessagesView` |

## `/entity/` (4)

| Route | Methods | View |
|---|---|---|
| `/entity/` | GET,POST,PUT,PATCH,DELETE | `PrajaLeadsAPIView` |
| `/entity/backfill/` | GET,POST,PUT,PATCH,DELETE | `PrajaLeadEntityBackfillAPIView` |
| `/entity/support_ticket/` | PATCH | `SupportTicketEntityAPIView` |
| `/entity/sync/` | GET,POST,PUT,PATCH,DELETE | `PrajaLeadSyncAPIView` |

## `/pages/` (4)

| Route | Methods | View |
|---|---|---|
| `/pages/` | GET,POST | `PageListCreateView` |
| `/pages/<uuid:pk>/` | GET,PUT,PATCH,DELETE | `PageDetailView` |
| `/pages/custom-icons/` | GET,POST | `CustomIconListCreateView` |
| `/pages/custom-icons/<uuid:pk>/` | DELETE | `CustomIconDetailView` |

## `/pyro-jobs/` (4)

| Route | Methods | View |
|---|---|---|
| `/pyro-jobs/<int:job_id>/` | GET | `PyroJobDetailView` |
| `/pyro-jobs/<int:job_id>/rerun/` | POST | `PyroRerunJobView` |
| `/pyro-jobs/enqueue/` | POST | `PyroEnqueueJobView` |
| `/pyro-jobs/types/` | GET | `PyroJobTypesView` |

## `/auth/` (3)

| Route | Methods | View |
|---|---|---|
| `/auth/forgot-password/` | POST | `SupabasePasswordRecoverView` |
| `/auth/reset-password/confirm/` | POST | `PasswordResetConfirmView` |
| `/auth/test-supabase-login/` | POST | `SupabaseAuthCheckView` |

## `/cron-jobs/` (3)

| Route | Methods | View |
|---|---|---|
| `/cron-jobs/release-leads-after-12h/` | POST | `ReleaseLeadsAfter12hCronView` |
| `/cron-jobs/run-script/` | POST | `CopyScriptView` |
| `/cron-jobs/unassign-snoozed-leads/` | POST | `UnassignSnoozedLeadsCronView` |

## `/notifications/` (2)

| Route | Methods | View |
|---|---|---|
| `/notifications/` | GET | `InAppNotificationListView` |
| `/notifications/<int:pk>/read/` | POST | `InAppNotificationMarkReadView` |

## `/openai/` (2)

| Route | Methods | View |
|---|---|---|
| `/openai/analyze-file/` | POST | `OpenAIFileAnalysisView` |
| `/openai/analyze-text/` | POST | `OpenAITextAnalysisView` |

## `/whatsapp/` (2)

| Route | Methods | View |
|---|---|---|
| `/whatsapp/templates/` | GET,POST | `WhatsAppTemplateListCreateView` |
| `/whatsapp/templates/<int:pk>/` | GET,PUT,PATCH,DELETE | `WhatsAppTemplateDetailView` |
