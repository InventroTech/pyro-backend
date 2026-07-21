# urls.py
from django.urls import path
from .views import (
    SupportTicketView,
    TicketClosureTimeAnalytics,
    DailyResolvedTicketsView,
    DailyPercentileResolutionTimeView,
    DailyAverageResolutionTimeView,
    StackedBarResolvedUnresolvedView,
    AnalyticsQueryView,
    CSEAverageResolutionTimeView,
    SLATimeView,
    SupportTicketListView,
    SupportTicketFilterOptionsView,
    GetTicketStatusView,
    GetCseStatsView,
    TeamOverviewView,
    TeamMembersView,
    TeamEventsView,
    TeamTimeSeriesView,
    UnassignedLeadsBreakdownView,
    CseFilterOptionsView,
    CseSupportTicketBreakdownView,
    CseOverviewView,
    CseMembersView,
    CseTimeSeriesView,
    AnalyticsAvailableTypesView,
    RmFilterOptionsView,
    RmOverviewView,
    RmMembersView,
    RmTimeSeriesView,
    AnalyticsBoardView,
    AnalyticsBoardDetailView,
)
app_name = "analytics"

urlpatterns = [
    path(
        'get-cse-stats/',
        GetCseStatsView.as_view(),
        name="get-cse-stats"
    ),
    path(
        'ticket-close-time/',
        TicketClosureTimeAnalytics.as_view(),
        name="ticket-close-time"
    ),
    path(
        'tickets/resolved/daily/',
        DailyResolvedTicketsView.as_view(),
        name="daily-resolved-tickets"
    ),
    path(
        'tickets/daily-percentile/',
        DailyPercentileResolutionTimeView.as_view(),
        name='daily-resolution-percentile'
    ),
    path(
        'tickets/daily-average/',
        DailyAverageResolutionTimeView.as_view(),
        name='daily-resolution-average'
    ),
    path(
        'tickets/stacked-bar-daily/',
        StackedBarResolvedUnresolvedView.as_view(),
        name='stacked-bar'
    ),
    path(
        'query/',
        AnalyticsQueryView.as_view(),
        name='analytics-query'
    ),
    path('support-ticket-count/', SupportTicketView.as_view(), name='support-ticket-count'),
    path(
        'cse-average-resolution-time/',
        CSEAverageResolutionTimeView.as_view(),
        name='cse-average-resolution-time'
    ),
    path(
        'sla-time/',
        SLATimeView.as_view(),
        name='sla-time'
    ),

    path("support-ticket/", SupportTicketListView.as_view(), name="support-ticket-list"),
    path("support-tickets/filter-options/", SupportTicketFilterOptionsView.as_view(),
         name="support-ticket-filter-options"),
    path("get-ticket-status/", GetTicketStatusView.as_view(), name="get-ticket-status"),
    
    # Team metrics endpoints
    path("team/overview/", TeamOverviewView.as_view(), name="team-overview"),
    path("team/members/", TeamMembersView.as_view(), name="team-members"),
    path("team/events/", TeamEventsView.as_view(), name="team-events"),
    path("team/time-series/", TeamTimeSeriesView.as_view(), name="team-time-series"),
    path("team/unassigned-leads/", UnassignedLeadsBreakdownView.as_view(), name="team-unassigned-leads"),

    # CSE support-ticket analytics
    path(
        "available-types/",
        AnalyticsAvailableTypesView.as_view(),
        name="analytics-available-types",
    ),
    path("cse/filter-options/", CseFilterOptionsView.as_view(), name="cse-filter-options"),
    path(
        "cse/support-ticket-breakdown/",
        CseSupportTicketBreakdownView.as_view(),
        name="cse-support-ticket-breakdown",
    ),
    path("cse/overview/", CseOverviewView.as_view(), name="cse-overview"),
    path("cse/members/", CseMembersView.as_view(), name="cse-members"),
    path("cse/time-series/", CseTimeSeriesView.as_view(), name="cse-time-series"),

    # RM lead analytics (EventLog)
    path("rm/filter-options/", RmFilterOptionsView.as_view(), name="rm-filter-options"),
    path("rm/overview/", RmOverviewView.as_view(), name="rm-overview"),
    path("rm/members/", RmMembersView.as_view(), name="rm-members"),
    path("rm/time-series/", RmTimeSeriesView.as_view(), name="rm-time-series"),

    # Saved analytics boards (one row per board, generic across analytics types)
    path("board/", AnalyticsBoardView.as_view(), name="analytics-board"),
    path(
        "board/<str:report_id>/",
        AnalyticsBoardDetailView.as_view(),
        name="analytics-board-detail",
    ),
]
