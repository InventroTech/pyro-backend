# urls.py
from django.urls import path
from .views import (
    SupportTicketView,
    TicketClosureTimeAnalytics,
    DailyResolvedTicketsView,
    DailyPercentileResolutionTimeView,
    StackedBarResolvedUnresolvedView,
    AnalyticsQueryView,
    CSEAverageResolutionTimeView,
    SupportTicketListView

)
app_name = "analytics"

urlpatterns = [
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

    path("support-ticket/", SupportTicketListView.as_view(), name="support-ticket-list"),
]
