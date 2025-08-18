# urls.py
from django.urls import path
from .views import (
    TicketClosureTimeAnalytics,
    DailyResolvedTicketsView,
    DailyPercentileResolutionTimeView,
    StackedBarResolvedUnresolvedView,
    AnalyticsQueryView
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
]
