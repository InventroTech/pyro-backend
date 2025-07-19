# urls.py
from django.urls import path
from .views import SupabaseAuthCheckView, TicketClosureTimeAnalytics, DailyResolvedTicketsView, DailyPercentileResolutionTimeView ,StackedBarResolvedUnresolvedView
urlpatterns = [
    path('test-supabase-login/', SupabaseAuthCheckView.as_view(), name = "authcheck"),
    path('ticket-close-time/', TicketClosureTimeAnalytics.as_view(), name = "ticket-close-time"),
    path('tickets/resolved/daily/', DailyResolvedTicketsView.as_view(), name="daily-resolved-tickets"),
    path('tickets/daily-percentile/', DailyPercentileResolutionTimeView.as_view()),
    path('tickets/stacked-bar-daily/', StackedBarResolvedUnresolvedView.as_view(), name = 'stacked-bar'),
]
