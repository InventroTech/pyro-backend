from django.urls import path
from .views import DumpTicketWebhookView, SaveAndContinueView, GetNextTicketView, SupportTicketUpdateView, TakeBreakView, UpdateCallStatusView, GetWIPTicketsView

app_name = 'support_ticket'

urlpatterns = [
    path('dump-ticket-webhook/', DumpTicketWebhookView.as_view(), name='dump-ticket-webhook'),
    path('save-and-continue/', SaveAndContinueView.as_view(), name='save-and-continue'),
    path('get-next-ticket/', GetNextTicketView.as_view(), name='get-next-ticket'),
    path('get-wip-tickets/', GetWIPTicketsView.as_view(), name='get-wip-tickets'),
    path("update-call-status/", UpdateCallStatusView.as_view(), name="update-call-status"),
    path('update/', SupportTicketUpdateView.as_view(), name='update-ticket'),
    path('take-break/', TakeBreakView.as_view(), name='take-break'),
]
