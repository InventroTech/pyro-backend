from django.urls import path
from .views import DumpTicketWebhookView, SaveAndContinueView, GetNextTicketView, SupportTicketUpdateView

app_name = 'support_ticket'

urlpatterns = [
    path('dump-ticket-webhook/', DumpTicketWebhookView.as_view(), name='dump-ticket-webhook'),
    path('save-and-continue/', SaveAndContinueView.as_view(), name='save-and-continue'),
    path('get-next-ticket/', GetNextTicketView.as_view(), name='get-next-ticket'),
    path('update/', SupportTicketUpdateView.as_view(), name='update-ticket'),
]
