from django.urls import path
from .views import DumpTicketWebhookView

app_name = 'ticket_operation'

urlpatterns = [
    path('dump-ticket-webhook/', DumpTicketWebhookView.as_view(), name='dump-ticket-webhook'),
]
