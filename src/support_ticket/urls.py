from django.urls import path
from .views import DumpTicketWebhookView, SaveAndContinueView

app_name = 'support_ticket'

urlpatterns = [
    path('dump-ticket-webhook/', DumpTicketWebhookView.as_view(), name='dump-ticket-webhook'),
    path('save-and-continue/', SaveAndContinueView.as_view(), name='save-and-continue'),
]
