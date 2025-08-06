from django.urls import path
from .views import TicketDumpWebhookView

urlpatterns = [
    path('webhook/', TicketDumpWebhookView.as_view(), name='ticket_dump_webhook'),
] 