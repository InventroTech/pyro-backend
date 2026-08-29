from django.urls import path
from .views import SendEmailView
from .zoho_views import (
    ZohoMailCallbackView,
    ZohoMailConnectView,
    ZohoMailDisconnectView,
    ZohoMailStatusView,
    ZohoMailSyncNowView,
)

app_name = 'email_protocol'

urlpatterns = [
    # Simple endpoint for external webhook calls
    # NOTE: Use send_email() function directly in code instead of this endpoint
    path('send/', SendEmailView.as_view(), name='send-email'),
    # Zoho Mail OAuth + shipment inbox sync
    path('zoho/connect/', ZohoMailConnectView.as_view(), name='zoho-mail-connect'),
    path('zoho/callback/', ZohoMailCallbackView.as_view(), name='zoho-mail-callback'),
    path('zoho/status/', ZohoMailStatusView.as_view(), name='zoho-mail-status'),
    path('zoho/disconnect/', ZohoMailDisconnectView.as_view(), name='zoho-mail-disconnect'),
    path('zoho/sync-now/', ZohoMailSyncNowView.as_view(), name='zoho-mail-sync-now'),
]
