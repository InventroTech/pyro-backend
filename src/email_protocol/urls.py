from django.urls import path
from .views import SendEmailView

app_name = 'email_protocol'

urlpatterns = [
    # Simple endpoint for external webhook calls
    # NOTE: Use send_email() function directly in code instead of this endpoint
    path('send/', SendEmailView.as_view(), name='send-email'),
]

