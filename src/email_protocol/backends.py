"""
Custom SMTP backend for Django email
"""
from django.core.mail.backends.smtp import EmailBackend


class CustomSMTPBackend(EmailBackend):
    """
    Custom SMTP backend that extends Django's default SMTP backend.
    This can be customized further based on specific requirements.
    """
    pass

