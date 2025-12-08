"""
Custom SMTP backend for Django email
"""
import ssl
import logging
import smtplib
from django.core.mail.backends.smtp import EmailBackend
from django.conf import settings

logger = logging.getLogger(__name__)


class CustomSMTPBackend(EmailBackend):
    """
    Custom SMTP backend that handles SSL certificate verification issues
    """
    
    def __init__(self, host=None, port=None, username=None, password=None,
                 use_tls=None, fail_silently=False, use_ssl=None, timeout=None,
                 ssl_keyfile=None, ssl_certfile=None, **kwargs):
        super().__init__(
            host=host, port=port, username=username, password=password,
            use_tls=use_tls, fail_silently=fail_silently, use_ssl=use_ssl,
            timeout=timeout, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile,
            **kwargs
        )
        
        # Get SSL verification setting from settings
        self.ssl_verify = getattr(settings, 'EMAIL_SSL_VERIFY', True)
        self.is_dev = getattr(settings, 'IS_DEV', False)
        logger.info(f"CustomSMTPBackend initialized - SSL Verify: {self.ssl_verify}, Is Dev: {self.is_dev}")
    
    def open(self):
        """
        Override to handle SSL context with optional verification
        """
        if self.connection:
            return False
        
        logger.info(f"Opening SMTP connection - Host: {self.host}, Port: {self.port}, Use TLS: {self.use_tls}, Use SSL: {self.use_ssl}")
        
        try:
            # Create SSL context
            if self.use_ssl or self.use_tls:
                if not self.ssl_verify and self.is_dev:
                    # Disable SSL verification for development
                    logger.warning("SSL certificate verification disabled for development")
                    context = ssl._create_unverified_context()
                else:
                    # Use default SSL context with verification
                    logger.info("Using SSL context with certificate verification")
                    context = ssl.create_default_context()
                
                if self.use_ssl:
                    # For SSL connection
                    self.connection = smtplib.SMTP_SSL(
                        self.host, self.port,
                        timeout=self.timeout,
                        keyfile=self.ssl_keyfile,
                        certfile=self.ssl_certfile,
                        context=context
                    )
                else:
                    # For TLS connection
                    self.connection = smtplib.SMTP(
                        self.host, self.port,
                        timeout=self.timeout
                    )
                    self.connection.starttls(context=context)
            else:
                # Non-secure connection
                self.connection = smtplib.SMTP(
                    self.host, self.port,
                    timeout=self.timeout
                )
            
            if self.username and self.password:
                self.connection.login(self.username, self.password)
            
            return True
            
        except Exception as e:
            if not self.fail_silently:
                raise
            logger.error(f"Failed to open SMTP connection: {e}")
            return False
