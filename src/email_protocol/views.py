"""
Email Protocol Views

NOTE: This email service is designed to be used INTERNALLY by calling the functions directly.
The endpoints here are just convenience wrappers for external webhook calls.

PRIMARY USAGE (Internal):
    from email_protocol.services import send_email, send_bulk_emails
    
    # Simple email
    success, msg = send_email(
        to_emails="user@example.com",
        subject="Welcome",
        message="Welcome message"
    )
    
    # Advanced email
    success, msg = send_email(
        to_emails=["user1@example.com", "user2@example.com"],
        subject="Update",
        message="Plain text",
        html_message="<h1>HTML</h1>",
        cc="manager@example.com",
        client_name="ClientABC"
    )
    
    # Bulk emails
    results = send_bulk_emails([
        {"to_emails": "user1@example.com", "subject": "Email 1", "message": "Message 1"},
        {"to_emails": "user2@example.com", "subject": "Email 2", "message": "Message 2"}
    ])
"""

import logging
import os
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny

from .serializers import SendEmailSerializer
from .services import send_email

logger = logging.getLogger(__name__)


@method_decorator(csrf_exempt, name='dispatch')
class SendEmailView(APIView):
    """
    Simple POST endpoint wrapper for internal email service
    
    NOTE: Use send_email() function directly in your code instead of this endpoint.
    This endpoint is only for external webhook calls.
    """
    permission_classes = [AllowAny]
    
    def options(self, request):
        """Handle CORS preflight requests"""
        response = Response('ok', status=status.HTTP_200_OK)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Headers'] = 'Content-Type, x-email-secret'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        return response
    
    def post(self, request):
        """
        Simple endpoint wrapper - use send_email() function directly in code instead
        
        Headers:
        - x-email-secret: Webhook secret for authentication
        
        Request body:
        {
            "email": "client@example.com",
            "subject": "Email Subject",
            "message": "Email message body",
            "html_message": "<h1>HTML</h1>"  // Optional
        }
        """
        try:
            # 1. Validate webhook secret for security
            email_secret = request.headers.get('x-email-secret')
            stored_secret = os.environ.get('EMAIL_WEBHOOK_SECRET')
            
            if not email_secret or email_secret != stored_secret:
                logger.warning('Unauthorized email request attempt.')
                return Response({
                    'error': 'Unauthorized: Invalid or missing x-email-secret header'
                }, status=status.HTTP_401_UNAUTHORIZED)
            
            # 2. Validate request data
            serializer = SendEmailSerializer(data=request.data)
            if not serializer.is_valid():
                logger.warning(f"Invalid email request data: {serializer.errors}")
                return Response({
                    'error': 'Invalid request data',
                    'details': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            validated_data = serializer.validated_data
            
            # 3. Call internal email function directly
            success, result_message = send_email(
                to_emails=validated_data['email'],
                subject=validated_data['subject'],
                message=validated_data['message'],
                html_message=validated_data.get('html_message')
            )
            
            if success:
                response = Response({
                    'success': True,
                    'message': result_message,
                    'recipient': validated_data['email'],
                    'subject': validated_data['subject']
                }, status=status.HTTP_200_OK)
            else:
                response = Response({
                    'success': False,
                    'error': result_message
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            response['Access-Control-Allow-Origin'] = '*'
            return response
                
        except Exception as error:
            logger.error(f"Error sending email: {error}", exc_info=True)
            response = Response({
                'success': False,
                'error': 'Internal server error',
                'details': str(error)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            response['Access-Control-Allow-Origin'] = '*'
            return response
