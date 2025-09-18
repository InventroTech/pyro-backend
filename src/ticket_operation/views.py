import logging
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from django.utils import timezone
from django.conf import settings
import os

from .models import SupportTicketDump

logger = logging.getLogger(__name__)

# Define the exact list of fields to accept from the payload (matching edge function)
ALLOWED_FIELDS = [
    'tenant_id',
    'ticket_date',
    'user_id',
    'name',
    'phone',
    'reason',
    'rm_name',
    'layout_status',
    'badge',
    'poster',
    'subscription_status',
    'atleast_paid_once',
    'source',
    'praja_dashboard_user_link',
    'display_pic_url'
]


@method_decorator(csrf_exempt, name='dispatch')
class DumpTicketWebhookView(APIView):
    """
    Django equivalent of the Supabase edge function dump-ticket-webhook.
    Does exactly what the edge function does - nothing more, nothing less.
    """
    permission_classes = [AllowAny]
    
    def options(self, request):
        """Handle CORS preflight requests"""
        response = Response('ok', status=status.HTTP_200_OK)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Headers'] = 'authorization, x-client-info, apikey, content-type, x-webhook-secret'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        return response
    
    def post(self, request):
        """Main webhook handler - exactly like edge function"""
        try:
            # 1. Validate webhook secret for security
            webhook_secret = request.headers.get('x-webhook-secret')
            stored_secret = os.environ.get('WEBHOOK_SECRET')
            
            if not webhook_secret or webhook_secret != stored_secret:
                logger.warning('Unauthorized webhook attempt.')
                return Response({
                    'error': 'Unauthorized: Invalid or missing webhook secret'
                }, status=status.HTTP_401_UNAUTHORIZED)
            
            # 2. Parse the incoming JSON payload
            payload = request.data
            if not payload or not isinstance(payload, dict):
                raise Exception("Invalid or empty JSON payload.")
            
            # 3. Create a clean data object with only the allowed fields
            cleaned_data = {}
            
            # The tenant_id is absolutely required.
            if not payload.get('tenant_id'):
                raise Exception("Missing required field: tenant_id")
            
            for field in ALLOWED_FIELDS:
                # If the field exists in the payload (and is not null/undefined), add it to our clean object.
                if payload.get(field) is not None:
                    cleaned_data[field] = payload[field]
            
            # Ensure ticket_date is present; default to now if not.
            if not cleaned_data.get('ticket_date'):
                cleaned_data['ticket_date'] = timezone.now()
            
            # Set default is_processed status for the cron job
            cleaned_data['is_processed'] = False
            
            # 4. Insert the cleaned data into the dump table
            dump_ticket = SupportTicketDump.objects.create(**cleaned_data)
            
            # 5. Success response
            return Response({
                'message': 'Ticket created successfully in dump table',
                'ticket_id': dump_ticket.id
            }, status=status.HTTP_200_OK)
            
        except Exception as error:
            logger.error(f'Critical error: {error}')
            return Response({
                'error': str(error)
            }, status=status.HTTP_400_BAD_REQUEST)

