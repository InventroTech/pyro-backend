import logging
import json
from datetime import datetime
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

logger = logging.getLogger(__name__)

# Define the exact list of fields to accept from the payload
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
    'display_pic_url',
    'assigned_to',
    'resolution_status',
    'resolution_time',
    'cse_name',
    'cse_remarks',
    'call_status',
    'call_attempts',
    'completed_at',
    'snooze_until'
]


@method_decorator(csrf_exempt, name='dispatch')
class TicketDumpWebhookView(APIView):
    """
    Webhook endpoint to dump tickets to support_ticket_dump table.
    Similar to Supabase Edge Function but in Django.
    """
    permission_classes = [AllowAny]

    def options(self, request):
        """
        Handle CORS preflight requests
        """
        response = Response()
        response["Access-Control-Allow-Origin"] = "*"
        response["Access-Control-Allow-Headers"] = "authorization, x-client-info, apikey, content-type, x-webhook-secret"
        response["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return response

    def post(self, request):
        """
        Handle ticket dump webhook requests
        
        Expected request body:
        {
            "tenant_id": "uuid",
            "user_id": "12345",
            "name": "John Doe",
            "phone": "+1234567890",
            "reason": "Technical issue",
            ...
        }
        """
        try:
            # Set CORS headers
            response_headers = {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type, x-webhook-secret',
                'Access-Control-Allow-Methods': 'POST, OPTIONS',
                'Content-Type': 'application/json'
            }

            # 1. Validate webhook secret for security
            webhook_secret = request.headers.get('x-webhook-secret')
            stored_secret = getattr(settings, 'WEBHOOK_SECRET', None)
            
            if not webhook_secret or webhook_secret != stored_secret:
                logger.warning('Unauthorized webhook attempt.')
                return Response({
                    'error': 'Unauthorized: Invalid or missing webhook secret'
                }, status=status.HTTP_401_UNAUTHORIZED, headers=response_headers)

            # 2. Parse the incoming JSON payload
            try:
                payload = request.data
                if not payload or not isinstance(payload, dict):
                    raise ValueError("Invalid or empty JSON payload.")
            except Exception as e:
                logger.error(f"JSON parsing error: {e}")
                return Response({
                    'error': 'Invalid JSON payload'
                }, status=status.HTTP_400_BAD_REQUEST, headers=response_headers)

            # 3. Create a clean data object with only the allowed fields
            cleaned_data = {}
            
            # The tenant_id is absolutely required
            if not payload.get('tenant_id'):
                return Response({
                    'error': 'Missing required field: tenant_id'
                }, status=status.HTTP_400_BAD_REQUEST, headers=response_headers)

            # Process allowed fields
            for field in ALLOWED_FIELDS:
                if payload.get(field) is not None:
                    cleaned_data[field] = payload[field]

            # Ensure ticket_date is present; default to now if not
            if not cleaned_data.get('ticket_date'):
                cleaned_data['ticket_date'] = datetime.now().isoformat()

            # Set default is_processed status for the cron job
            cleaned_data['is_processed'] = False

            # 4. Insert the cleaned data into the source database dump table
            # Use source database connection instead of Django default
            import os
            from supabase import create_client, Client
            
            # Get source database credentials from environment
            source_url = os.getenv('SOURCE_SUPABASE_URL')
            source_key = os.getenv('SOURCE_SERVICE_ROLE_KEY')
            
            if not source_url or not source_key:
                logger.error('Source database credentials not found in environment variables')
                return Response({
                    'error': 'Database configuration error'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR, headers=response_headers)
            
            # Create Supabase client for source database
            supabase: Client = create_client(source_url, source_key)
            
            # Insert into source database
            try:
                result = supabase.table('support_ticket_dump').insert(cleaned_data).execute()
                ticket_id = result.data[0]['id'] if result.data else None
                
                if not ticket_id:
                    raise Exception("Failed to get ticket ID from source database")
                    
            except Exception as db_error:
                logger.error(f'Database insert error: {db_error}')
                return Response({
                    'error': f'Database error: {str(db_error)}'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR, headers=response_headers)

            # 5. Success response
            return Response({
                'message': 'Ticket created successfully in dump table',
                'ticket_id': ticket_id
            }, status=status.HTTP_200_OK, headers=response_headers)

        except Exception as error:
            logger.error(f'Critical error in ticket dump webhook: {error}')
            return Response({
                'error': str(error)
            }, status=status.HTTP_400_BAD_REQUEST, headers=response_headers)
