import logging
import json
import base64
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from django.utils import timezone
from django.db import transaction
from config.supabase_auth import SupabaseJWTAuthentication
import os

from .models import SupportTicketDump
from .models import SupportTicket
from .serializers import SaveAndContinueSerializer, SaveAndContinueResponseSerializer, SupportTicketResponseSerializer, GetNextTicketResponseSerializer
from .services import MixpanelService, TicketTimeService
from authz.permissions import IsTenantAuthenticated
from accounts.models import LegacyUser
from datetime import timedelta
from .serializers import (
    UpdateCallStatusRequestSerializer,
)
from .serializers import SupportTicketSerializer
from .utils import send_to_mixpanel

logger = logging.getLogger(__name__)

# Define the exact list of fields to accept from the payload (matching edge function)
ALLOWED_FIELDS = [
    'tenant_id',
    'ticket_date',
    'user_id',
    'name',
    'phone',
    'reason',
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


class SaveAndContinueView(APIView):
    """
    Django equivalent of the Supabase save-and-continue edge function.
    Updates support tickets with resolution status and sends Mixpanel events.
    """
    authentication_classes = [SupabaseJWTAuthentication]
    
    def options(self, request):
        """Handle CORS preflight requests"""
        response = Response('ok', status=status.HTTP_200_OK)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        return response
    
    def post(self, request):
        """Main save-and-continue handler - exactly like edge function"""
        try:
            # Get JWT claims from the authentication middleware
            if not hasattr(request, 'jwt_claims'):
                return Response({
                    'error': 'Missing or invalid auth header'
                }, status=status.HTTP_401_UNAUTHORIZED)
            
            jwt_claims = request.jwt_claims
            user_id = jwt_claims.get('sub')
            user_email = jwt_claims.get('email')
            
            logger.info(f'CSE processing request - CSE ID: {user_id}, CSE Email: {user_email}')
            
            if not user_id:
                return Response({
                    'error': 'No user id in JWT'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate request data
            serializer = SaveAndContinueSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    'error': 'Invalid request data',
                    'details': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            validated_data = serializer.validated_data
            ticket_id = validated_data['ticketId']
            resolution_status = validated_data.get('resolutionStatus')
            call_status = validated_data.get('callStatus')
            cse_remarks = validated_data.get('cseRemarks')
            resolution_time = validated_data.get('resolutionTime')
            other_reasons = validated_data.get('otherReasons', [])
            
            logger.info(f'Processing ticket: {ticket_id} with resolution status: {resolution_status}')
            
            # Get current ticket
            try:
                current_ticket = SupportTicket.objects.get(id=ticket_id)
            except SupportTicket.DoesNotExist:
                logger.error(f'Ticket not found: {ticket_id}')
                return Response({
                    'error': 'Ticket not found'
                }, status=status.HTTP_404_NOT_FOUND)
            
            current_time = timezone.now()
            
            # Calculate accumulated resolution time
            time_service = TicketTimeService()
            final_resolution_time = "0:00"
            
            if resolution_time:
                logger.info(f'Processing resolutionTime: {resolution_time}')
                logger.info(f'Existing time: {current_ticket.resolution_time}')
                
                if ":" in resolution_time:
                    # Add the new time to existing time
                    final_resolution_time = time_service.add_time_strings(
                        current_ticket.resolution_time or "0:00", 
                        resolution_time
                    )
                    logger.info(f'Added times, result: {final_resolution_time}')
                else:
                    # If no valid resolution time provided, keep the existing one
                    final_resolution_time = current_ticket.resolution_time or "0:00"
                    logger.info(f'No valid time format, keeping existing: {final_resolution_time}')
            else:
                # If no new resolution time provided, keep the existing one
                final_resolution_time = current_ticket.resolution_time or "0:00"
                logger.info(f'No resolution time provided, keeping existing: {final_resolution_time}')
            
            # Always save cse_name for all resolution statuses
            cse_name = user_email
            
            # Update the current ticket
            logger.info(f'Updating ticket with resolution: {resolution_status}, call status: {call_status}')
            
            # Apply memory constraint: don't update assigned_to and tenant_id in staging
            update_data = {
                'resolution_status': resolution_status,
                'cse_remarks': cse_remarks,
                'cse_name': cse_name,
                'call_status': call_status,
                'resolution_time': final_resolution_time,
                'call_attempts': (current_ticket.call_attempts or 0) + 1,
                'completed_at': current_time,
                'other_reasons': other_reasons
            }
            
            # Convert user_id string to UUID if needed and update assigned_to
            try:
                from uuid import UUID
                if isinstance(user_id, str):
                    update_data['assigned_to'] = UUID(user_id)
                else:
                    update_data['assigned_to'] = user_id
            except ValueError:
                logger.warning(f'Invalid UUID format for user_id: {user_id}, skipping assigned_to update')
                # Don't update assigned_to if user_id is not a valid UUID
            
            for field, value in update_data.items():
                setattr(current_ticket, field, value)
            
            current_ticket.save()
            
            # Send Mixpanel events based on resolution status
            mixpanel_service = MixpanelService()
            mixpanel_event_name = ''
            
            if resolution_status == 'Resolved':
                mixpanel_event_name = 'pyro_resolve'
            elif resolution_status == "Can't Resolve":
                mixpanel_event_name = 'pyro_cannot_resolve'
            elif resolution_status == 'WIP':
                mixpanel_event_name = 'pyro_call_later'
            
            # Send Mixpanel events - REQUIRED, must work
            if mixpanel_event_name and current_ticket.user_id:
                logger.info(f'Sending REQUIRED Mixpanel events for user_id: {current_ticket.user_id}, event: {mixpanel_event_name}')
                
                mixpanel_properties = {
                    'support_ticket_id': ticket_id,
                    'remarks': cse_remarks or '',
                    'cse_email_id': user_email,
                    'reasons': other_reasons or []
                }
                
                jwt_token = getattr(request, 'token', None)
                
                # Send Mixpanel events - exactly like working Edge function
                mixpanel_service.send_to_mixpanel_sync(
                    current_ticket.user_id, 
                    'pyro_connected', 
                    mixpanel_properties
                )
                
                mixpanel_service.send_to_mixpanel_sync(
                    current_ticket.user_id, 
                    mixpanel_event_name, 
                    mixpanel_properties
                )
            elif mixpanel_event_name and not current_ticket.user_id:
                logger.info(f'No customer user_id found in ticket, skipping Mixpanel event for: {mixpanel_event_name}')
            else:
                logger.info(f'No Mixpanel event configured for resolution status: {resolution_status}')
            
            # Prepare response
            logger.info(f'Ticket updated successfully: {ticket_id}, resolution: {resolution_status}')
            
            # Serialize the updated ticket
            ticket_serializer = SupportTicketResponseSerializer(current_ticket)
            
            response_data = {
                'success': True,
                'message': 'Ticket updated successfully',
                'updatedTicket': ticket_serializer.data,
                'userId': user_id,
                'userEmail': user_email,
                'totalResolutionTime': final_resolution_time or "0:00"
            }
            
            response = Response(response_data, status=status.HTTP_200_OK)
            response['Access-Control-Allow-Origin'] = '*'
            return response
            
        except Exception as error:
            logger.error(f'Error in save-and-continue function: {error}')
            response = Response({
                'error': 'Internal server error'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            response['Access-Control-Allow-Origin'] = '*'
            return response


class GetNextTicketView(APIView):
    """
    Django equivalent of the Supabase get-next-ticket edge function.
    Simple LIFO logic: newest unassigned ticket first, then snoozed tickets.
    """
    authentication_classes = [SupabaseJWTAuthentication]
    
    def options(self, request):
        """Handle CORS preflight requests"""
        response = Response('ok', status=status.HTTP_200_OK)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        response['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        return response
    
    def get(self, request):
        """Main get-next-ticket handler - exactly like edge function"""
        try:
            # Get JWT claims from the authentication middleware
            if not hasattr(request, 'jwt_claims'):
                return Response({
                    'error': 'Missing or invalid auth header'
                }, status=status.HTTP_401_UNAUTHORIZED)
            
            jwt_claims = request.jwt_claims
            user_id = jwt_claims.get('sub')
            user_email = jwt_claims.get('email')
            
            if not user_id:
                return Response({
                    'error': 'No user id in JWT'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Combine all the logs into one log line

            logger.info(f"=== TICKET ORDERING VALIDATION ===")
            logger.info(f"Current time: {timezone.now()}")
            logger.info(f"User ID: {user_id}")
            logger.info(f"User Email: {user_email}")
            
            # Get the next ticket
            with transaction.atomic():
                next_ticket = self._get_and_assign_ticket(user_id, user_email)
                
            
            # If no tickets available, return empty object
            if not next_ticket:
                response = Response({}, status=status.HTTP_200_OK)
                response['Access-Control-Allow-Origin'] = '*'
                return response
            
            # Return the ticket
            response_data = {'ticket': next_ticket}
            serializer = GetNextTicketResponseSerializer(response_data)
            
            response = Response(serializer.data, status=status.HTTP_200_OK)
            response['Access-Control-Allow-Origin'] = '*'
            return response
            
        except Exception as error:
            logger.error(f'Error in get-next-ticket function: {error}')
            response = Response({
                'error': 'Internal server error'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            response['Access-Control-Allow-Origin'] = '*'
            return response
    
    def _get_and_assign_ticket(self, user_id, user_email):
        """
        Simplified logic to get and assign a ticket to the user.
        1. First get the newest unassigned ticket (LIFO - Last In, First Out)
        2. Then check for snoozed tickets for this user
        """
        current_time = timezone.now()
        
        
        unassigned_ticket = SupportTicket.objects.select_for_update(
            skip_locked=True,
            of=("self",)
        ).filter(
            assigned_to__isnull=True,
            resolution_status__isnull=True
        ).order_by('-created_at')[:1].first()
        
        if unassigned_ticket:
            logger.info(f"UNASSIGNED TICKET FOUND: ID {unassigned_ticket.id}")
            logger.info(f"Ticket created at: {unassigned_ticket.created_at}")
            
            # Try to assign the ticket to the user
            unassigned_ticket.assigned_to = user_id
            unassigned_ticket.cse_name = user_email
            unassigned_ticket.save()
            return unassigned_ticket
        
        # 2. Look for snoozed tickets for this user as fallback
        logger.info(f"5 - Looking for snoozed tickets for user: {user_id}")
        logger.info(f"5 - Current time: {current_time}")
        
        snoozed_tickets = SupportTicket.objects.filter(
            assigned_to=user_id,
            resolution_status__isnull=True,
            snooze_until__isnull=False,
            snooze_until__lte=current_time
        ).order_by('-snooze_until')[:1]
        
        logger.info(f"6 - Found {len(snoozed_tickets)} snoozed tickets")
        
        if snoozed_tickets:
            ticket = snoozed_tickets[0]
            logger.info("7 - SNOOZED TICKET FOUND")
            logger.info(f"Snoozed ticket ID: {ticket.id}")
            logger.info(f"Snooze until: {ticket.snooze_until}")
            logger.info(f"Created at: {ticket.created_at}")
            return ticket
        
        # No tickets available
        logger.info("8 - No tickets available")
        return None

class UpdateCallStatusView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        try:
            ser = UpdateCallStatusRequestSerializer(data=request.data)
            if not ser.is_valid():
                return Response({"error": ser.errors}, status=status.HTTP_400_BAD_REQUEST)

            payload = ser.validated_data
            ticket_id = payload["ticketId"]
            call_status = payload["callStatus"]

            resolution_status = payload.get("resolutionStatus")
            cse_remarks = payload.get("cseRemarks")
            resolution_time = payload.get("resolutionTime")
            other_reasons = payload.get("otherReasons")
            assigned_to = payload.get("assignedTo")

            with transaction.atomic():
                ticket = SupportTicket.objects.select_for_update().filter(
                    id=ticket_id, tenant_id=request.tenant
                ).first()
                if not ticket:
                    return Response({"error": "Ticket not found"}, status=status.HTTP_404_NOT_FOUND)

                now = timezone.now()
                snooze_until = None

                # Snooze / Close logic
                if call_status == "Not Connected":
                    is_first = not ticket.call_attempts
                    if is_first:
                        snooze_until = now + timedelta(hours=1)
                        resolution_status = "Snoozed"
                    else:
                        snooze_until = now + timedelta(days=365 * 10)
                        resolution_status = resolution_status or "Closed"

                # Resolve assignment
                final_assigned_to = assigned_to or getattr(request.user, "supabase_uid", None)
                legacy_user = LegacyUser.objects.filter(uid=request.user.supabase_uid).first()
                final_cse_name = getattr(legacy_user, "name", "")

                # Build update fields
                update_fields = {
                    "call_status": call_status,
                    "call_attempts": (ticket.call_attempts or 0) + 1,
                    "completed_at": now,
                    "snooze_until": snooze_until,
                    "assigned_to_id": str(final_assigned_to) if final_assigned_to else None,
                    "cse_name": final_cse_name,
                }
                if resolution_status is not None:
                    update_fields["resolution_status"] = resolution_status
                if cse_remarks is not None:
                    update_fields["cse_remarks"] = cse_remarks
                if resolution_time is not None:
                    update_fields["resolution_time"] = resolution_time
                if other_reasons is not None:
                    update_fields["other_reasons"] = other_reasons

                for field, value in update_fields.items():
                    setattr(ticket, field, value)

                ticket.save()

            # Send Mixpanel event (outside transaction)
            if call_status == "Not Connected" and ticket.user_id:
                send_to_mixpanel(
                    ticket.user_id,
                    "pyro_not_connected",
                    {
                        "support_ticket_id": ticket_id,
                        "remarks": cse_remarks or "",
                        "cse_email_id": getattr(request.user, "email", None),
                        "reasons": other_reasons or [],
                    },
                )

            return Response(SupportTicketSerializer(ticket).data, status=200)

        except Exception as e:
            # log.exception("Error updating call status")
            return Response({"error": "Internal server error"}, status=500)
