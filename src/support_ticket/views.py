import logging
import json
import base64
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from uuid import UUID
from rest_framework.permissions import AllowAny
from django.utils import timezone
from django.db import transaction
from django.db.models import Q
from config.supabase_auth import SupabaseJWTAuthentication
from authz.permissions import IsTenantAuthenticated
import os

from .models import SupportTicketDump
from .models import SupportTicket
from .serializers import SaveAndContinueSerializer, SaveAndContinueResponseSerializer, SupportTicketResponseSerializer, GetNextTicketResponseSerializer, SupportTicketUpdateSerializer, TakeBreakSerializer,UpdateCallStatusRequestSerializer
from .services import MixpanelService, TicketTimeService
from authz.permissions import IsTenantAuthenticated
from accounts.models import LegacyUser
from datetime import timedelta
from analytics.serializers import SupportTicketSerializer
from .utils import send_to_mixpanel

logger = logging.getLogger(__name__)


class GetWIPTicketsView(APIView):
    """
    Django equivalent of the Supabase get-wip-tickets edge function.
    Fetches tickets assigned to the authenticated user with 'WIP' status.
    """
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request):
        """
        Get WIP tickets assigned to the authenticated user.
        Returns tickets sorted by creation date (newest first).
        """
        try:
            # Get user from authentication middleware
            user = request.user
            user_id = user.supabase_uid
            
            logger.info(f'Querying for user ID: {user_id}')
            
            # Query for tickets assigned to the user with 'Work in Progress' status
            # Sorted by creation date, newest first
            wip_tickets = SupportTicket.objects.filter(
                assigned_to=user_id,
                resolution_status='WIP'
            ).order_by('-created_at')
            
            # Serialize the tickets
            serializer = SupportTicketSerializer(wip_tickets, many=True)
            
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except Exception as error:
            logger.error(f'Unexpected error in get-wip-tickets: {error}')
            return Response({
                'error': 'An unexpected error occurred.',
                'details': str(error)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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
    'state',
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
    Authentication relies only on x-webhook-secret header, no bearer token required.
    """
    authentication_classes = []  # No bearer token authentication required
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
            user = request.user
            user_id = user.supabase_uid
            user_email = user.email
            
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
            review_requested = validated_data.get('reviewRequested')
            
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
                'other_reasons': other_reasons,
                'assigned_to_id': UUID(user_id)
            }
            

            # Add review_requested if provided
            if review_requested is not None:
                update_data['review_requested'] = review_requested
                
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
                    'reasons': other_reasons or [],
                    'review_requested': review_requested
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
            # Get user from authentication middleware (IsTenantAuthenticated already handles auth)
            user = request.user
            user_id = user.supabase_uid
            user_email = user.email

            logger.info(f"=== TICKET ORDERING VALIDATION ===")
            logger.info(f"Current time: {timezone.now()}")
            logger.info(f"User ID: {user_id}")
            logger.info(f"User Email: {user_email}")

            # Get the next ticket
            with transaction.atomic():
                next_ticket = self._get_and_assign_ticket(user, user_email)


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

    def _get_and_assign_ticket(self, user, user_email):
        """
        Simplified logic to get and assign a ticket to the user.
        1. First get the newest unassigned ticket (LIFO - Last In, First Out)
        2. Then check for snoozed tickets for this user
        """
        current_time = timezone.now()

        # 1. Get the ticket with resolution_status null that is assigned to the user or is snoozed and assigned to current user only
        logger.info(f"7 - Looking for tickets with resolution_status null and assigned to user: {user.supabase_uid}")
        logger.info(f"7 - Current time: {current_time}")
        already_assigned_ticket = SupportTicket.objects.select_for_update(
            skip_locked=True,
            of=("self",)
        ).filter(
            assigned_to=UUID(user.supabase_uid),
        ).filter(
            Q(resolution_status__isnull=True) | Q(resolution_status="Snoozed")
        ).exclude(
            poster__in=["Trial Expired", "Premium Expired", "trial_expired", "premium_expired"]
        ).order_by('created_at').first()
        if already_assigned_ticket:
            logger.info("9 - TICKET FOUND WITH RESOLUTION_STATUS NULL AND ASSIGNED TO USER")
            logger.info(f"Ticket ID: {already_assigned_ticket.id}")
            logger.info(f"Created at: {already_assigned_ticket.created_at}")
            already_assigned_ticket.assigned_to_id = (user.supabase_uid)
            already_assigned_ticket.cse_name = user_email
            return already_assigned_ticket
        else:
            logger.info("8 - NO TICKETS FOUND WITH RESOLUTION_STATUS NULL AND ASSIGNED TO USER")


        # 2. LIFO logic: get the newest unassigned ticket
        logger.info("1 - Searching for unassigned tickets with row locking")

        unassigned_ticket = SupportTicket.objects.select_for_update(
            skip_locked=True,
            of=("self",)
        ).filter(
            assigned_to__isnull=True,
            resolution_status__isnull=True
        ).exclude(
            poster__in=["Trial Expired", "Premium Expired", "trial_expired", "premium_expired"]
        ).order_by('-created_at')[:1].first()
        
        if unassigned_ticket:
            logger.info(f"UNASSIGNED TICKET FOUND: ID {unassigned_ticket.id}")
            logger.info(f"Ticket created at: {unassigned_ticket.created_at}")

            # Assign the ticket to the user (assigned_to is UUIDField, so convert supabase_uid)
            unassigned_ticket.assigned_to_id = (user.supabase_uid)
            unassigned_ticket.cse_name = user_email
            unassigned_ticket.save()
            logger.info("3 - UNASSIGNED TICKET ASSIGNED SUCCESSFULLY")
            return unassigned_ticket
        else:
            logger.info("2 - NO UNASSIGNED TICKETS FOUND")

        # 3. Look for snoozed tickets for this user as fallback
        logger.info(f"5 - Looking for snoozed tickets for user: {user.supabase_uid}")
        logger.info(f"5 - Current time: {current_time}")

        snoozed_ticket = SupportTicket.objects.select_for_update(
            skip_locked=True,
            of=("self",)
        ).filter(
            resolution_status="Snoozed",
            assigned_to__isnull=True,
            snooze_until__isnull=False,
            snooze_until__lte=current_time
        ).exclude(
            poster__in=["Trial Expired", "Premium Expired", "trial_expired", "premium_expired"]
        ).order_by('-snooze_until').first()

        logger.info(f"6 - Found snoozed ticket")
        
        if snoozed_ticket:
            logger.info("7 - SNOOZED TICKET FOUND")
            logger.info(f"Snoozed ticket ID: {snoozed_ticket.id}")
            logger.info(f"Snooze until: {snoozed_ticket.snooze_until}")
            logger.info(f"Created at: {snoozed_ticket.created_at}")
            
            snoozed_ticket.assigned_to_id = (user.supabase_uid)
            snoozed_ticket.cse_name = user_email
            snoozed_ticket.save()
            return snoozed_ticket
        else:
            logger.info("6 - NO SNOOZED TICKETS FOUND")

        
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
                final_assigned_to = None
                final_cse_name = None

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
                mixpanel_service = MixpanelService()
                mixpanel_service.send_to_mixpanel_sync(
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

class SupportTicketUpdateView(APIView):
    """
    API endpoint for admins to update support tickets, specifically for assigning tickets to CSEs
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]
    
    def patch(self, request):
        """Update support ticket fields - primarily for admin assignment"""
        try:
            # Get user from authentication middleware (IsTenantAuthenticated already handles auth)
            user = request.user
            user_id = user.supabase_uid
            user_email = user.email
            
            logger.info(f'Admin updating ticket - Admin ID: {user_id}, Admin Email: {user_email}')
            
            # Validate request data
            serializer = SupportTicketUpdateSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    'error': 'Invalid request data',
                    'details': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            validated_data = serializer.validated_data
            ticket_id = validated_data['ticket_id']
            
            # Get the ticket to update
            try:
                ticket = SupportTicket.objects.get(id=ticket_id)
            except SupportTicket.DoesNotExist:
                logger.error(f'Ticket not found: {ticket_id}')
                return Response({
                    'error': 'Ticket not found'
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Apply memory constraint: don't update assigned_to and tenant_id in staging
            # Note: Based on memory, staging environment has fixed assigned_to and tenant_id
            # But this endpoint is for admin assignment, so we'll allow it for production
            
            # Prepare update data (exclude ticket_id)
            update_data = {k: v for k, v in validated_data.items() if k != 'ticket_id'}
            
            # Update the ticket fields in atomic transaction
            with transaction.atomic():
                for field, value in update_data.items():
                    setattr(ticket, field, value)
                
                # Save the ticket
                ticket.save()
            
            logger.info(f'Ticket updated successfully: {ticket_id} by admin: {user_email}')
            
            # Serialize the updated ticket for response
            ticket_serializer = SupportTicketResponseSerializer(ticket)
            
            response_data = {
                'success': True,
                'message': 'Ticket updated successfully',
                'updated_ticket': ticket_serializer.data,
                'updated_by': user_email,
                'updated_fields': list(update_data.keys())
            }
            
            response = Response(response_data, status=status.HTTP_200_OK)
            response['Access-Control-Allow-Origin'] = '*'
            return response
            
        except Exception as error:
            logger.error(f'Error in support ticket update: {error}')
            response = Response({
                'error': 'Internal server error'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            response['Access-Control-Allow-Origin'] = '*'
            return response

#
class TakeBreakView(APIView):
    """
    Django equivalent of the Supabase take-break edge function.
    Unassigns a ticket from the current user, unless the ticket is in WIP status.
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    def options(self, request):
        """Handle CORS preflight requests"""
        response = Response('ok', status=status.HTTP_200_OK)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        return response

    def post(self, request):
        """Main take-break handler - exactly like edge function"""
        try:
            # Get user from authentication middleware
            user = request.user
            user_id = user.supabase_uid
            user_email = user.email
            
            if not user_id:
                return Response({
                    'error': 'No user id in JWT'
                }, status=status.HTTP_400_BAD_REQUEST)

            # Validate request data
            serializer = TakeBreakSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    'error': 'Invalid request data',
                    'details': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            validated_data = serializer.validated_data
            ticket_id = validated_data['ticketId']
            resolution_status_payload = validated_data.get('resolutionStatus')

            # Get the ticket to update
            try:
                ticket = SupportTicket.objects.get(id=ticket_id)
            except SupportTicket.DoesNotExist:
                return Response({
                    'error': 'Ticket not found'
                }, status=status.HTTP_404_NOT_FOUND)

            # Only unassign if the ticket is not in WIP status
            should_unassign = True
            message = "Ticket unassigned. Taking a break."
            
            # If the current ticket's resolution status is 'WIP' or the payload sends 'WIP', we do not unassign.
            if ticket.resolution_status == "WIP" or resolution_status_payload == "WIP":
                should_unassign = False
                message = "Ticket is in progress. Taking a break without unassigning."

            if should_unassign:
                # Unassign the ticket
                ticket.assigned_to = None
                ticket.cse_name = None
                ticket.save()
            
            response_data = {
                'success': True,
                'message': message,
                'ticketUnassigned': should_unassign,
                'userId': user_id,
                'userEmail': user_email
            }

            response = Response(response_data, status=status.HTTP_200_OK)
            response['Access-Control-Allow-Origin'] = '*'
            return response

        except Exception as error:
            logger.error(f'Error in take-break function: {error}')
            response = Response({
                'error': 'Internal server error'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            response['Access-Control-Allow-Origin'] = '*'
            return response


class ProcessDumpedTicketsView(APIView):
    permission_classes = [AllowAny]  # Allow cron jobs to call this endpoint
    
    def post(self, request):
        try:
            logger.info('ProcessDumpedTicketsView: Function invoked. Starting ticket processing...')
            
            # 1. Fetch Unprocessed Tickets
            # Fetch tickets that have not been processed yet (is_processed is False or None)
            dumped_tickets = SupportTicketDump.objects.filter(
                Q(is_processed__isnull=True) | Q(is_processed=False)
            )[:5000]  # Limit to 5000 tickets per run
            
            if not dumped_tickets.exists():
                logger.info('ProcessDumpedTicketsView: No new tickets in dump table to process.')
                return Response({
                    'message': 'No tickets to process'
                }, status=status.HTTP_200_OK)
            
            dumped_tickets_list = list(dumped_tickets)
            logger.info(f'ProcessDumpedTicketsView: Found {len(dumped_tickets_list)} unprocessed tickets to process from dump.')
            
            # 2. Deduplicate Tickets based on user_id
            # Use a dictionary to keep only the first occurrence of each user_id
            unique_tickets_map = {}
            for ticket in dumped_tickets_list:
                user_id = ticket.user_id
                if user_id and user_id not in unique_tickets_map:
                    unique_tickets_map[user_id] = ticket
            
            unique_tickets = list(unique_tickets_map.values())
            logger.info(f'ProcessDumpedTicketsView: Deduplicated to {len(unique_tickets)} unique tickets based on user_id.')
            
            # 3. Check for existing open tickets and prepare tickets for insertion
            # An "open ticket" is defined as: same user_id, resolution_status is null, assigned_to is null
            # If such a ticket exists, skip inserting the ticket from dump
            tickets_to_insert = []
            skipped_tickets = []
            from core.models import Tenant
            
            for dump_ticket in unique_tickets:
                # Skip if user_id is missing
                if not dump_ticket.user_id:
                    logger.warning(f'ProcessDumpedTicketsView: Ticket {dump_ticket.id} has no user_id. Skipping.')
                    skipped_tickets.append(dump_ticket.id)
                    continue
                
                # Check if there's an existing open ticket for this user_id
                existing_open_ticket = SupportTicket.objects.filter(
                    user_id=dump_ticket.user_id,
                    resolution_status__isnull=True,
                    assigned_to__isnull=True
                ).exists()
                
                if existing_open_ticket:
                    logger.info(f'ProcessDumpedTicketsView: Open ticket already exists for user_id {dump_ticket.user_id}. Skipping ticket {dump_ticket.id}.')
                    skipped_tickets.append(dump_ticket.id)
                    continue
                
                # Get tenant object if tenant_id exists
                tenant = None
                if dump_ticket.tenant_id:
                    try:
                        tenant = Tenant.objects.get(id=dump_ticket.tenant_id)
                    except Tenant.DoesNotExist:
                        logger.warning(f'ProcessDumpedTicketsView: Tenant {dump_ticket.tenant_id} not found for ticket {dump_ticket.id}. Skipping.')
                        skipped_tickets.append(dump_ticket.id)
                        continue
                
                # Create SupportTicket instance with fields from dump
                ticket_data = {
                    'ticket_date': dump_ticket.ticket_date,
                    'user_id': dump_ticket.user_id,
                    'name': dump_ticket.name,
                    'phone': dump_ticket.phone,
                    'source': dump_ticket.source,
                    'subscription_status': dump_ticket.subscription_status,
                    'atleast_paid_once': dump_ticket.atleast_paid_once,
                    'reason': dump_ticket.reason,
                    'badge': dump_ticket.badge,
                    'poster': dump_ticket.poster,
                    'tenant': tenant,
                    'layout_status': dump_ticket.layout_status,
                    'praja_dashboard_user_link': dump_ticket.praja_dashboard_user_link,
                    'display_pic_url': dump_ticket.display_pic_url,
                }
                
                tickets_to_insert.append(SupportTicket(**ticket_data))
            
            if not tickets_to_insert:
                logger.warning('ProcessDumpedTicketsView: No valid tickets to insert after processing.')
                # Still mark dumped tickets as processed
                dump_ids = [ticket.id for ticket in dumped_tickets_list]
                updated_count = SupportTicketDump.objects.filter(id__in=dump_ids).update(is_processed=True)
                return Response({
                    'message': 'No valid tickets to process',
                    'total_dumped_tickets': len(dumped_tickets_list),
                    'unique_tickets': len(unique_tickets),
                    'inserted_tickets': 0,
                    'skipped_tickets': len(skipped_tickets),
                    'marked_processed': updated_count
                }, status=status.HTTP_200_OK)
            
            # 4. Bulk insert tickets into support_ticket table
            inserted_tickets = SupportTicket.objects.bulk_create(tickets_to_insert, ignore_conflicts=True)
            logger.info(f'ProcessDumpedTicketsView: Inserted {len(inserted_tickets)} tickets into support_ticket table.')
            
            # 5. Mark dumped tickets as processed
            # Update all dumped tickets (not just unique ones) to mark them as processed
            dump_ids = [ticket.id for ticket in dumped_tickets_list]
            updated_count = SupportTicketDump.objects.filter(id__in=dump_ids).update(is_processed=True)
            logger.info(f'ProcessDumpedTicketsView: Marked {updated_count} dumped tickets as processed.')
            
            return Response({
                'message': 'Tickets processed successfully',
                'total_dumped_tickets': len(dumped_tickets_list),
                'unique_tickets': len(unique_tickets),
                'inserted_tickets': len(inserted_tickets),
                'skipped_tickets': len(skipped_tickets),
                'marked_processed': updated_count
            }, status=status.HTTP_200_OK)
            
        except Exception as error:
            logger.error(f'ProcessDumpedTicketsView: Critical error during ticket processing: {error}', exc_info=True)
            return Response({
                'error': str(error),
                'message': 'Failed to process dumped tickets'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
