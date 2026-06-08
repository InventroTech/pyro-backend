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
from django.db import IntegrityError
from config.supabase_auth import SupabaseJWTAuthentication
from authz.permissions import IsTenantAuthenticated
import os
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Union

from .models import SupportTicketDump
from .models import SupportTicket
from .serializers import SaveAndContinueSerializer, SaveAndContinueResponseSerializer, SupportTicketResponseSerializer, GetNextTicketResponseSerializer, SupportTicketUpdateSerializer, TakeBreakSerializer,UpdateCallStatusRequestSerializer
from .services import MixpanelService, TicketTimeService
from background_jobs.queue_service import get_queue_service
from background_jobs.models import BackgroundJob, JobStatus, JobType
from core.models import Tenant
from crm_records.models import Record
from user_settings.models import Group, TenantMemberSetting
from user_settings.services import USER_KV_GROUP_ID_KEY
from authz.permissions import IsTenantAuthenticated
from authz.models import TenantMembership
from accounts.models import SupabaseAuthUser
from datetime import timedelta
from analytics.serializers import SupportTicketSerializer
from .utils import send_to_mixpanel, ticket_to_mixpanel_data

logger = logging.getLogger(__name__)

SUPPORT_TICKET_ENTITY_TYPE = "support_ticket"
DUMP_BATCH_LIMIT = 5000


def _enqueue_mixpanel_event(
    *,
    user_id: Any,
    event_name: str,
    properties: Dict[str, Any],
    tenant_id: Any = None,
) -> None:
    if not user_id:
        logger.warning("Skipping Mixpanel enqueue for event=%s due to missing user_id", event_name)
        return
    try:
        queue_service = get_queue_service()
        queue_service.enqueue_job(
            job_type=JobType.SEND_MIXPANEL_EVENT,
            payload={
                "user_id": str(user_id),
                "event_name": event_name,
                "properties": properties or {},
            },
            tenant_id=str(tenant_id) if tenant_id else None,
            priority=0,
            max_attempts=3,
        )
    except Exception as e:
        logger.error("Failed to enqueue Mixpanel event=%s user_id=%s error=%s", event_name, user_id, e, exc_info=True)


def _dedupe_dumps_latest_wins(
    dumped_tickets: Sequence[SupportTicketDump],
) -> List[SupportTicketDump]:
    """One row per user_id; later rows in the batch win."""
    unique: Dict[str, SupportTicketDump] = {}
    for dump in dumped_tickets:
        if dump.user_id:
            unique[dump.user_id] = dump
    return list(unique.values())


def _build_support_record_data(ticket: SupportTicket) -> Dict[str, Any]:
    snooze_until = ticket.snooze_until
    tenant_id = str(ticket.tenant_id) if ticket.tenant_id else None
    assigned_to = str(ticket.assigned_to_id) if ticket.assigned_to_id else None

    data: Dict[str, Any] = {
        "support_ticket_id": ticket.id,
        "ticket_id": ticket.id,
        "created_at": ticket.created_at.isoformat() if ticket.created_at else None,
        "ticket_date": ticket.ticket_date.isoformat() if ticket.ticket_date else None,
        "user_id": ticket.user_id,
        "name": ticket.name,
        "phone": ticket.phone,
        "source": ticket.source,
        "subscription_status": ticket.subscription_status,
        "atleast_paid_once": ticket.atleast_paid_once,
        "reason": ticket.reason,
        "other_reasons": ticket.other_reasons or [],
        "badge": ticket.badge,
        "poster": ticket.poster,
        "tenant_id": tenant_id,
        "assigned_to": assigned_to,
        "layout_status": ticket.layout_status,
        "state": ticket.state,
        "resolution_status": ticket.resolution_status,
        "resolution_time": ticket.resolution_time,
        "cse_name": ticket.cse_name,
        "cse_remarks": ticket.cse_remarks,
        "call_status": ticket.call_status or "Call Waiting",
        "call_attempts": ticket.call_attempts if ticket.call_attempts is not None else 0,
        "rm_name": ticket.rm_name,
        "completed_at": ticket.completed_at.isoformat() if ticket.completed_at else None,
        "snooze_until": snooze_until.isoformat() if snooze_until else None,
        "praja_dashboard_user_link": ticket.praja_dashboard_user_link,
        "display_pic_url": ticket.display_pic_url,
        "dumped_at": ticket.dumped_at.isoformat() if ticket.dumped_at else None,
        "review_requested": bool(ticket.review_requested),
    }
    if snooze_until:
        data["next_call_at"] = snooze_until.isoformat()
    return data


def _delete_open_support_records_for_user(
    *,
    user_id: str,
    tenant_id: Optional[Any] = None,
) -> int:
    qs = Record.objects.filter(
        entity_type=SUPPORT_TICKET_ENTITY_TYPE,
        data__user_id=str(user_id),
    ).filter(
        Q(data__resolution_status__isnull=True) | Q(data__resolution_status="")
    )
    if tenant_id:
        qs = qs.filter(tenant_id=tenant_id)
    count, _ = qs.delete()
    return count


def _delete_open_support_tickets_for_user(
    user_id: str,
    tenant_id: Optional[Union[str, UUID]] = None,
) -> int:
    qs = SupportTicket.objects.filter(user_id=user_id, resolution_status__isnull=True)
    if tenant_id is not None:
        qs = qs.filter(tenant_id=tenant_id)
    count, _ = qs.delete()
    return count


def enqueue_ticket_created_mixpanel(ticket: SupportTicket) -> None:
    user_id = ticket.user_id or str(ticket.id)
    properties = {
        "ticket_id": ticket.id,
        "tenant_id": str(ticket.tenant.id) if ticket.tenant else None,
        "created_at": ticket.created_at.isoformat() if ticket.created_at else None,
        "ticket_date": ticket.ticket_date.isoformat() if ticket.ticket_date else None,
        "user_id": ticket.user_id,
        "name": ticket.name,
        "phone": ticket.phone,
        "source": ticket.source,
        "subscription_status": ticket.subscription_status,
        "atleast_paid_once": ticket.atleast_paid_once,
        "reason": ticket.reason,
        "other_reasons": ticket.other_reasons or [],
        "badge": ticket.badge,
        "poster": ticket.poster,
        "assigned_to": str(ticket.assigned_to.id) if ticket.assigned_to else None,
        "layout_status": ticket.layout_status,
        "state": ticket.state,
        "resolution_status": ticket.resolution_status,
        "resolution_time": ticket.resolution_time,
        "cse_name": ticket.cse_name,
        "cse_remarks": ticket.cse_remarks,
        "call_status": ticket.call_status,
        "call_attempts": ticket.call_attempts,
        "rm_name": ticket.rm_name,
        "completed_at": ticket.completed_at.isoformat() if ticket.completed_at else None,
        "snooze_until": ticket.snooze_until.isoformat() if ticket.snooze_until else None,
        "praja_dashboard_user_link": ticket.praja_dashboard_user_link,
        "display_pic_url": ticket.display_pic_url,
        "dumped_at": ticket.dumped_at.isoformat() if ticket.dumped_at else None,
        "review_requested": ticket.review_requested,
    }
    get_queue_service().enqueue_job(
        job_type=JobType.SEND_MIXPANEL_EVENT,
        payload={
            "user_id": str(user_id),
            "event_name": "pyro_st_ticket_created",
            "properties": properties,
        },
        tenant_id=str(ticket.tenant_id) if ticket.tenant_id else None,
        priority=0,
    )


def enqueue_process_dumped_tickets_job(
    tenant_id: Union[str, UUID],
    *,
    priority: int = 0,
) -> Optional[BackgroundJob]:
    tid = str(tenant_id)
    active = BackgroundJob.objects.filter(
        job_type=JobType.PROCESS_DUMPED_TICKETS,
        tenant_id=tid,
        status__in=[JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.RETRYING],
    ).exists()
    if active:
        logger.info(
            "enqueue_process_dumped_tickets_job: active job already exists for tenant=%s",
            tid,
        )
        return None

    job = get_queue_service().enqueue_job(
        job_type=JobType.PROCESS_DUMPED_TICKETS,
        payload={},
        tenant_id=tid,
        priority=priority,
    )
    logger.info(
        "enqueue_process_dumped_tickets_job: enqueued job_id=%s tenant=%s",
        job.id,
        tid,
    )
    return job


def enqueue_process_dumped_tickets_for_pending_dumps() -> Dict[str, Any]:
    """
    Enqueue one ``process_dumped_tickets`` job per tenant with unprocessed dump rows.
    Called by the background worker every 5 minutes (same cadence as the old cron).
    """
    tenant_ids = (
        SupportTicketDump.objects.filter(
            Q(is_processed__isnull=True) | Q(is_processed=False)
        )
        .values_list("tenant_id", flat=True)
        .distinct()
    )
    enqueued = []
    skipped = []
    for tid in tenant_ids:
        job = enqueue_process_dumped_tickets_job(tid)
        if job:
            enqueued.append({"tenant_id": str(tid), "job_id": job.id})
        else:
            skipped.append(str(tid))
    return {"enqueued": enqueued, "skipped_active_job": skipped}


def _mirror_tickets_to_records(tickets: Iterable[SupportTicket]) -> int:
    mirrored = 0
    for ticket in tickets:
        if not ticket.id or not ticket.tenant_id:
            logger.warning(
                "Skipping records mirror for ticket %s: missing id or tenant_id",
                ticket.id,
            )
            continue
        Record.objects.create(
            tenant_id=ticket.tenant_id,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data=_build_support_record_data(ticket),
        )
        mirrored += 1
    return mirrored


def _support_ticket_from_dump(dump_ticket: SupportTicketDump, tenant: Tenant) -> SupportTicket:
    return SupportTicket(
        ticket_date=dump_ticket.ticket_date,
        user_id=dump_ticket.user_id,
        name=dump_ticket.name,
        phone=dump_ticket.phone,
        source=dump_ticket.source,
        subscription_status=dump_ticket.subscription_status,
        atleast_paid_once=dump_ticket.atleast_paid_once,
        reason=dump_ticket.reason,
        badge=dump_ticket.badge,
        poster=dump_ticket.poster,
        tenant=tenant,
        layout_status=dump_ticket.layout_status,
        state=dump_ticket.state,
        praja_dashboard_user_link=dump_ticket.praja_dashboard_user_link,
        display_pic_url=dump_ticket.display_pic_url,
        dumped_at=timezone.now(),
    )


@dataclass
class ProcessDumpedTicketsResult:
    total_dumped_tickets: int
    unique_tickets: int
    inserted_tickets: int
    mirrored_records: int
    skipped_tickets: int
    marked_processed: int


def process_dumped_tickets(
    *,
    tenant_id: Optional[Union[str, UUID]] = None,
    on_ticket_created: Optional[Callable[[SupportTicket], None]] = None,
    batch_limit: int = DUMP_BATCH_LIMIT,
) -> ProcessDumpedTicketsResult:
    dumped_qs = SupportTicketDump.objects.filter(
        Q(is_processed__isnull=True) | Q(is_processed=False)
    )
    if tenant_id is not None:
        dumped_qs = dumped_qs.filter(tenant_id=tenant_id)
    dumped_qs = dumped_qs.order_by("id")[:batch_limit]
    dumped_tickets_list = list(dumped_qs)

    if not dumped_tickets_list:
        logger.info("process_dumped_tickets: No new tickets in dump table to process.")
        return ProcessDumpedTicketsResult(0, 0, 0, 0, 0, 0)

    unique_tickets = _dedupe_dumps_latest_wins(dumped_tickets_list)
    tickets_to_insert: List[SupportTicket] = []
    skipped = 0

    for dump_ticket in unique_tickets:
        if not dump_ticket.user_id:
            skipped += 1
            continue
        if not dump_ticket.tenant_id:
            skipped += 1
            continue
        try:
            tenant = Tenant.objects.get(id=dump_ticket.tenant_id)
        except Tenant.DoesNotExist:
            skipped += 1
            continue

        with transaction.atomic():
            _delete_open_support_tickets_for_user(
                dump_ticket.user_id,
                tenant_id=dump_ticket.tenant_id,
            )
            _delete_open_support_records_for_user(
                user_id=dump_ticket.user_id,
                tenant_id=dump_ticket.tenant_id,
            )
            tickets_to_insert.append(_support_ticket_from_dump(dump_ticket, tenant))

    dump_ids = [t.id for t in dumped_tickets_list]

    if not tickets_to_insert:
        marked = SupportTicketDump.objects.filter(id__in=dump_ids).update(is_processed=True)
        return ProcessDumpedTicketsResult(
            total_dumped_tickets=len(dumped_tickets_list),
            unique_tickets=len(unique_tickets),
            inserted_tickets=0,
            mirrored_records=0,
            skipped_tickets=skipped,
            marked_processed=marked,
        )

    inserted_tickets = SupportTicket.objects.bulk_create(
        tickets_to_insert,
        ignore_conflicts=True,
    )
    mirrored = _mirror_tickets_to_records(inserted_tickets)

    if on_ticket_created:
        for ticket in inserted_tickets:
            try:
                on_ticket_created(ticket)
            except Exception as exc:
                logger.error(
                    "process_dumped_tickets: on_ticket_created failed for ticket %s: %s",
                    ticket.id,
                    exc,
                    exc_info=True,
                )

    marked = SupportTicketDump.objects.filter(id__in=dump_ids).update(is_processed=True)
    return ProcessDumpedTicketsResult(
        total_dumped_tickets=len(dumped_tickets_list),
        unique_tickets=len(unique_tickets),
        inserted_tickets=len(inserted_tickets),
        mirrored_records=mirrored,
        skipped_tickets=skipped,
        marked_processed=marked,
    )


def process_dumped_tickets_job_result(result: ProcessDumpedTicketsResult) -> Dict[str, Any]:
    return asdict(result)


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
            # Use .only() to prevent N+1 queries by fetching only required fields
            wip_tickets = SupportTicket.objects.filter(
                assigned_to=user_id,
                resolution_status='WIP'
            ).only(
                "id", "created_at", "ticket_date", "user_id", "name", "phone", "source",
                "subscription_status", "atleast_paid_once", "reason", "other_reasons",
                "badge", "poster", "tenant_id", "assigned_to", "layout_status", "state",
                "resolution_status", "resolution_time", "cse_name", "cse_remarks",
                "call_status", "call_attempts", "rm_name", "completed_at", "snooze_until",
                "praja_dashboard_user_link", "display_pic_url", "dumped_at", "review_requested"
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
            
            # Staging row for background job processor
            cleaned_data['is_processed'] = False
            
            # 4. Insert the cleaned data into the dump table (processed every 5 min by worker)
            dump_ticket = SupportTicketDump.objects.create(**cleaned_data)

            return Response({
                'message': 'Ticket created successfully in dump table',
                'ticket_id': dump_ticket.id,
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
    permission_classes = [IsTenantAuthenticated] # <--- ADD THIS LINE!
    
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
                mixpanel_event_name = 'pyro_st_resolve'
            elif resolution_status == "Can't Resolve":
                mixpanel_event_name = 'pyro_st_cannot_resolve'
            elif resolution_status == 'WIP':
                mixpanel_event_name = 'pyro_st_call_later'
            
            # Send Mixpanel events - REQUIRED, must work (old properties + all ticket column data)
            if mixpanel_event_name and current_ticket.user_id:
                logger.info(f'Sending REQUIRED Mixpanel events for user_id: {current_ticket.user_id}, event: {mixpanel_event_name}')
                
                mixpanel_properties = {
                    'support_ticket_id': ticket_id,
                    'remarks': cse_remarks or '',
                    'cse_email_id': user_email,
                    'reasons': other_reasons or [],
                    'review_requested': review_requested
                }
                mixpanel_properties.update(ticket_to_mixpanel_data(current_ticket))
                
                _enqueue_mixpanel_event(
                    user_id=current_ticket.user_id,
                    event_name='pyro_st_connected',
                    properties=mixpanel_properties
                )
                
                _enqueue_mixpanel_event(
                    user_id=current_ticket.user_id,
                    event_name=mixpanel_event_name,
                    properties=mixpanel_properties
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

            logger.info("=" * 80)
            logger.info("🎫 [GetNextTicketView] GET TICKETS BUTTON CLICKED")
            logger.info("=" * 80)
            logger.info(f"=== TICKET ORDERING VALIDATION ===")
            logger.info(f"Current time: {timezone.now()}")
            logger.info(f"User ID: {user_id}")
            logger.info(f"User Email: {user_email}")

            # Ensure current user exists in auth.users (FK target for assigned_to) before assigning
            try:
                user_uuid = UUID(str(user_id))
            except (ValueError, AttributeError, TypeError):
                logger.warning(
                    "[GetNextTicketView] Invalid user supabase_uid; cannot assign ticket",
                    extra={"user_id": user_id, "user_email": user_email},
                )
                response = Response(
                    {
                        "error": "Your account could not be verified. Please sign out and sign in again, or contact support.",
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )
                response["Access-Control-Allow-Origin"] = "*"
                return response

            if not SupabaseAuthUser.objects.filter(id=user_uuid).exists():
                logger.warning(
                    "[GetNextTicketView] User not found in auth.users (assignee would violate FK); refusing to assign",
                    extra={
                        "user_id": str(user_uuid),
                        "user_email": user_email,
                        "assignee_in_auth_users": False,
                    },
                )
                try:
                    import sentry_sdk
                    sentry_sdk.set_user({"id": str(user_uuid), "email": user_email or ""})
                    sentry_sdk.set_tag("get_next_ticket_assigned_to_fk", "assignee_not_in_auth_users")
                except Exception:
                    pass
                response = Response(
                    {
                        "error": "Your account is not found in the auth system. Please sign out and sign in again, or contact support.",
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )
                response["Access-Control-Allow-Origin"] = "*"
                return response

            # Get the next ticket
            logger.info(f"[GetNextTicketView] Calling _get_and_assign_ticket to find and assign ticket...")
            with transaction.atomic():
                next_ticket = self._get_and_assign_ticket(request, user, user_email)

            # If no tickets available, return empty object
            if not next_ticket:
                logger.info("[GetNextTicketView] ⚠️ No tickets available - returning empty response")
                response = Response({}, status=status.HTTP_200_OK)
                response['Access-Control-Allow-Origin'] = '*'
                return response
            
            # Return the ticket
            logger.info(f"[GetNextTicketView] ✅ Ticket found and assigned - Ticket ID: {next_ticket.id}")
            logger.info(f"[GetNextTicketView] Ticket user_id (customer): {next_ticket.user_id}")
            logger.info(f"[GetNextTicketView] Assigned to CSE: {user_email} ({user_id})")
            response_data = {'ticket': next_ticket}
            serializer = GetNextTicketResponseSerializer(response_data)
            
            response = Response(serializer.data, status=status.HTTP_200_OK)
            response['Access-Control-Allow-Origin'] = '*'
            logger.info("=" * 80)
            return response
            
        except IntegrityError as error:
            user_id_ctx = getattr(request.user, "supabase_uid", None)
            user_email_ctx = getattr(request.user, "email", None)
            logger.error(
                "get-next-ticket: database constraint violation (e.g. assigned_to FK); assignee may not exist in auth.users",
                exc_info=True,
                extra={
                    "user_id": str(user_id_ctx) if user_id_ctx else None,
                    "user_email": user_email_ctx,
                    "error": str(error),
                },
            )
            try:
                import sentry_sdk
                sentry_sdk.set_user({"id": str(user_id_ctx), "email": user_email_ctx or ""})
                sentry_sdk.set_tag("get_next_ticket_assigned_to_fk", "integrity_error")
                sentry_sdk.capture_exception(error)
            except Exception:
                pass
            response = Response(
                {"error": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
            response["Access-Control-Allow-Origin"] = "*"
            return response
        except Exception as error:
            user_id_ctx = getattr(request.user, "supabase_uid", None)
            user_email_ctx = getattr(request.user, "email", None)
            logger.error(f"Error in get-next-ticket function: {error}", exc_info=True)
            try:
                import sentry_sdk
                sentry_sdk.set_user({"id": str(user_id_ctx), "email": user_email_ctx or ""})
            except Exception:
                pass
            response = Response(
                {"error": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
            response["Access-Control-Allow-Origin"] = "*"
            return response

    def _get_and_assign_ticket(self, request, user, user_email):
        """
        Simplified logic to get and assign a ticket to the user.
        1. First get the newest unassigned ticket (LIFO - Last In, First Out)
        2. Then check for snoozed tickets for this user
        """
        current_time = timezone.now()
        tenant = getattr(request, "tenant", None)
        
        logger.info(f"[_get_and_assign_ticket] Starting ticket assignment process")
        logger.info(f"[_get_and_assign_ticket] User: {user.supabase_uid} ({user_email})")
        logger.info(f"[_get_and_assign_ticket] Tenant: {tenant.id if tenant else None}")
        logger.info(f"[_get_and_assign_ticket] Current time: {current_time}")

        # 1. Get the ticket with resolution_status null that is assigned to the user or is snoozed and assigned to current user only
        logger.info(f"[_get_and_assign_ticket] Step 1: Looking for tickets with resolution_status null and assigned to user: {user.supabase_uid}")
        
        try:
            user_uuid_obj = UUID(str(user.supabase_uid))
            logger.info(f"[_get_and_assign_ticket] Successfully converted user.supabase_uid to UUID: {user_uuid_obj}")
        except (ValueError, AttributeError, TypeError) as e:
            logger.error(f"[_get_and_assign_ticket] Failed to convert user.supabase_uid to UUID: {e}")
            logger.error(f"[_get_and_assign_ticket] user.supabase_uid type: {type(user.supabase_uid)}, value: {user.supabase_uid}")
            # Return None if we can't convert to UUID
            return None

        def _apply_ticket_filters_for_user(qs):
            """
            Filter tickets using the assigned user group only.
            If no group is configured, leave queryset unchanged.
            """
            membership = TenantMembership.objects.filter(
                tenant=tenant,
                user_id=request.user.supabase_uid,
            ).first()
            if not membership:
                logger.info("[_get_and_assign_ticket] Group ticket filter: no membership found, leaving queryset unchanged")
                return qs

            group_row = TenantMemberSetting.objects.filter(
                tenant=tenant,
                tenant_membership=membership,
                key=USER_KV_GROUP_ID_KEY,
            ).first()
            group_id = group_row.value if group_row else None
            if not isinstance(group_id, int):
                logger.info("[_get_and_assign_ticket] Group ticket filter: no GROUP setting, leaving queryset unchanged")
                return qs

            group = Group.objects.filter(tenant=tenant, id=group_id).first()
            group_data = group.group_data if group and isinstance(group.group_data, dict) else {}
            states = group_data.get("states") if isinstance(group_data.get("states"), list) else []
            posters = group_data.get("posters") if isinstance(group_data.get("posters"), list) else []

            if states:
                qs = qs.filter(state__in=states)
            if posters:
                qs = qs.filter(poster__in=posters)

            logger.info(
                "[_get_and_assign_ticket] Group ticket filter applied: group_id=%s states=%s posters=%s remaining=%s",
                group_id,
                states or "(none)",
                posters or "(none)",
                qs.count(),
            )
            return qs
        
        already_assigned_qs = SupportTicket.objects.select_for_update(
            skip_locked=True,
            of=("self",)
        ).filter(
            assigned_to=request.user.supabase_uid,
        ).filter(
            Q(resolution_status__isnull=True) | Q(resolution_status="Snoozed")
        ).exclude(
            poster__in=["Trial Expired", "Premium Expired", "trial_expired", "premium_expired"]
        )
        
        # Apply routing rules to already assigned tickets as well
        
        logger.info(f"[_get_and_assign_ticket] Step 1: Applying routing rules to already assigned tickets")
        try:
            already_assigned_qs = _apply_ticket_filters_for_user(already_assigned_qs)
        except Exception as routing_error:
            logger.error(f"[_get_and_assign_ticket] Step 1: Error applying routing rules: {routing_error}")
            logger.exception(routing_error)
            # Continue without routing rules if there's an error
        
        already_assigned_ticket = already_assigned_qs.order_by('created_at').first()
        
        if already_assigned_ticket:
            logger.info(f"[_get_and_assign_ticket] Step 1 SUCCESS: Found ticket already assigned to user")
            logger.info(f"[_get_and_assign_ticket] Ticket ID: {already_assigned_ticket.id}")
            logger.info(f"[_get_and_assign_ticket] Ticket created at: {already_assigned_ticket.created_at}")
            logger.info(f"[_get_and_assign_ticket] Ticket resolution_status: {already_assigned_ticket.resolution_status}")
            already_assigned_ticket.assigned_to_id = user_uuid_obj
            already_assigned_ticket.cse_name = user_email
            already_assigned_ticket.save()
            
            # Send Mixpanel event for ticket assignment
            logger.info(f"[_get_and_assign_ticket] Step 1: Preparing to send Mixpanel event 'support_ticket_assignment'")
            logger.info(f"[_get_and_assign_ticket] Step 1: Ticket user_id: {already_assigned_ticket.user_id}")
            
            if already_assigned_ticket.user_id:
                try:
                    logger.info(f"[_get_and_assign_ticket] Step 1: ✅ Ticket has user_id, sending Mixpanel event")
                    logger.info(f"[_get_and_assign_ticket] Step 1: Event: support_ticket_assignment")
                    logger.info(f"[_get_and_assign_ticket] Step 1: User ID (customer): {already_assigned_ticket.user_id}")
                    logger.info(f"[_get_and_assign_ticket] Step 1: CSE assigned: {user_email} ({user_uuid_obj})")
                    logger.info(f"[_get_and_assign_ticket] Step 1: Ticket ID: {already_assigned_ticket.id}")
                    
                    mixpanel_service = MixpanelService()
                    mixpanel_properties = {
                        "ticket_id": already_assigned_ticket.id,
                        "tenant_id": str(already_assigned_ticket.tenant.id) if already_assigned_ticket.tenant else None,
                        "assigned_to": str(user_uuid_obj),
                        "cse_name": user_email,
                        "cse_email": user_email,
                        "poster": already_assigned_ticket.poster,
                        "source": already_assigned_ticket.source,
                        "resolution_status": already_assigned_ticket.resolution_status,
                        "created_at": already_assigned_ticket.created_at.isoformat() if already_assigned_ticket.created_at else None,
                    }
                    logger.info(f"[_get_and_assign_ticket] Step 1: Mixpanel properties: {json.dumps(mixpanel_properties, indent=2, default=str)}")
                    _enqueue_mixpanel_event(
                        user_id=already_assigned_ticket.user_id,
                        event_name="pyro_st_assigned",
                        properties=mixpanel_properties
                    )
                    logger.info(f"[_get_and_assign_ticket] Step 1: ✅ Mixpanel event 'support_ticket_assignment' sent successfully")
                except Exception as mixpanel_error:
                    logger.error(f"[_get_and_assign_ticket] Step 1: ❌ Error sending Mixpanel event: {mixpanel_error}")
                    logger.exception(mixpanel_error)
            else:
                logger.warning(f"[_get_and_assign_ticket] Step 1: ⚠️ Skipping Mixpanel event - ticket.user_id is None or empty")
            
            return already_assigned_ticket
        else:
            logger.info(f"[_get_and_assign_ticket] Step 1: No tickets found with resolution_status null and assigned to user")

        # 2. LIFO logic: get the newest unassigned ticket, constrained by routing rules if present
        logger.info(f"[_get_and_assign_ticket] Step 2: Searching for unassigned tickets with row locking")

        base_qs = SupportTicket.objects.select_for_update(
            skip_locked=True,
            of=("self",)
        ).filter(
            assigned_to__isnull=True,
            resolution_status__isnull=True
        ).exclude(
            poster__in=["Trial Expired", "Premium Expired", "trial_expired", "premium_expired"]
        ).order_by('-created_at')
        
        logger.info(f"[_get_and_assign_ticket] Step 2: Base queryset count (before routing): {base_qs.count()}")

        # Apply per-user routing rule if configured for tickets
        
        logger.info(f"[_get_and_assign_ticket] Step 2: Applying routing rules for tenant={tenant.id}, user={user_uuid_obj}, queue_type=ticket")
        try:
            base_qs = _apply_ticket_filters_for_user(base_qs)
            logger.info(f"[_get_and_assign_ticket] Step 2: After routing rules, queryset count: {base_qs.count()}")
        except Exception as routing_error:
            logger.error(f"[_get_and_assign_ticket] Step 2: Error applying routing rules: {routing_error}")
            logger.exception(routing_error)
            # Continue without routing rules if there's an error
        

        unassigned_ticket = base_qs.order_by("-created_at")[:1].first()
        
        if unassigned_ticket:
            logger.info(f"[_get_and_assign_ticket] Step 2 SUCCESS: Found unassigned ticket")
            logger.info(f"[_get_and_assign_ticket] Ticket ID: {unassigned_ticket.id}")
            logger.info(f"[_get_and_assign_ticket] Ticket created at: {unassigned_ticket.created_at}")
            logger.info(f"[_get_and_assign_ticket] Ticket poster: {unassigned_ticket.poster}")
            logger.info(f"[_get_and_assign_ticket] Ticket source: {unassigned_ticket.source}")

            # Assign the ticket to the user (assigned_to is UUIDField, so use UUID object)
            unassigned_ticket.assigned_to_id = user_uuid_obj
            unassigned_ticket.cse_name = user_email
            unassigned_ticket.save()
            logger.info(f"[_get_and_assign_ticket] Step 2: Successfully assigned ticket {unassigned_ticket.id} to user {user_uuid_obj}")
            
            # Send Mixpanel event for ticket assignment
            logger.info(f"[_get_and_assign_ticket] Step 2: Preparing to send Mixpanel event 'support_ticket_assignment'")
            logger.info(f"[_get_and_assign_ticket] Step 2: Ticket user_id: {unassigned_ticket.user_id}")
            
            if unassigned_ticket.user_id:
                try:
                    logger.info(f"[_get_and_assign_ticket] Step 2: ✅ Ticket has user_id, sending Mixpanel event")
                    logger.info(f"[_get_and_assign_ticket] Step 2: Event: support_ticket_assignment")
                    logger.info(f"[_get_and_assign_ticket] Step 2: User ID (customer): {unassigned_ticket.user_id}")
                    logger.info(f"[_get_and_assign_ticket] Step 2: CSE assigned: {user_email} ({user_uuid_obj})")
                    logger.info(f"[_get_and_assign_ticket] Step 2: Ticket ID: {unassigned_ticket.id}")
                    
                    mixpanel_properties = {
                        "ticket_id": unassigned_ticket.id,
                        "tenant_id": str(unassigned_ticket.tenant.id) if unassigned_ticket.tenant else None,
                        "assigned_to": str(user_uuid_obj),
                        "cse_name": user_email,
                        "cse_email": user_email,
                        "poster": unassigned_ticket.poster,
                        "source": unassigned_ticket.source,
                        "resolution_status": unassigned_ticket.resolution_status,
                        "created_at": unassigned_ticket.created_at.isoformat() if unassigned_ticket.created_at else None,
                    }
                    logger.info(f"[_get_and_assign_ticket] Step 2: Mixpanel properties: {json.dumps(mixpanel_properties, indent=2, default=str)}")
                    _enqueue_mixpanel_event(
                        user_id=unassigned_ticket.user_id,
                        event_name="pyro_st_assigned",
                        properties=mixpanel_properties
                    )
                    logger.info(f"[_get_and_assign_ticket] Step 2: ✅ Mixpanel event 'support_ticket_assignment' sent successfully")
                except Exception as mixpanel_error:
                    logger.error(f"[_get_and_assign_ticket] Step 2: ❌ Error sending Mixpanel event: {mixpanel_error}")
                    logger.exception(mixpanel_error)
            else:
                logger.warning(f"[_get_and_assign_ticket] Step 2: ⚠️ Skipping Mixpanel event - ticket.user_id is None or empty")
            
            return unassigned_ticket
        else:
            logger.info(f"[_get_and_assign_ticket] Step 2: No unassigned tickets found matching criteria")

        # 3. Look for snoozed tickets for this user as fallback
        logger.info(f"[_get_and_assign_ticket] Step 3: Looking for snoozed tickets")
        logger.info(f"[_get_and_assign_ticket] Step 3: Current time: {current_time}")

        snoozed_qs = SupportTicket.objects.select_for_update(
            skip_locked=True,
            of=("self",)
        ).filter(
            resolution_status="Snoozed",
            assigned_to__isnull=True,
            snooze_until__isnull=False,
            snooze_until__lte=current_time
        ).exclude(
            poster__in=["Trial Expired", "Premium Expired", "trial_expired", "premium_expired"]
        )
        
        # Apply routing rules to snoozed tickets as well
        if tenant and request.user:
            logger.info(f"[_get_and_assign_ticket] Step 3: Applying routing rules to snoozed tickets")
            try:
                snoozed_qs = _apply_ticket_filters_for_user(snoozed_qs)
                logger.info(f"[_get_and_assign_ticket] Step 3: After routing rules, snoozed queryset count: {snoozed_qs.count()}")
            except Exception as routing_error:
                logger.error(f"[_get_and_assign_ticket] Step 3: Error applying routing rules: {routing_error}")
                logger.exception(routing_error)
                # Continue without routing rules if there's an error
        
        snoozed_ticket = snoozed_qs.order_by('-snooze_until').first()
        
        if snoozed_ticket:
            logger.info(f"[_get_and_assign_ticket] Step 3 SUCCESS: Found snoozed ticket")
            logger.info(f"[_get_and_assign_ticket] Snoozed ticket ID: {snoozed_ticket.id}")
            logger.info(f"[_get_and_assign_ticket] Snooze until: {snoozed_ticket.snooze_until}")
            logger.info(f"[_get_and_assign_ticket] Created at: {snoozed_ticket.created_at}")
            
            snoozed_ticket.assigned_to_id = user_uuid_obj
            snoozed_ticket.cse_name = user_email
            snoozed_ticket.save()
            logger.info(f"[_get_and_assign_ticket] Step 3: Successfully assigned snoozed ticket {snoozed_ticket.id} to user {user_uuid_obj}")
            
            # Send Mixpanel event for ticket assignment
            logger.info(f"[_get_and_assign_ticket] Step 3: Preparing to send Mixpanel event 'support_ticket_assignment'")
            logger.info(f"[_get_and_assign_ticket] Step 3: Ticket user_id: {snoozed_ticket.user_id}")
            
            if snoozed_ticket.user_id:
                try:
                    logger.info(f"[_get_and_assign_ticket] Step 3: ✅ Ticket has user_id, sending Mixpanel event")
                    logger.info(f"[_get_and_assign_ticket] Step 3: Event: support_ticket_assignment")
                    logger.info(f"[_get_and_assign_ticket] Step 3: User ID (customer): {snoozed_ticket.user_id}")
                    logger.info(f"[_get_and_assign_ticket] Step 3: CSE assigned: {user_email} ({user_uuid_obj})")
                    logger.info(f"[_get_and_assign_ticket] Step 3: Ticket ID: {snoozed_ticket.id}")
                    logger.info(f"[_get_and_assign_ticket] Step 3: Snooze until: {snoozed_ticket.snooze_until}")
                    
                    mixpanel_service = MixpanelService()
                    mixpanel_properties = {
                        "ticket_id": snoozed_ticket.id,
                        "tenant_id": str(snoozed_ticket.tenant.id) if snoozed_ticket.tenant else None,
                        "assigned_to": str(user_uuid_obj),
                        "cse_name": user_email,
                        "cse_email": user_email,
                        "poster": snoozed_ticket.poster,
                        "source": snoozed_ticket.source,
                        "resolution_status": snoozed_ticket.resolution_status,
                        "created_at": snoozed_ticket.created_at.isoformat() if snoozed_ticket.created_at else None,
                        "snooze_until": snoozed_ticket.snooze_until.isoformat() if snoozed_ticket.snooze_until else None,
                    }
                    logger.info(f"[_get_and_assign_ticket] Step 3: Mixpanel properties: {json.dumps(mixpanel_properties, indent=2, default=str)}")
                    _enqueue_mixpanel_event(
                        user_id=snoozed_ticket.user_id,
                        event_name="pyro_st_assigned",
                        properties=mixpanel_properties
                    )
                    logger.info(f"[_get_and_assign_ticket] Step 3: ✅ Mixpanel event 'support_ticket_assignment' sent successfully")
                except Exception as mixpanel_error:
                    logger.error(f"[_get_and_assign_ticket] Step 3: ❌ Error sending Mixpanel event: {mixpanel_error}")
                    logger.exception(mixpanel_error)
            else:
                logger.warning(f"[_get_and_assign_ticket] Step 3: ⚠️ Skipping Mixpanel event - ticket.user_id is None or empty")
            
            return snoozed_ticket
        else:
            logger.info(f"[_get_and_assign_ticket] Step 3: No snoozed tickets found")

        
        # No tickets available
        logger.info(f"[_get_and_assign_ticket] FINAL: No tickets available for user {user_uuid_obj}")
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

            # Send Mixpanel event (outside transaction): old properties + all ticket column data
            if call_status == "Not Connected" and ticket.user_id:
                mixpanel_service = MixpanelService()
                mixpanel_properties = {
                    "support_ticket_id": ticket_id,
                    "remarks": cse_remarks or "",
                    "cse_email_id": getattr(request.user, "email", None),
                    "reasons": other_reasons or [],
                }
                mixpanel_properties.update(ticket_to_mixpanel_data(ticket))
                mixpanel_service.send_to_mixpanel_sync(
                    ticket.user_id,
                    "pyro_st_not_connected",
                    mixpanel_properties,
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
    """
    Manual / ops trigger: enqueue ``process_dumped_tickets`` background job(s).

    Normal flow: background worker enqueues every 5 minutes for tenants with
    unprocessed dumps. POST body may include ``tenant_id`` for a single tenant.
    """
    permission_classes = [AllowAny]

    def post(self, request):
        try:
            tenant_id = (request.data or {}).get('tenant_id')
            if tenant_id:
                job = enqueue_process_dumped_tickets_job(tenant_id)
                if not job:
                    return Response({
                        'message': 'Job already queued or running for tenant',
                        'tenant_id': str(tenant_id),
                    }, status=status.HTTP_200_OK)
                return Response({
                    'message': 'Job enqueued',
                    'job_id': job.id,
                    'tenant_id': str(tenant_id),
                }, status=status.HTTP_202_ACCEPTED)

            result = enqueue_process_dumped_tickets_for_pending_dumps()
            return Response({
                'message': 'Jobs enqueued for tenants with unprocessed dumps',
                **result,
            }, status=status.HTTP_202_ACCEPTED)

        except Exception as error:
            logger.error(
                'ProcessDumpedTicketsView: Failed to enqueue jobs: %s',
                error,
                exc_info=True,
            )
            return Response({
                'error': str(error),
                'message': 'Failed to enqueue process dumped tickets jobs',
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
