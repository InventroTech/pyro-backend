import os
from rest_framework import generics, status, serializers
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError, NotFound
from rest_framework.permissions import AllowAny
from authz.permissions import IsTenantAuthenticated
from core.pagination import MetaPageNumberPagination
from core.models import Tenant
from django.utils import timezone
from datetime import datetime, time, timedelta, timezone as std_utc
try:
    from dateutil import parser as date_parser
except ImportError:
    date_parser = None
from django.db.models import Q, F, Count, Case, When, Value, IntegerField
from django.db import transaction
from django.db import IntegrityError
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiExample, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
import logging

logger = logging.getLogger(__name__)
from .models import Record, EventLog, RuleSet, RuleExecutionLog, EntityTypeSchema, CallAttemptMatrix, ScoringRule, PartnerEvent, ApiSecretKey
from .serializers import RecordSerializer, EventLogSerializer, RuleSetSerializer, RuleExecutionLogSerializer, EntityTypeSchemaSerializer, LeadScoringRequestSerializer, CallAttemptMatrixSerializer, ScoringRuleModelSerializer
from .mixins import TenantScopedMixin
from .events import dispatch_event
from .scoring import calculate_and_update_lead_score
from user_settings.models import UserSettings
from .permissions import HasAPISecret
from support_ticket.services import MixpanelService, RMAssignedMixpanelService
from background_jobs.queue_service import get_queue_service
from background_jobs.models import JobType
from .lead_filters import get_lead_filters_for_user
import requests
import uuid
from authz.models import TenantMembership
from email_protocol.services import send_email
from email_protocol.templates.newRequestUnmannd import build_new_request_unmannd_email

from crm_records.lead_assignment_tracking import merge_first_assignment_today_anchor
from crm_records.lead_pipeline.pipeline import LeadPipeline
from crm_records.lead_pipeline.post_assignment import PostAssignmentActions


def _parse_lead_stage_param(value):
    """Parse lead_stage query param (comma-separated) into a set of uppercase stage names."""
    if not value or not isinstance(value, str):
        return set()
    return {v.strip().upper() for v in value.split(',') if v.strip()}


def _legacy_get_next_lead_assignee_is_unassigned(value):
    """GetNextLeadView (SELF TRIAL legacy path): True when ``data.assigned_to`` is still empty / null."""
    if value is None:
        return True
    s = str(value).strip()
    if s == "":
        return True
    return s.lower() in ("null", "none")


def _legacy_get_next_lead_assignees_match(stored, requester: str) -> bool:
    """True when ``assigned_to`` in DB matches the requesting RM (trimmed, case-insensitive)."""
    if _legacy_get_next_lead_assignee_is_unassigned(stored):
        return False
    return str(stored).strip().lower() == str(requester).strip().lower()


def _notify_team_lead_for_inventory_request(request, record):
    """
    Send team-lead email notification for newly created inventory requests.
    Never raises (best effort only).
    """
    if not record or record.entity_type not in {"inventory_request", "unmannd_request"}:
        return

    data = record.data if isinstance(record.data, dict) else {}
    team_lead_value = data.get("team_lead")
    if team_lead_value in (None, ""):
        logger.info(
            "Inventory request %s email skipped: team_lead missing in payload.",
            getattr(record, "id", None),
        )
        return

    tenant = getattr(request, "tenant", None)
    if not tenant:
        logger.info(
            "Inventory request %s email skipped: tenant missing on request context.",
            getattr(record, "id", None),
        )
        return

    try:
        team_lead_email = None
        team_lead_name = "Team Lead"
        membership = None
        try:
            membership = TenantMembership.objects.filter(
                tenant=tenant,
                id=team_lead_value,
                is_active=True,
            ).select_related("role").first()
        except Exception:
            membership = None

        if membership:
            team_lead_email = (membership.email or "").strip().lower()
            team_lead_name = (membership.name or membership.email or "Team Lead").strip()

        if not team_lead_email:
            logger.info(
                "Inventory request %s email skipped: no active team lead email found for team_lead=%s",
                record.id,
                team_lead_value,
            )
            return

        frontend_base = (os.environ.get("PYRO_FRONTEND_URL") or os.environ.get("FRONTEND_URL") or "").strip().rstrip("/")
        tenant_slug = str(getattr(tenant, "slug", "") or "").strip()
        if frontend_base and "/app/" in frontend_base:
            # If env already includes app path, use it directly.
            redirect_url = frontend_base
        elif frontend_base and tenant_slug:
            # Preferred app redirect pattern.
            redirect_url = f"{frontend_base}/app/{tenant_slug}"
        elif frontend_base:
            redirect_url = frontend_base
        elif tenant_slug:
            # Hard fallback to production app URL shape.
            redirect_url = f"https://app.thepyro.ai/app/{tenant_slug}"
        else:
            redirect_url = request.build_absolute_uri(f"/crm-records/records/{record.id}/")

        requester_name = str(data.get("requester_name") or "Requestor").strip()
        subject, text_body, html_body = build_new_request_unmannd_email(
            {
                "request_id": record.id,
                "tenant_name": getattr(tenant, "name", "Pyro"),
                "team_lead_name": team_lead_name,
                "requester_name": requester_name,
                "department": str(data.get("department") or "N/A").strip(),
                "item_name": str(data.get("item_name_freeform") or data.get("item_name") or "N/A").strip(),
                "quantity": str(data.get("quantity_required") or "N/A").strip(),
                "urgency": str(data.get("urgency_level") or "N/A").strip(),
                "status_text": str(data.get("status_text") or data.get("status") or "Request submitted").strip(),
                "redirect_url": redirect_url,
            }
        )

        success, msg = send_email(
            to_emails=team_lead_email,
            subject=subject,
            message=text_body,
            html_message=html_body,
            client_name="InventoryRequestNotification",
            fail_silently=True,
        )
        if not success:
            logger.warning(
                "Inventory request %s email notification failed for team_lead=%s: %s",
                record.id,
                team_lead_email,
                msg,
            )
        else:
            logger.info(
                "Inventory request %s email notification sent to %s (team_lead=%s).",
                record.id,
                team_lead_email,
                team_lead_value,
            )
    except Exception:
        logger.exception("Unexpected error while sending inventory request email notification for record=%s", getattr(record, "id", None))

from .helper import parse_numeric_lookup, coerce_numeric
from .assignee_display import build_assigned_to_search_q


class RecordListCreateView(TenantScopedMixin, generics.ListCreateAPIView):
    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    permission_classes = [IsTenantAuthenticated]
    pagination_class = MetaPageNumberPagination
    
    def get_queryset(self):
        """
        Filter records by tenant and support dynamic filtering on any field.
        Supports both direct model fields and JSON data fields.
        
        Query Parameters:
        - entity_type: Filter by entity type
        - lead_stage: Include only records with this lead_stage (e.g. SNOOZED for follow-up page). Comma-separated for multiple.
        - exclude_events: Exclude records with specified events or lead_stage values (comma-separated).
                         If lead_stage is also set, stages listed in lead_stage are never excluded (so follow-up page works).
                         Uppercase = exclude by lead_stage (e.g. TRIAL_ACTIVATED). Lowercase = exclude by EventLog event (e.g. trial_activated).
        - resolution_status, affiliated_party: Filter by data JSON (affiliated_party supports comma-separated values)
        - Any other field: Will be searched in the data JSON field
        - search + search_fields: When search_fields includes assigned_to, search matches raw data.assigned_to
          and leads whose assignee TenantMembership name/email contains the search term.
        
        Common usage (both pages):
        - My leads (exclude trial activated): ?entity_type=lead&assigned_to={{current_user}}&exclude_events=TRIAL_ACTIVATED
        - Follow-up leads (only SNOOZED):     ?entity_type=lead&assigned_to={{current_user}}&lead_stage=SNOOZED
        
        Examples:
        - ?entity_type=lead&assigned_to=user123&lead_stage=SNOOZED
        - ?entity_type=lead&assigned_to=user123&exclude_events=TRIAL_ACTIVATED
        - ?entity_type=lead&assigned_to=user123&exclude_events=TRIAL_ACTIVATED,NOT_INTERESTED
        """
        queryset = super().get_queryset()
        
        # Get all query parameters
        query_params = self.request.query_params
        
        # Filter by entity_type (direct model field)
        entity_type = query_params.get('entity_type')
        if entity_type:
            queryset = queryset.filter(entity_type=entity_type)
        
        # Dynamic filtering on data JSON field
        # Get all query params except known model fields
        model_fields = {'entity_type', 'search', 'search_fields', 'page', 'page_size', 'ordering', 'created_at__gte', 'created_at__lte', 'exclude_events'}
        data_filters = {k: v for k, v in query_params.items() if k not in model_fields}
        
        # Build Q objects for JSON field filtering
        # When listing "not connected" / retry leads (lead_stage=NOT_CONNECTED etc.), include unassigned leads too
        # so SELF TRIAL (assigned_to=null) appears alongside SALES LEAD (assigned_to=user)
        retry_stages = {'NOT_CONNECTED', 'CALL_BACK_LATER', 'IN_QUEUE'}
        lead_stage_val = (data_filters.get('lead_stage') or '').strip().upper()
        if ',' in lead_stage_val:
            lead_stage_vals = {v.strip().upper() for v in lead_stage_val.split(',') if v.strip()}
            include_unassigned_for_retry = bool(lead_stage_vals & retry_stages)
        else:
            include_unassigned_for_retry = entity_type == 'lead' and lead_stage_val in retry_stages
        assigned_to_val = data_filters.get('assigned_to')

        q_objects = Q()
        for field_name, field_value in data_filters.items():
            # For "not connected" list: assigned_to=user should also include unassigned (SELF TRIAL)
            if (
                field_name == 'assigned_to'
                and include_unassigned_for_retry
                and assigned_to_val
                and entity_type == 'lead'
            ):
                single_val = field_value.strip() if isinstance(field_value, str) else field_value
                field_q = (
                    Q(**{f'data__{field_name}': single_val})
                    | Q(**{f'data__{field_name}__isnull': True})
                    | Q(**{f'data__{field_name}': ''})
                    | Q(**{f'data__{field_name}': 'null'})
                    | Q(**{f'data__{field_name}': 'None'})
                )
                q_objects &= field_q
                continue
            # Numeric comparison lookups: total_price__gte=50000 -> data__total_price__gte with numeric 50000
            numeric_lookup = parse_numeric_lookup(field_name)
            if numeric_lookup:
                base_key, lookup_suffix = numeric_lookup
                num_val, ok = coerce_numeric(field_value)
                if ok and num_val is not None:
                    q_objects &= Q(**{f'data__{base_key}{lookup_suffix}': num_val})
                else:
                    # Invalid value for numeric lookup; treat as exact match on the full key (likely no match)
                    q_objects &= Q(**{f'data__{field_name}': field_value})
                continue
            # Support multiple values for the same field (comma-separated)
            if ',' in str(field_value):
                values = [v.strip() for v in str(field_value).split(',') if v.strip()]
                field_q = Q()
                for value in values:
                    field_q |= Q(**{f'data__{field_name}': value})
                q_objects &= field_q
            else:
                # Single value - exact match
                q_objects &= Q(**{f'data__{field_name}': field_value})
        
        if q_objects:
            queryset = queryset.filter(q_objects)
        
        # Support ordering
        ordering = query_params.get('ordering')
        if ordering:
            if ordering.startswith('-'):
                ord_field = ordering[1:]
                ord_prefix = '-'
            else:
                ord_field = ordering
                ord_prefix = ''

            if ord_field.startswith('data__'):
                queryset = queryset.order_by(f"{ord_prefix}{ord_field}")
            else:
                queryset = queryset.order_by(ordering)
        else:
            # Default ordering
            queryset = queryset.order_by('-created_at')

        # Enhanced search functionality
        search_term = query_params.get('search', '').strip()
        search_fields = query_params.get('search_fields', '').strip()
        
        if search_term:
            q_search = Q()
            
            if search_fields:
                # Search in specific fields provided in search_fields parameter
                field_list = [field.strip() for field in search_fields.split(',') if field.strip()]
                
                for field in field_list:
                    # Determine if it's a normal model field or a JSONB field
                    if field in ['entity_type', 'created_at', 'updated_at']:
                        # Normal model fields
                        q_search |= Q(**{f"{field}__icontains": search_term})
                    elif field == 'assigned_to':
                        q_search |= build_assigned_to_search_q(
                            getattr(self.request, "tenant", None),
                            search_term,
                        )
                    else:
                        # JSONB fields in data column (including name)
                        q_search |= Q(**{f"data__{field}__icontains": search_term})
            else:
                # Fallback: search across all available fields
                # Search in normal model fields
                # Note: name is now in data column, so it will be searched via data__name below
                
                # Search in JSONB fields - we'll search in common fields and any existing data
                # Get all unique keys from existing data to search in
                # Dynamically collect all unique JSON fields from existing records for this queryset
                from itertools import chain
                all_data_keys = set(chain.from_iterable(
                    record.data.keys() for record in queryset if isinstance(record.data, dict)
                ))
                common_json_fields = list(all_data_keys)
                # Always include 'name' in search since it's now in data column
                if 'name' not in common_json_fields:
                    common_json_fields.append('name')
                for field in common_json_fields:
                    q_search |= Q(**{f"data__{field}__icontains": search_term})
                
                # Also search for any field that might contain the search term
                # This is a more generic approach for unknown JSONB fields
                q_search |= Q(data__icontains=search_term)
            
            queryset = queryset.filter(q_search)
        
        # Exclude records with specified events or lead_stage values if requested via query parameter
        # When lead_stage is explicitly set (e.g. lead_stage=SNOOZED for follow-up page), we never exclude
        # that stage so both "my leads" and "follow-up leads" work with a common API.
        exclude_events_param = query_params.get('exclude_events', '').strip()
        # Get lead_stage from query params (may be in data_filters too, but we need it here for exclude logic)
        lead_stage_param = query_params.get('lead_stage', '')
        include_lead_stages = _parse_lead_stage_param(lead_stage_param)
        
        # Only apply exclude logic if exclude_events is provided
        if exclude_events_param and entity_type == 'lead':
            # Parse comma-separated event names or lead_stage values
            exclude_values = [e.strip() for e in exclude_events_param.split(',') if e.strip()]
            
            if exclude_values:
                # Build Q object to exclude records with any of the specified events or lead_stage values
                exclude_q = Q()
                for exclude_value in exclude_values:
                    # Do not exclude a stage that the user explicitly asked to include (e.g. lead_stage=SNOOZED)
                    if include_lead_stages:
                        exclude_upper = exclude_value.upper() if not exclude_value.isupper() else exclude_value
                        if exclude_upper in include_lead_stages:
                            logger.debug(f"[RecordListCreateView] Skipping exclude {exclude_value} (requested via lead_stage)")
                            continue
                    
                    # For leads, check both lead_stage field and events
                    # Uppercase values (like TRIAL_ACTIVATED) are treated as lead_stage values
                    # Lowercase values (like trial_activated) are treated as event names
                    if exclude_value.isupper():
                        logger.debug(f"[RecordListCreateView] Excluding leads with lead_stage={exclude_value}")
                        exclude_q |= Q(data__lead_stage=exclude_value)
                    else:
                        if '.' not in exclude_value:
                            full_event_name = f"{entity_type}.{exclude_value}"
                        else:
                            full_event_name = exclude_value
                        logger.debug(f"[RecordListCreateView] Excluding leads with event={full_event_name} or lead_stage={exclude_value.upper()}")
                        exclude_q |= Q(events__event=full_event_name)
                        exclude_q |= Q(data__lead_stage=exclude_value.upper())
                
                if exclude_q:
                    logger.info(f"[RecordListCreateView] Applying exclude filter for {len(exclude_values)} value(s): {exclude_values}")
                    queryset = queryset.exclude(exclude_q)
        elif exclude_events_param:
            # For non-lead entities, only check events
            exclude_values = [e.strip() for e in exclude_events_param.split(',') if e.strip()]
            if exclude_values:
                exclude_q = Q()
                for exclude_value in exclude_values:
                    if entity_type and '.' not in exclude_value:
                        full_event_name = f"{entity_type}.{exclude_value}"
                    else:
                        full_event_name = exclude_value
                    exclude_q |= Q(events__event=full_event_name)
                
                if exclude_q:
                    queryset = queryset.exclude(exclude_q)
            
        return queryset
    
    def perform_create(self, serializer):
        """
        Create record with tenant and entity_type assignment.
        entity_type can come from query params or request body.
        """
        # Get entity_type from query params or request data
        entity_type = self.request.query_params.get('entity_type')
        if not entity_type:
            entity_type = self.request.data.get('entity_type')
        
        if not entity_type:
            raise ValidationError({
                'entity_type': 'This field is required. Provide it in query params or request body.'
            })
        
        record = serializer.save(
            tenant=self.request.tenant,
            entity_type=entity_type
        )
        
        if entity_type == 'lead':
            try:
                from support_ticket.services import MixpanelService
                from background_jobs.queue_service import get_queue_service
                from background_jobs.models import JobType
                
                lead_data = record.data or {}
                user_id = lead_data.get('praja_id') or lead_data.get('user_id') or str(record.id)
                event_name = 'pyro_crm_lead_created'
                
                logger.info("=" * 80)
                logger.info(f"🚀 [Mixpanel] Creating lead {record.id}, sending to Mixpanel")
                logger.info(f"   Lead ID: {record.id}")
                logger.info(f"   Tenant: {record.tenant.name if record.tenant else 'None'} ({record.tenant.id if record.tenant else None})")
                logger.info(f"   User ID: {user_id} (from praja_id={lead_data.get('praja_id')} or user_id={lead_data.get('user_id')})")
                logger.info(f"   Event: {event_name}")
                logger.info(f"   Lead Name: {lead_data.get('name', 'N/A')}")
                logger.info(f"   Phone: {lead_data.get('phone_number', 'N/A')}")
                logger.info(f"   Lead Stage: {lead_data.get('lead_stage', 'N/A')}")
                logger.info(f"   Lead Score: {lead_data.get('lead_score', 'N/A')}")
                logger.info(f"   Properties Count: {len(lead_data) + 5}")  # +5 for base properties
                logger.info("=" * 80)
                
                properties = {
                    'lead_id': record.id,
                    'tenant_id': str(record.tenant.id) if record.tenant else None,
                    'entity_type': record.entity_type,
                    'created_at': record.created_at.isoformat() if record.created_at else None,
                    'updated_at': record.updated_at.isoformat() if record.updated_at else None,
                }
                properties.update(lead_data)
                if record.pyro_data:
                    properties.update(record.pyro_data)
                
                # Enqueue background job (single send; do not also send sync to avoid duplicate Mixpanel events)
                queue_service = get_queue_service()
                queue_service.enqueue_job(
                    job_type=JobType.SEND_MIXPANEL_EVENT,
                    payload={
                        "user_id": str(user_id),
                        "event_name": event_name,
                        "properties": properties
                    },
                    priority=0,
                    tenant_id=str(record.tenant.id) if record.tenant else None,
                    max_attempts=3
                )
            except Exception as e:
                logger.error(f"❌ [Mixpanel] Error sending lead {record.id}: {e}")

        if entity_type in {"inventory_request", "unmannd_request"}:
            _notify_team_lead_for_inventory_request(self.request, record)
    
    def put(self, request, *args, **kwargs):
        """
        Update an existing record by record_id.
        
        Record ID can be provided in:
        1. URL path: /crm-records/records/123 (if URL is configured with <int:pk>)
        2. Query parameter: /crm-records/records/?record_id=123
        3. Request body: {"record_id": 123, ...}
        
        Expected payload:
        {
            "record_id": 123,  // optional if provided in URL/query
            "name": "Updated Name",
            "data": {"updated": "fields"},
            "entity_type": "lead"  // optional
        }
        """
        # Try to get record_id from multiple sources
        record_id = None
        
        # 1. Try URL parameter (if configured as /records/<int:pk>/)
        if 'pk' in kwargs:
            record_id = kwargs['pk']
        
        # 2. Try query parameter
        if not record_id:
            record_id = request.query_params.get('record_id')
        
        # 3. Try request body
        if not record_id:
            record_id = request.data.get('record_id')
        
        if not record_id:
            return Response(
                {'error': 'record_id is required for updates. Provide it in URL, query parameter, or request body.'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Get the record within tenant scope
            record = self.get_queryset().get(id=record_id)
        except Record.DoesNotExist:
            return Response(
                {'error': f'Record with id {record_id} not found'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Update the record
        serializer = self.get_serializer(record, data=request.data, partial=False)
        serializer.is_valid(raise_exception=True)
        
        # Preserve tenant (don't allow changing tenant)
        serializer.save(tenant=self.request.tenant)
        
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    def patch(self, request, *args, **kwargs):
        """
        Partially update an existing record by record_id.
        
        Record ID can be provided in:
        1. URL path: /crm-records/records/123 (if URL is configured with <int:pk>)
        2. Query parameter: /crm-records/records/?record_id=123
        3. Request body: {"record_id": 123, ...}
        
        Expected payload:
        {
            "record_id": 123,  // optional if provided in URL/query
            "name": "Updated Name",  // partial update - only include fields to update
            "data": {"updated": "fields"},
            "entity_type": "lead"  // optional
        }
        """
        # Try to get record_id from multiple sources
        record_id = None
        
        # 1. Try URL parameter (if configured as /records/<int:pk>/)
        if 'pk' in kwargs:
            record_id = kwargs['pk']
        
        # 2. Try query parameter
        if not record_id:
            record_id = request.query_params.get('record_id')
        
        # 3. Try request body
        if not record_id:
            record_id = request.data.get('record_id')
        
        if not record_id:
            return Response(
                {'error': 'record_id is required for updates. Provide it in URL, query parameter, or request body.'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Get the record within tenant scope
            record = self.get_queryset().get(id=record_id)
        except Record.DoesNotExist:
            return Response(
                {'error': f'Record with id {record_id} not found'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Partially update the record
        serializer = self.get_serializer(record, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        
        # Preserve tenant (don't allow changing tenant)
        serializer.save(tenant=self.request.tenant)
        
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    def delete(self, request, *args, **kwargs):
        """
        Delete an existing record by record_id from URL path.
        
        URL: /crm-records/records/538/
        The record ID (538) comes from the URL path parameter.
        """
        # Get record_id from URL path parameter
        record_id = kwargs.get('pk')
        
        if not record_id:
            return Response(
                {'error': 'Record ID is required in URL path'}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Get the record within tenant scope
            record = self.get_queryset().get(id=record_id)
        except Record.DoesNotExist:
            return Response(
                {'error': f'Record with id {record_id} not found'}, 
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Delete the record
        record_data = {
            'id': record.id,
            'name': (record.data or {}).get('name', '') if isinstance(record.data, dict) else '',
            'entity_type': record.entity_type,
            'tenant_id': str(record.tenant_id)
        }
        
        record.delete()
        
        return Response({
            'success': True,
            'message': f'Record {record_id} deleted successfully',
            'deleted_record': record_data
        }, status=status.HTTP_200_OK)


class RecordDetailView(TenantScopedMixin, generics.RetrieveUpdateAPIView):
    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    permission_classes = [IsTenantAuthenticated]

    def get_object(self):
        """
        Prefer record_id from request body; fall back to URL pk for compatibility.
        """
        record_id = self.request.data.get('record_id')
        if record_id is not None:
            try:
                record_id_int = int(record_id)
            except (TypeError, ValueError):
                raise ValidationError({'record_id': 'Must be an integer.'})

            try:
                # Always query database directly, bypassing any queryset cache
                record = Record.objects.filter(
                    id=record_id_int,
                    tenant=self.request.tenant
                ).first()
                if not record:
                    raise NotFound('Record not found or access denied')
                # Force refresh to ensure latest data from DB
                record.refresh_from_db()
                return record
            except Record.DoesNotExist:
                raise NotFound('Record not found or access denied')

        # Fallback: use default URL kwarg (pk) - also ensure fresh DB query
        obj = super().get_object()
        obj.refresh_from_db()
        return obj
    
    def perform_update(self, serializer):
        """
        Update record. "Not connected" logic is handled by the rule engine when lead_stage is set to "NOT_CONNECTED".
        """
        serializer.save()

    def delete(self, request, *args, **kwargs):
        """
        Delete the record identified by URL pk (e.g. DELETE /crm-records/records/2642/).
        """
        record = self.get_object()
        record_data = {
            'id': record.id,
            'name': (record.data or {}).get('name', '') if isinstance(record.data, dict) else '',
            'entity_type': record.entity_type,
            'tenant_id': str(record.tenant_id)
        }
        record.delete()
        return Response({
            'success': True,
            'message': f'Record {record.id} deleted successfully',
            'deleted_record': record_data
        }, status=status.HTTP_200_OK)


class EntityProxyView(TenantScopedMixin, generics.ListCreateAPIView):
    """
    Proxy view for entity-specific endpoints (e.g., /leads/, /tickets/).
    Provides friendly URLs while reusing the same Record logic.
    """
    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    permission_classes = [IsTenantAuthenticated]
    pagination_class = MetaPageNumberPagination
    entity_type = None  # Set this in URL configuration
    
    def get_queryset(self):
        """Filter by tenant and the specific entity type."""
        queryset = super().get_queryset()
        if self.entity_type:
            queryset = queryset.filter(entity_type=self.entity_type)
        return queryset
    
    def perform_create(self, serializer):
        """Create record for this entity type (e.g. lead)."""
        record = serializer.save(
            tenant=self.request.tenant,
            entity_type=self.entity_type
        )
        
        if self.entity_type == 'lead':
            try:
                from support_ticket.services import MixpanelService
                from background_jobs.queue_service import get_queue_service
                from background_jobs.models import JobType
                
                lead_data = record.data or {}
                user_id = lead_data.get('praja_id') or lead_data.get('user_id') or str(record.id)
                event_name = 'pyro_crm_lead_created'
                
                logger.info("=" * 80)
                logger.info(f"🚀 [Mixpanel] Creating lead {record.id} via EntityProxyView, sending to Mixpanel")
                logger.info(f"   Lead ID: {record.id}")
                logger.info(f"   Tenant: {record.tenant.name if record.tenant else 'None'} ({record.tenant.id if record.tenant else None})")
                logger.info(f"   User ID: {user_id} (from praja_id={lead_data.get('praja_id')} or user_id={lead_data.get('user_id')})")
                logger.info(f"   Event: {event_name}")
                logger.info(f"   Lead Name: {lead_data.get('name', 'N/A')}")
                logger.info(f"   Phone: {lead_data.get('phone_number', 'N/A')}")
                logger.info(f"   Lead Stage: {lead_data.get('lead_stage', 'N/A')}")
                logger.info(f"   Lead Score: {lead_data.get('lead_score', 'N/A')}")
                logger.info("=" * 80)
                
                properties = {
                    'lead_id': record.id,
                    'tenant_id': str(record.tenant.id) if record.tenant else None,
                    'entity_type': record.entity_type,
                    'created_at': record.created_at.isoformat() if record.created_at else None,
                    'updated_at': record.updated_at.isoformat() if record.updated_at else None,
                }
                properties.update(lead_data)
                if record.pyro_data:
                    properties.update(record.pyro_data)
                
                # Enqueue background job (single send; do not also send sync to avoid duplicate Mixpanel events)
                queue_service = get_queue_service()
                job = queue_service.enqueue_job(
                    job_type=JobType.SEND_MIXPANEL_EVENT,
                    payload={
                        "user_id": str(user_id),
                        "event_name": event_name,
                        "properties": properties
                    },
                    priority=0,
                    tenant_id=str(record.tenant.id) if record.tenant else None,
                    max_attempts=3
                )
            except Exception as e:
                logger.error(f"❌ [Mixpanel] Error sending lead {record.id}: {e}")

        if self.entity_type in {"inventory_request", "unmannd_request"}:
            _notify_team_lead_for_inventory_request(self.request, record)


class RecordEventView(TenantScopedMixin, APIView):
    """
    Handle event creation and logging for records.
    POST /records/<id>/events/ - Log an event for a specific record.
    """
    permission_classes = [IsTenantAuthenticated]

    @extend_schema(
        summary="Log event for record",
        description="Logs an event in the EventLog and dispatches it for processing. "
                   "This endpoint is used to track user actions and system events for records.",
        request={
            'application/json': {
                'type': 'object',
                'properties': {
                    'event': {
                        'type': 'string',
                        'description': 'Name of the event (e.g., button_click, win_clicked, lost_clicked)',
                        'example': 'button_click'
                    },
                    'payload': {
                        'type': 'object',
                        'description': 'Additional data associated with the event',
                        'example': {
                            'button_type': 'call_later',
                            'user_id': 'user123',
                            'timestamp': '2025-01-01T00:00:00Z'
                        }
                    }
                },
                'required': ['event']
            }
        },
        responses={
            200: OpenApiResponse(
                description="Event logged successfully",
                examples=[
                    OpenApiExample(
                        name="Success Response",
                        value={
                            "ok": True,
                            "logged": True,
                            "event_id": 123,
                            "message": "Event 'button_click' logged successfully"
                        }
                    )
                ]
            ),
            400: OpenApiResponse(
                description="Bad request - missing or invalid data",
                examples=[
                    OpenApiExample(
                        name="Missing Event",
                        value={"error": "Event name is required"}
                    ),
                    OpenApiExample(
                        name="Invalid Payload",
                        value={"error": "Payload must be a valid JSON object"}
                    )
                ]
            ),
            404: OpenApiResponse(
                description="Record not found or access denied",
                examples=[
                    OpenApiExample(
                        name="Record Not Found",
                        value={"error": "Record not found or access denied"}
                    )
                ]
            ),
            500: OpenApiResponse(
                description="Internal server error",
                examples=[
                    OpenApiExample(
                        name="Server Error",
                        value={"error": "Failed to log event: Database connection failed"}
                    )
                ]
            )
        },
        tags=["Events"]
    )
    def post(self, request, pk=None):
        """
        Log an event for a specific record.
        Validates record exists and belongs to tenant, then creates EventLog entry.
        """
        tenant = getattr(request, 'tenant', None)
        record_id = request.data.get("record_id")

        logger.info(
            "[EventAPI] Incoming event POST: record_id=%s tenant_id=%s user=%s headers=%s",
            record_id,
            getattr(tenant, 'id', None),
            getattr(getattr(request, 'user', None), 'supabase_uid', None),
            {k: v for k, v in request.headers.items() if k.startswith('X-') or k == 'Authorization'}
        )
        # Find the record, ensuring it belongs to the current tenant
        try:
            record = Record.objects.get(id=record_id, tenant=tenant)
            logger.debug("[EventAPI] Found record id=%s entity_type=%s", record.id, record.entity_type)
        except Record.DoesNotExist:
            logger.warning(
                "[EventAPI] Record not found or access denied: record_id=%s tenant_id=%s",
                record_id,
                getattr(tenant, 'id', None)
            )
            return Response(
                {"error": "Record not found or access denied"}, 
                status=status.HTTP_404_NOT_FOUND
            )

        # Extract event data from request
        event_name = request.data.get("event")
        payload = request.data.get("payload", {})
        logger.info("[EventAPI] Parsed event request: event=%s payload_keys=%s", event_name, list(payload.keys()) if isinstance(payload, dict) else type(payload))

        # Validate required fields
        if not event_name:
            logger.error("[EventAPI] Missing event name in request body for record_id=%s", record_id)
            return Response(
                {"error": "Event name is required"}, 
                status=status.HTTP_400_BAD_REQUEST
            )

        # Normalize payload: allow JSON string payloads and parse them to dict
        if isinstance(payload, str):
            try:
                import json
                payload = json.loads(payload) if payload.strip() else {}
            except Exception as e:
                logger.error("[EventAPI] Could not parse payload JSON string for record_id=%s: %s", record_id, e)
                return Response(
                    {"error": "Payload must be a valid JSON object"},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # Validate payload is a dictionary
        if not isinstance(payload, dict):
            logger.error("[EventAPI] Invalid payload type for record_id=%s: type=%s", record_id, type(payload))
            return Response(
                {"error": "Payload must be a valid JSON object"}, 
                status=status.HTTP_400_BAD_REQUEST
            )

        # Create the event log entry
        try:
            with transaction.atomic():
                # Event processing is handled by the rule engine via dispatch_event
                # No hardcoded logic here - rules handle "not connected" events
                
                event_log = EventLog.objects.create(
                    record=record,
                    tenant=request.tenant,
                    event=event_name,
                    payload=payload,
                    timestamp=timezone.now()
                )
            
            # Log the event creation
            logger.info(
                "[EventAPI] Logged event id=%s name=%s for record_id=%s tenant_id=%s",
                event_log.id,
                event_name,
                record.id,
                getattr(request.tenant, 'id', None)
            )
            
            # Dispatch the event for processing
            dispatch_success = dispatch_event(event_name, record, payload)
            
            if not dispatch_success:
                logger.warning("[EventAPI] Event dispatch returned False for event=%s record_id=%s", event_name, record.id)
            
            return Response({
                "ok": True, 
                "logged": True,
                "event_id": event_log.id,
                "message": f"Event '{event_name}' logged successfully"
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.exception("[EventAPI] Failed to log or dispatch event for record_id=%s: %s", record_id, e)
            return Response(
                {"error": f"Failed to log event: {str(e)}"}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class EventLogListView(TenantScopedMixin, generics.ListAPIView):
    """
    Admin-only view for listing all logged events for a tenant.
    Allows debugging and auditing of event system.
    """
    serializer_class = EventLogSerializer
    permission_classes = [IsTenantAuthenticated]
    pagination_class = MetaPageNumberPagination

    @extend_schema(
        summary="List all events for tenant",
        description="Retrieves a paginated list of all events logged for the current tenant. "
                   "Supports filtering by record ID, event name, user_supabase_uid, and date range. Includes summary statistics.",
        parameters=[
            {
                'name': 'record',
                'in': 'query',
                'description': 'Filter events by record ID',
                'required': False,
                'schema': {'type': 'integer'},
                'example': 18
            },
            {
                'name': 'event',
                'in': 'query',
                'description': 'Filter events by event name',
                'required': False,
                'schema': {'type': 'string'},
                'example': 'button_click'
            },
            {
                'name': 'user_supabase_uid',
                'in': 'query',
                'description': 'Filter events by user_supabase_uid (from payload)',
                'required': False,
                'schema': {'type': 'string'},
                'example': '22c38153-4029-4332-9849-747871332449'
            },
            {
                'name': 'timestamp__gte',
                'in': 'query',
                'description': 'Filter events with timestamp greater than or equal to this date (ISO format)',
                'required': False,
                'schema': {'type': 'string', 'format': 'date-time'},
                'example': '2025-12-16T00:00:00Z'
            },
            {
                'name': 'timestamp__lte',
                'in': 'query',
                'description': 'Filter events with timestamp less than or equal to this date (ISO format)',
                'required': False,
                'schema': {'type': 'string', 'format': 'date-time'},
                'example': '2025-12-16T23:59:59Z'
            }
        ],
        responses={
            200: OpenApiResponse(
                description="List of events with summary statistics",
                examples=[
                    OpenApiExample(
                        name="Success Response",
                        value={
                            "data": [
                                {
                                    "id": 123,
                                    "record_id": 18,
                                    "tenant_id": "e35e7279-d92d-4cdf-8014-98deaab639c0",
                                    "event": "button_click",
                                    "payload": {
                                        "button_type": "call_later",
                                        "user_id": "user123"
                                    },
                                    "timestamp": "2025-01-01T00:00:00Z"
                                }
                            ],
                            "page_meta": {
                                "count": 14,
                                "next": None,
                                "previous": None,
                                "page_size": 20
                            },
                            "summary": {
                                "total_events": 14,
                                "event_counts": {
                                    "button_click": 2,
                                    "win_clicked": 3,
                                    "lost_clicked": 3
                                },
                                "filters": {
                                    "record": "18",
                                    "event": "button_click"
                                }
                            }
                        }
                    )
                ]
            ),
            403: OpenApiResponse(
                description="Authentication required",
                examples=[
                    OpenApiExample(
                        name="Authentication Error",
                        value={"detail": "Authentication credentials were not provided."}
                    )
                ]
            )
        },
        tags=["Events", "Admin"]
    )
    def list(self, request, *args, **kwargs):
        """
        Override list to add summary statistics.
        """
        response = super().list(request, *args, **kwargs)
        
        # Add summary statistics to response
        queryset = self.get_queryset()
        
        # Get event counts by type
        event_counts = {}
        for event_log in queryset:
            event_name = event_log.event
            event_counts[event_name] = event_counts.get(event_name, 0) + 1
        
        # Add summary to response
        if hasattr(response, 'data'):
            response.data['summary'] = {
                'total_events': queryset.count(),
                'event_counts': event_counts,
                'filters': {
                    'record': request.query_params.get('record'),
                    'event': request.query_params.get('event')
                }
            }
        
        return response
    
    def get_queryset(self):
        """
        Get events for the current tenant, with optional filtering.
        Supports filtering by:
        - record: record ID
        - event: event name
        - user_supabase_uid: user ID from payload (filters payload__user_supabase_uid)
        - timestamp__gte: timestamp greater than or equal
        - timestamp__lte: timestamp less than or equal
        """
        queryset = EventLog.objects.filter(tenant=self.request.tenant)
        
        # Optional filtering by record ID
        record_id = self.request.query_params.get('record')
        if record_id:
            queryset = queryset.filter(record_id=record_id)
        
        # Optional filtering by event name
        event_name = self.request.query_params.get('event')
        if event_name:
            queryset = queryset.filter(event=event_name)
        
        # Optional filtering by user_supabase_uid from payload
        user_supabase_uid = self.request.query_params.get('user_supabase_uid')
        if user_supabase_uid:
            queryset = queryset.filter(payload__user_supabase_uid=user_supabase_uid)
        
        # Optional filtering by date range
        timestamp_gte = self.request.query_params.get('timestamp__gte')
        if timestamp_gte:
            try:
                from django.utils.dateparse import parse_datetime
                dt = parse_datetime(timestamp_gte)
                if dt:
                    queryset = queryset.filter(timestamp__gte=dt)
            except (ValueError, TypeError):
                pass  # Ignore invalid date format
        
        timestamp_lte = self.request.query_params.get('timestamp__lte')
        if timestamp_lte:
            try:
                from django.utils.dateparse import parse_datetime
                dt = parse_datetime(timestamp_lte)
                if dt:
                    queryset = queryset.filter(timestamp__lte=dt)
            except (ValueError, TypeError):
                pass  # Ignore invalid date format
        
        # Order by most recent first
        return queryset.order_by('-timestamp')


class EventLogCountView(TenantScopedMixin, APIView):
    """
    Get count of events matching filters.
    More efficient than fetching all events just to count them.
    """
    permission_classes = [IsTenantAuthenticated]

    @extend_schema(
        summary="Get event count",
        description="Returns the count of events matching the provided filters. "
                   "Supports filtering by event name, user_supabase_uid, and date range.",
        parameters=[
            {
                'name': 'event',
                'in': 'query',
                'description': 'Filter events by event name',
                'required': False,
                'schema': {'type': 'string'},
                'example': 'lead.trial_activated'
            },
            {
                'name': 'user_supabase_uid',
                'in': 'query',
                'description': 'Filter events by user_supabase_uid (from payload)',
                'required': False,
                'schema': {'type': 'string'},
                'example': '22c38153-4029-4332-9849-747871332449'
            },
            {
                'name': 'timestamp__gte',
                'in': 'query',
                'description': 'Filter events with timestamp greater than or equal to this date (ISO format)',
                'required': False,
                'schema': {'type': 'string', 'format': 'date-time'},
                'example': '2025-12-16T00:00:00Z'
            },
            {
                'name': 'timestamp__lte',
                'in': 'query',
                'description': 'Filter events with timestamp less than or equal to this date (ISO format)',
                'required': False,
                'schema': {'type': 'string', 'format': 'date-time'},
                'example': '2025-12-16T23:59:59Z'
            }
        ],
        responses={
            200: OpenApiResponse(
                description="Event count",
                examples=[
                    OpenApiExample(
                        name="Count Response",
                        value={
                            "count": 42
                        }
                    )
                ]
            )
        },
        tags=["Events", "Admin"]
    )
    def get(self, request):
        """
        Get count of events matching the filters.
        Uses the same filtering logic as EventLogListView but only returns count.
        """
        queryset = EventLog.objects.filter(tenant=request.tenant)
        
        # Optional filtering by event name
        event_name = request.query_params.get('event')
        if event_name:
            queryset = queryset.filter(event=event_name)
        
        # Optional filtering by user_supabase_uid from payload
        user_supabase_uid = request.query_params.get('user_supabase_uid')
        if user_supabase_uid:
            queryset = queryset.filter(payload__user_supabase_uid=user_supabase_uid)
        
        # Optional filtering by date range
        timestamp_gte = request.query_params.get('timestamp__gte')
        if timestamp_gte:
            try:
                from django.utils.dateparse import parse_datetime
                dt = parse_datetime(timestamp_gte)
                if dt:
                    queryset = queryset.filter(timestamp__gte=dt)
            except (ValueError, TypeError):
                pass  # Ignore invalid date format
        
        timestamp_lte = request.query_params.get('timestamp__lte')
        if timestamp_lte:
            try:
                from django.utils.dateparse import parse_datetime
                dt = parse_datetime(timestamp_lte)
                if dt:
                    queryset = queryset.filter(timestamp__lte=dt)
            except (ValueError, TypeError):
                pass  # Ignore invalid date format
        
        count = queryset.count()
        
        return Response({
            "count": count
        }, status=status.HTTP_200_OK)


class LeadStatsView(APIView):
    """
    Get lead statistics for the current tenant's CRM records.
    """
    permission_classes = [IsTenantAuthenticated]
    
    @extend_schema(
        summary="Get lead statistics",
        description="Returns statistics about leads for the current tenant, including counts by status.",
        responses={
            200: OpenApiResponse(
                description="Lead statistics",
                examples=[
                    OpenApiExample(
                        name="Stats Response",
                        value={
                            "total_leads": 100,
                            "fresh": 30,
                            "in_queue": 27,
                            "assigned": 26,
                            "snoozed": 10,
                            "not_connected": 3,
                            "closed": 4
                        }
                    )
                ]
            )
        },
        tags=["Leads", "Statistics"]
    )
    def get(self, request):
        """Get statistics about leads for the current tenant."""
        tenant = request.tenant
        
        if not tenant:
            return Response({
                "total_leads": 0,
                "fresh": 0,
                "in_queue": 0,
                "assigned": 0,
                "snoozed": 0,
                "not_connected": 0,
                "closed": 0
            }, status=status.HTTP_200_OK)
        
        # Get all leads for this tenant - use tenant_id for better index usage
        leads_qs = Record.objects.filter(tenant_id=tenant.id, entity_type='lead')
        
        # Use database aggregation instead of Python loops for performance
        # Count total leads
        total_leads = leads_qs.count()
        
        # Count by lead_stage using JSONB field aggregation (lead_stage stored in CAPITAL)
        # Stages: FRESH, IN_QUEUE, ASSIGNED, SNOOZED, NOT_CONNECTED, CLOSED (no WON, LOST, SCHEDULED, CALL_LATER)
        stage_counts = leads_qs.aggregate(
            fresh=Count('id', filter=Q(data__lead_stage='FRESH')),
            in_queue=Count('id', filter=Q(data__lead_stage='IN_QUEUE')),
            assigned=Count('id', filter=Q(data__lead_stage='ASSIGNED')),
            snoozed=Count('id', filter=Q(data__lead_stage='SNOOZED')),
            not_connected=Count('id', filter=Q(data__lead_stage='NOT_CONNECTED')),
            closed=Count('id', filter=Q(data__lead_stage='CLOSED')),
        )
        
        stats = {
            "total_leads": total_leads,
            "fresh": stage_counts.get('fresh', 0) or 0,
            "in_queue": stage_counts.get('in_queue', 0) or 0,
            "assigned": stage_counts.get('assigned', 0) or 0,
            "snoozed": stage_counts.get('snoozed', 0) or 0,
            "not_connected": stage_counts.get('not_connected', 0) or 0,
            "closed": stage_counts.get('closed', 0) or 0,
        }
        
        return Response(stats, status=status.HTTP_200_OK)


class GetNextLeadView(APIView):
    """
    Get and assign the next available lead from the queue for CRM records.
    Atomically fetches and assigns the highest-scoring unassigned lead to the caller.
    """
    permission_classes = [IsTenantAuthenticated]

    # Enforce next_call_at cooldown: fresh (0 attempts) always eligible; retry (1+ attempts) only when next_call_at is set and past.
    _NEXT_CALL_READY_WHERE = """
        (
            COALESCE((data->>'call_attempts')::int, 0) = 0
            OR (
                (data->>'next_call_at') IS NOT NULL
                AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
                AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
                AND (data->>'next_call_at')::timestamptz <= NOW()
            )
        )
    """

    # Fresh leads (for RM assignment) are those with lead_stage = FRESH only.
    QUEUEABLE_STATUSES = ('FRESH','IN_QUEUE')
    ASSIGNED_STATUS = 'ASSIGNED'

    # LIFO for get-next-lead: most recent subscription first (same guards as next_call_at-style JSON timestamps).
    _SUBSCRIPTION_TIME_STAMP_SORT_SQL = """
        CASE
            WHEN (data->>'subscription_time_stamp') IS NOT NULL
                AND TRIM(COALESCE(data->>'subscription_time_stamp', '')) != ''
                AND LOWER(TRIM(COALESCE(data->>'subscription_time_stamp', ''))) NOT IN ('null', 'none')
            THEN (data->>'subscription_time_stamp')::timestamptz
            ELSE NULL
        END
    """
    
    def _get_call_attempt_matrix(self, tenant, lead_type: str):
        """
        Get call attempt matrix configuration for a lead type.
        Returns None if not found (will use default behavior).
        """
        try:
            matrix = CallAttemptMatrix.objects.get(
                tenant=tenant,
                lead_type=lead_type
            )
            return matrix
        except CallAttemptMatrix.DoesNotExist:
            return None
    
    def _should_exclude_lead_by_matrix(self, record, lead_data: dict, matrix: CallAttemptMatrix, now):
        """
        Check if a lead should be excluded based on call attempt matrix rules.
        Returns (should_exclude: bool, reason: str)
        """
        if not matrix:
            return False, None
        
        # Check max call attempts
        call_attempts = lead_data.get('call_attempts', 0)
        try:
            call_attempts_int = int(call_attempts) if call_attempts is not None else 0
        except (TypeError, ValueError):
            call_attempts_int = 0
        
        if call_attempts_int >= matrix.max_call_attempts:
            return True, f"Max call attempts ({matrix.max_call_attempts}) reached"
        
        # Check SLA (days since record creation)
        if record and record.created_at:
            days_since_creation = (now - record.created_at).days
            if days_since_creation > matrix.sla_days:
                return True, f"SLA ({matrix.sla_days} days) exceeded"
        
        # Check minimum time between calls
        next_call_at_str = lead_data.get('next_call_at')
        if next_call_at_str and call_attempts_int > 0:
            try:
                # Try parsing with dateutil first, fallback to datetime
                if date_parser:
                    try:
                        next_call_at = date_parser.parse(next_call_at_str)
                    except:
                        next_call_at = datetime.fromisoformat(next_call_at_str.replace('Z', '+00:00'))
                else:
                    # Fallback to datetime.fromisoformat
                    next_call_at = datetime.fromisoformat(next_call_at_str.replace('Z', '+00:00'))
                
                # Ensure both comparable: rule engine stores UTC; treat naive next_call_at as UTC
                if now.tzinfo is None and next_call_at.tzinfo is not None:
                    next_call_at = next_call_at.replace(tzinfo=None)
                elif now.tzinfo is not None and next_call_at.tzinfo is None:
                    # Rule engine stores timezone.now() (UTC); treat naive next_call_at as UTC
                    next_call_at = next_call_at.replace(tzinfo=std_utc.utc)
                
                hours_since_last_call = (now - next_call_at).total_seconds() / 3600
                if hours_since_last_call < matrix.min_time_between_calls_hours:
                    return True, f"Minimum time between calls ({matrix.min_time_between_calls_hours} hours) not met"
            except Exception as e:
                logger.debug(f"[GetNextLead] Error parsing next_call_at for min time check: {e}")
        
        return False, None
    
    def _lead_is_due_for_call(self, lead_data: dict, now) -> bool:
        """Return True if lead is eligible to be called now (cooldown respected). Fresh (0 attempts) always due; retry only when next_call_at <= now."""
        if not isinstance(lead_data, dict):
            return True
        try:
            call_attempts_int = int(lead_data.get('call_attempts') or 0)
        except (TypeError, ValueError):
            call_attempts_int = 0
        if call_attempts_int == 0:
            return True
        raw = lead_data.get('next_call_at')
        if raw is None or raw == '' or raw == 'null':
            return False
        try:
            if isinstance(raw, datetime):
                next_call_at = raw
            elif date_parser:
                next_call_at = date_parser.parse(str(raw))
            else:
                next_call_at = datetime.fromisoformat(str(raw).replace('Z', '+00:00'))
            if now.tzinfo is None and next_call_at.tzinfo:
                next_call_at = next_call_at.replace(tzinfo=None)
            elif now.tzinfo and next_call_at.tzinfo is None:
                next_call_at = next_call_at.replace(tzinfo=std_utc.utc)
            return next_call_at <= now
        except Exception:
            return False

    def _not_connected_retry_response(
        self,
        *,
        tenant,
        user,
        tenant_membership,
        user_identifier,
        user_uuid,
        eligible_lead_types,
        eligible_lead_sources,
        eligible_lead_statuses,
        eligible_states,
        log_label: str,
    ):
        """
        When the main queue has no suitable lead: due NOT_CONNECTED/IN_QUEUE retries — assigned-to-me first,
        else unassigned (filters + routing), lock-assign, then same JSON shape as a normal assign.
        Returns Response 200 or None.
        """
        due_nc = """
            COALESCE((data->>'call_attempts')::int, 0) BETWEEN 1 AND 6
            AND UPPER(COALESCE(data->>'lead_stage','')) IN ('NOT_CONNECTED', 'IN_QUEUE')
            AND (data->>'next_call_at') IS NOT NULL AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
            AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
            AND (data->>'next_call_at')::timestamptz <= NOW()
        """
        retry_candidate = (
            Record.objects.filter(tenant=tenant, entity_type="lead", data__assigned_to=user_identifier)
            .extra(select={"call_attempts_int": "COALESCE((data->>'call_attempts')::int, 0)"}, where=[due_nc])
            .order_by("call_attempts_int", "updated_at", "id")
            .first()
        )
        if not retry_candidate:
            unassigned_where = f"""
                (
                    (data->>'assigned_to') IS NULL
                    OR TRIM(COALESCE(data->>'assigned_to', '')) = ''
                    OR LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) IN ('null', 'none')
                )
                AND {due_nc}
            """
            qs = Record.objects.filter(tenant=tenant, entity_type="lead").extra(
                select={"call_attempts_int": "COALESCE((data->>'call_attempts')::int, 0)"},
                where=[unassigned_where],
            )
            if eligible_lead_types:
                qs = qs.filter(data__affiliated_party__in=eligible_lead_types)
            if eligible_lead_sources:
                qs = qs.filter(data__lead_source__in=eligible_lead_sources)
            if eligible_lead_statuses:
                qs = qs.filter(data__lead_status__in=eligible_lead_statuses)
            if eligible_states:
                qs = qs.filter(data__state__in=eligible_states)
            picked = qs.order_by("call_attempts_int", "updated_at", "id").first()
            if not picked:
                return None
            with transaction.atomic():
                locked = Record.objects.select_for_update(skip_locked=True).filter(pk=picked.pk).first()
                if not locked:
                    return None
                data = (locked.data or {}).copy()
                if not _legacy_get_next_lead_assignee_is_unassigned(data.get("assigned_to")):
                    logger.info(
                        "%s Unassigned NC retry lost race record_id=%s assigned_to=%s user=%s",
                        log_label,
                        locked.pk,
                        data.get("assigned_to"),
                        user_identifier,
                    )
                    return None
                data["assigned_to"] = user_identifier
                data["lead_stage"] = self.ASSIGNED_STATUS
                if "call_attempts" not in data or data.get("call_attempts") in (None, "", "null"):
                    data["call_attempts"] = 0
                locked.data = data
                locked.updated_at = timezone.now()
                locked.save(update_fields=["data", "updated_at"])
                retry_candidate = locked

        serialized_data = RecordSerializer(retry_candidate).data
        lead_data = retry_candidate.data or {}
        body = {
            "id": retry_candidate.id,
            "name": (retry_candidate.data or {}).get("name", "") if isinstance(retry_candidate.data, dict) else "",
            "phone_no": lead_data.get("phone_number", ""),
            "praja_id": lead_data.get("praja_id"),
            "lead_status": lead_data.get("lead_stage") or "",
            "lead_score": lead_data.get("lead_score"),
            "lead_type": lead_data.get("affiliated_party") or lead_data.get("poster"),
            "assigned_to": lead_data.get("assigned_to"),
            "attempt_count": lead_data.get("call_attempts", 0),
            "last_call_outcome": lead_data.get("last_call_outcome"),
            "next_call_at": lead_data.get("next_call_at"),
            "do_not_call": lead_data.get("do_not_call", False),
            "resolved_at": lead_data.get("closure_time"),
            "premium_poster_count": lead_data.get("premium_poster_count"),
            "package_to_pitch": lead_data.get("package_to_pitch"),
            "last_active_date_time": lead_data.get("last_active_date_time"),
            "latest_remarks": lead_data.get("latest_remarks"),
            "lead_description": lead_data.get("lead_description"),
            "affiliated_party": lead_data.get("affiliated_party"),
            "rm_dashboard": lead_data.get("rm_dashboard"),
            "user_profile_link": lead_data.get("user_profile_link"),
            "whatsapp_link": lead_data.get("whatsapp_link"),
            "lead_source": lead_data.get("lead_source"),
            "created_at": serialized_data.get("created_at"),
            "updated_at": serialized_data.get("updated_at"),
            "data": lead_data,
            "record": serialized_data,
        }
        retry_candidate.refresh_from_db()
        PostAssignmentActions().run(
            record=retry_candidate,
            tenant=tenant,
            user=user,
            tenant_membership=tenant_membership,
            user_identifier=user_identifier,
            user_uuid=user_uuid,
            lead_data=retry_candidate.data or {},
        )
        ac = body.get("attempt_count", 0)
        logger.info(
            "%s Returning not-connected retry lead record_id=%s user=%s call_attempts=%s (attempt %s of max 6).",
            log_label,
            retry_candidate.id,
            user_identifier,
            ac,
            ac,
        )
        return Response(body, status=status.HTTP_200_OK)

    def _order_by_score(self, qs, now_iso=None):
        """
        Order queryset with priority:
        1. Expired snoozed leads first
        2. Then by call_attempts: 0 attempts > 1 attempt > 2 attempt > 3 attempt > 4 attempt > 5 attempt
        3. Within each attempt level: for lead_status SALES LEAD only, lead score (descending); otherwise score is ignored
        4. Then LIFO by subscription_time_stamp (most recent first), then updated_at, creation date, id

        This ensures:
        - Fresh leads (0 attempts) come first
        - "Not connected" leads ordered by attempts (1, 2, 3, 4, 5)
        - lead_status SALES LEAD: higher score before lower; other statuses: subscription_time_stamp LIFO after call_attempts
        """
        qs = qs.extra(where=[self._NEXT_CALL_READY_WHERE])

        if logger.isEnabledFor(logging.DEBUG):
            try:
                sql = str(qs.query)
                has_next_call = "next_call_at" in sql
                logger.debug(
                    "[GetNextLead] _order_by_score: next_call_at in WHERE=%s (re-applied here so base filter cannot be lost)",
                    has_next_call,
                )
            except Exception:
                pass

        # Note on ordering:
        # - lead_status = SALES LEAD: expired snoozed, call_attempts, lead_score, then LIFO by subscription_time_stamp.
        # - Any other lead_status: same through call_attempts, then lead_score_for_sort is constant so
        #   subscription_time_stamp LIFO decides within the attempt band.
        if now_iso:
            qs = qs.extra(
                select={
                    'lead_score': "COALESCE((data->>'lead_score')::float, -1)",
                    'call_attempts_int': "COALESCE((data->>'call_attempts')::int, 0)",
                    'lead_score_for_sort': """
                        CASE
                            WHEN data->>'lead_status' = 'SALES LEAD'
                            THEN COALESCE((data->>'lead_score')::float, -1)
                            ELSE 0
                        END
                    """,
                    'subscription_time_stamp_sort': self._SUBSCRIPTION_TIME_STAMP_SORT_SQL,
                    'is_expired_snoozed': """
                        CASE 
                            WHEN data->>'lead_stage' = 'SNOOZED' 
                            AND (data->>'next_call_at') IS NOT NULL 
                            AND TRIM(COALESCE(data->>'next_call_at', '')) != '' 
                            AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
                            AND (data->>'next_call_at')::timestamptz <= NOW()
                            THEN 0
                            ELSE 1
                        END
                    """,
                }
            ).order_by(
                'is_expired_snoozed',  # Expired snoozed leads first (0), then others (1)
                'call_attempts_int',  # Priority: 0 attempts > 1 attempt > 2 attempt > 3 attempt > 4 attempt > 5 attempt
                F('lead_score_for_sort').desc(nulls_last=True),  # Only SALES LEAD uses real score; else constant
                F('subscription_time_stamp_sort').desc(nulls_last=True),  # LIFO: latest subscription first
                '-updated_at',
                'created_at',
                'id'
            )
        else:
            # Fallback when now_iso is not provided (same ordering, no is_expired_snoozed)
            qs = qs.extra(
                select={
                    'lead_score': "COALESCE((data->>'lead_score')::float, -1)",
                    'call_attempts_int': "COALESCE((data->>'call_attempts')::int, 0)",
                    'lead_score_for_sort': """
                        CASE
                            WHEN data->>'lead_status' = 'SALES LEAD'
                            THEN COALESCE((data->>'lead_score')::float, -1)
                            ELSE 0
                        END
                    """,
                    'subscription_time_stamp_sort': self._SUBSCRIPTION_TIME_STAMP_SORT_SQL,
                }
            ).order_by(
                'call_attempts_int',  # Priority: 0 attempts > 1 attempt > 2 attempt > 3 attempt > 4 attempt > 5 attempt
                F('lead_score_for_sort').desc(nulls_last=True),  # Only SALES LEAD uses real score; else constant
                F('subscription_time_stamp_sort').desc(nulls_last=True),  # LIFO: latest subscription first
                '-updated_at',
                'created_at',
                'id'
            )
        return qs
    
    @extend_schema(
        summary="Get next lead from queue",
        description="Atomically fetches and assigns the next available lead from the queue for CRM records. "
                   "Lead filters (party, lead source, lead status, routing rules) are loaded from the database only; "
                   "no frontend overrides. Logic: 1) Resolve user 2) Load lead filters from DB (UserSettings + routing) "
                   "3) Filter leads by eligible party/source/status and apply routing 4) Order by score 5) Return first entry.",
        responses={
            200: OpenApiResponse(
                description="Lead assigned successfully or no leads available",
                examples=[
                    OpenApiExample(
                        name="Lead Found",
                        value={
                            "record": {
                                "id": 123,
                                "tenant_id": "e35e7279-d92d-4cdf-8014-98deaab639c0",
                                "entity_type": "lead",
                                "name": "John Doe",
                                "data": {
                                    "lead_stage": "ASSIGNED",
                                    "praja_id": "PRAJA123456",
                                    "phone_number": "+919876543210",
                                    "lead_score": 85.5,
                                    "assigned_to": "user123",
                                    "call_attempts": 2,
                                    "next_call_at": "2025-01-02T10:00:00Z",
                                    "closure_time": None,
                                    "lead_source": "Website",
                                    "package_to_pitch": "Premium Plan - ₹999/month"
                                },
                                "created_at": "2025-01-01T00:00:00Z",
                                "updated_at": "2025-01-01T00:00:00Z"
                            }
                        }
                    ),
                    OpenApiExample(
                        name="No Leads Available",
                        value={}
                    )
                ]
            ),
            403: OpenApiResponse(
                description="Authentication required",
                examples=[
                    OpenApiExample(
                        name="Auth Error",
                        value={"detail": "Authentication credentials were not provided."}
                    )
                ]
            )
        },
        tags=["Leads", "CRM Records"]
    )
    def get(self, request):
        """
        Get next unassigned lead from the queue and assign it to the current user.
        Added enhanced logging for diagnosis when no leads are assigned,
        especially to help debug why we are not getting leads even when there are some in queueable states.
        """
        user = request.user
        tenant = request.tenant
        debug_mode = request.query_params.get('debug') in ('1', 'true', 'yes')

        logger.info(
            "[GetNextLead] START request user=%s tenant=%s debug=%s",
            getattr(user, 'email', getattr(user, 'supabase_uid', None)),
            tenant.id if tenant else None,
            debug_mode,
        )

        if not tenant:
            logger.warning("[GetNextLead] Step 0: Abort - no tenant context available")
            logger.info("[GetNextLead] END EMPTY: no tenant.")
            return Response({}, status=status.HTTP_200_OK)

        # Step 1: Get user identifier (supabase_uid or email)
        user_identifier = getattr(user, 'supabase_uid', None) or getattr(user, 'email', None)

        if not user_identifier:
            logger.warning("[GetNextLead] Step 1: Abort - no user identifier available")
            logger.info("[GetNextLead] END EMPTY: no user identifier.")
            return Response({}, status=status.HTTP_200_OK)

        logger.info("[GetNextLead] Step 1: user_identifier=%s", user_identifier)

        # Get current time for checking snoozed leads expiration
        from django.utils import timezone
        now = timezone.now()
        now_iso = now.isoformat()
        logger.info("[GetNextLead] Step 1 done: now=%s", now_iso)

        # Step 2: Lead filters from DB only (party, lead source, lead status, routing) - no frontend overrides
        logger.info("[GetNextLead] Step 2: Loading lead filters from DB (party, sources, statuses, daily_limit)...")
        filters = get_lead_filters_for_user(tenant, user_identifier)
        eligible_lead_types = filters.eligible_lead_types
        eligible_lead_sources = filters.eligible_lead_sources
        eligible_lead_statuses = filters.eligible_lead_statuses
        eligible_states = filters.eligible_states
        daily_limit = filters.daily_limit
        user_uuid = filters.user_uuid
        tenant_membership = filters.tenant_membership

        logger.info(
            "[GetNextLead] Step 2 done: eligible_lead_types=%s eligible_lead_sources=%s eligible_lead_statuses=%s eligible_states=%s daily_limit=%s",
            eligible_lead_types,
            eligible_lead_sources or "(none)",
            eligible_lead_statuses or "(none)",
            eligible_states or "(none)",
            daily_limit,
        )

        # Early re-routing:
        # - SELF TRIAL RMs keep the legacy monolithic flow unchanged.
        #   (identified via eligible_lead_statuses containing "SELF TRIAL")
        # - SALES LEAD / others go through the bucketed LeadPipeline.
        if "SELF TRIAL" not in (eligible_lead_statuses or []):
            logger.info(
                "[GetNextLead] Step 2b: bucketed LeadPipeline (SALES) user=%s tenant=%s debug=%s — "
                "see [LeadPipeline] lines for daily_limit, filters, per-bucket qs_count",
                user_identifier,
                tenant.id if tenant else None,
                debug_mode,
            )
            pipeline = LeadPipeline()
            record = pipeline.get_next(tenant=tenant, request_user=user, debug=debug_mode)
            if not record:
                logger.info(
                    "[GetNextLead] Step 2b done: no record assigned (empty {}). "
                    "Diagnose with [LeadPipeline] start/daily_limit_check/bucket_try/end_empty logs. user=%s",
                    user_identifier,
                )
                return Response({}, status=status.HTTP_200_OK)
            logger.info(
                "[GetNextLead] Step 2b done: assigned record_id=%s user=%s",
                record.pk,
                user_identifier,
            )
            return Response(_flatten_lead_response(record), status=status.HTTP_200_OK)

        # If user has no eligible lead types assigned, push all leads to the RM
        # (no filtering by affiliated_party / party type)
        if not eligible_lead_types:
            logger.info(
                "[GetNextLead] User %s has no party types configured - pushing all queueable leads to RM",
                user_identifier,
            )

        # Step 2.5: Enforce daily lead pull limit (if configured)
        # Count how many leads this user has been assigned today for this tenant.
        # We use Record.updated_at (assignment updates updated_at) as the time signal.
        logger.info("[GetNextLead] Step 2.5: Checking daily limit (daily_limit=%s)...", daily_limit)
        if daily_limit is not None:
            try:
                daily_limit_int = int(daily_limit)
            except (TypeError, ValueError):
                daily_limit_int = None

            if daily_limit_int is not None and daily_limit_int >= 0:
                from django.utils import timezone
                # Support both aware and naive datetimes (some deployments run with USE_TZ=False)
                if timezone.is_aware(now):
                    start_of_day = timezone.localtime(now).replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
                # Count only fresh leads first assigned to this user today
                # This counts leads that were first assigned to this user today, regardless of current assignment status
                # This ensures that if an RM clicks "not connected" and unassigns a lead, it still counts toward their limit
                # We count by first_assigned_to and first_assigned_at to track the original assignment
                # Count leads first assigned to this user today
                # Key: first_assigned_to tracks the ORIGINAL RM who first picked up the lead
                # Even if call_attempts increases (e.g., RM1 clicks "not connected"), 
                # it still counts toward RM1's daily limit because first_assigned_to = RM1
                assigned_today = Record.objects.filter(
                    tenant=tenant,
                    entity_type='lead',
                ).extra(
                    where=[
                        """
                        -- Count leads first assigned to this user today
                        -- Use first_assigned_to and first_assigned_at (new tracking)
                        -- This counts ALL leads first assigned to this user today, regardless of call_attempts
                        -- (because call_attempts increases when RM clicks buttons, but first_assigned_to stays the same)
                        (
                            data->>'first_assigned_to' = %s
                            AND data->>'first_assigned_at' IS NOT NULL
                            AND data->>'first_assigned_at' != ''
                            AND (data->>'first_assigned_at')::timestamptz >= %s
                        )
                        -- For backward compatibility: if first_assigned_at doesn't exist,
                        -- count leads currently assigned to this user (assigned_to = user, non-null) that were updated today
                        OR (
                            (data->>'first_assigned_at' IS NULL OR TRIM(COALESCE(data->>'first_assigned_at', '')) = '')
                            AND (data->>'assigned_to') IS NOT NULL
                            AND TRIM(COALESCE(data->>'assigned_to', '')) != ''
                            AND LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) NOT IN ('null', 'none')
                            AND data->>'assigned_to' = %s
                            AND updated_at >= %s
                            AND COALESCE((data->>'call_attempts')::int, 0) = 0
                        )
                        """
                    ],
                    params=[user_identifier, start_of_day, user_identifier, start_of_day]
                ).count()

                logger.info(
                    "[GetNextLead] Step 2.5: assigned_today=%d daily_limit_int=%d limit_reached=%s",
                    assigned_today,
                    daily_limit_int,
                    assigned_today >= daily_limit_int and not debug_mode,
                )

                if assigned_today >= daily_limit_int and not debug_mode:
                    logger.info(
                        "[GetNextLead] Step 2.5: Daily limit reached for user=%s (assigned_today=%d, daily_limit=%d).",
                        user_identifier,
                        assigned_today,
                        daily_limit_int,
                    )
                    logger.info(
                        "[GetNextLead] FALLBACK [daily-limit]: Using not-connected-retry path (not main queue). "
                        "Assigned-to-user not-connected leads only: due (next_call_at <= now), minimum call_attempts (1 first, then 2..6).",
                    )
                    resp = self._not_connected_retry_response(
                        tenant=tenant,
                        user=user,
                        tenant_membership=tenant_membership,
                        user_identifier=user_identifier,
                        user_uuid=user_uuid,
                        eligible_lead_types=eligible_lead_types,
                        eligible_lead_sources=eligible_lead_sources,
                        eligible_lead_statuses=eligible_lead_statuses,
                        eligible_states=eligible_states,
                        log_label="[GetNextLead] FALLBACK [daily-limit]:",
                    )
                    if resp:
                        return resp

                    logger.info(
                        "[GetNextLead] FALLBACK [daily-limit]: No due not-connected retry leads for user=%s "
                        "(assigned_to=user, call_attempts 1–6, next_call_at <= now, lead_stage=NOT_CONNECTED/IN_QUEUE only).",
                        user_identifier,
                    )
                    logger.info(
                        "[GetNextLead] FALLBACK [daily-limit]: No due not-connected retry leads (next_call_at <= now, call_attempts 1–6).",
                    )
                    logger.info("[GetNextLead] END EMPTY: daily limit reached, no due retry leads.")
                    return Response({}, status=status.HTTP_200_OK)
                else:
                    logger.info(
                        "[GetNextLead] Step 2.5: Under daily limit (assigned_today=%d < daily_limit=%d) or debug_mode - not using fallback.",
                        assigned_today, daily_limit_int,
                    )
                    logger.info(
                        "[GetNextLead] MAIN QUEUE: Fetching next lead from main queue (fresh + unassigned retry due), not daily-limit retry path.",
                    )
            else:
                logger.info("[GetNextLead] Step 2.5: daily_limit value invalid or disabled - proceeding to Step 3 (main queue).")
        else:
            logger.info("[GetNextLead] Step 2.5: daily_limit not set - proceeding to Step 3 (main queue).")

        # Step 3: Filter leads by eligible lead types (affiliated_party field) and unassigned status
        from django.db.models import Q
        logger.info("[GetNextLead] Step 3: Building main queue (queueable WHERE + affiliated_party + routing)...")
        total_leads_in_tenant = Record.objects.filter(tenant=tenant, entity_type='lead').count()
        logger.info(
            "[GetNextLead] Step 3: tenant=%s total_leads_in_tenant=%d user_identifier=%s user_uuid=%s",
            tenant.slug if getattr(tenant, 'slug', None) else tenant.id, total_leads_in_tenant, user_identifier, user_uuid,
        )

        # Step 3a: SNOOZED/IN_QUEUE leads with next_call_at due first (before fresh). Priority: (1) assigned to me, (2) unassigned, then main queue.
        candidate = None
        _snoozed_due_common = """
                UPPER(COALESCE(data->>'lead_stage','')) IN ('SNOOZED', 'IN_QUEUE')
                AND (data->>'next_call_at') IS NOT NULL
                AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
                AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
                AND (data->>'next_call_at')::timestamptz <= NOW()
                AND COALESCE((data->>'call_attempts')::int, 0) < 6
                """
        # 3a(i): Assigned to current user (my SNOOZED/IN_QUEUE leads due for callback)
        _assigned_snoozed_where = (
            "data->>'assigned_to' IS NOT NULL AND TRIM(COALESCE(data->>'assigned_to', '')) != '' AND data->>'assigned_to' = %s AND " + _snoozed_due_common
        )
        assigned_snoozed_qs = Record.objects.filter(
            tenant=tenant,
            entity_type='lead'
        ).extra(where=[_assigned_snoozed_where], params=[user_identifier])
        # Routing rules removed for lead flow; group/KV filters only.
        if eligible_lead_types:
            assigned_snoozed_qs = assigned_snoozed_qs.filter(data__affiliated_party__in=eligible_lead_types)
        if eligible_lead_sources:
            assigned_snoozed_qs = assigned_snoozed_qs.filter(data__lead_source__in=eligible_lead_sources)
        if eligible_lead_statuses:
            assigned_snoozed_qs = assigned_snoozed_qs.filter(data__lead_status__in=eligible_lead_statuses)
        if eligible_states:
            assigned_snoozed_qs = assigned_snoozed_qs.filter(data__state__in=eligible_states)
        ordered_assigned_snoozed = self._order_by_score(assigned_snoozed_qs, now_iso)
        for c in ordered_assigned_snoozed[:50]:
            if self._lead_is_due_for_call(c.data, now):
                candidate = c
                logger.info(
                    "[GetNextLead] Step 3a(i): Selected SNOOZED/IN_QUEUE-due lead_id=%s assigned to me (next_call_at passed).",
                    c.id,
                )
                break
        # 3a(ii): If none assigned to me, try unassigned SNOOZED/IN_QUEUE with next_call_at due
        if not candidate:
            _unassigned_snoozed_where = """
                (
                    (data->>'assigned_to') IS NULL
                    OR TRIM(COALESCE(data->>'assigned_to', '')) = ''
                    OR LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) IN ('null', 'none')
                )
                AND """ + _snoozed_due_common
            unassigned_snoozed_qs = Record.objects.filter(
                tenant=tenant,
                entity_type='lead'
            ).extra(where=[_unassigned_snoozed_where])
            # Routing rules removed for lead flow; group/KV filters only.
            if eligible_lead_types:
                unassigned_snoozed_qs = unassigned_snoozed_qs.filter(data__affiliated_party__in=eligible_lead_types)
            if eligible_lead_sources:
                unassigned_snoozed_qs = unassigned_snoozed_qs.filter(data__lead_source__in=eligible_lead_sources)
            if eligible_lead_statuses:
                unassigned_snoozed_qs = unassigned_snoozed_qs.filter(data__lead_status__in=eligible_lead_statuses)
            if eligible_states:
                unassigned_snoozed_qs = unassigned_snoozed_qs.filter(data__state__in=eligible_states)
            unassigned_snoozed_qs = unassigned_snoozed_qs.extra(
                where=["""
                    NOT (
                        (data->>'assigned_to') IS NOT NULL
                        AND TRIM(COALESCE(data->>'assigned_to', '')) != ''
                        AND LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) NOT IN ('null', 'none')
                        AND data->>'assigned_to' != %s
                    )
                """],
                params=[user_identifier],
            )
            ordered_unassigned_snoozed = self._order_by_score(unassigned_snoozed_qs, now_iso)
            for c in ordered_unassigned_snoozed[:50]:
                if self._lead_is_due_for_call(c.data, now):
                    candidate = c
                    logger.info(
                        "[GetNextLead] Step 3a(ii): Selected unassigned SNOOZED/IN_QUEUE-due lead_id=%s (next_call_at passed).",
                        c.id,
                    )
                    break
        if not candidate:
            logger.info("[GetNextLead] Step 3a: No snoozed-due leads (assigned to me or unassigned); proceeding to main queue (fresh leads).")

        # Common WHERE conditions for queueable leads: unassigned, lead_stage in (FRESH, IN_QUEUE), 0 call attempts.
        # assigned_to: match JSON null, empty, or string 'null'/'None' (exact data shape)
        # Retry logic for NOT_CONNECTED etc. is handled separately; main queue is pure fresh (call_attempts = 0).
        _queueable_where = """
                (
                    (data->>'assigned_to') IS NULL
                    OR TRIM(COALESCE(data->>'assigned_to', '')) = ''
                    OR LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) IN ('null', 'none')
                )
                AND UPPER(COALESCE(data->>'lead_stage','')) IN ('FRESH','IN_QUEUE')
                AND COALESCE((data->>'call_attempts')::int, 0) = 0
                """
        # Single queue: queueable leads (unassigned, lead_stage in (FRESH, IN_QUEUE), 0 call_attempts).
        base_qs = Record.objects.filter(
            tenant=tenant,
            entity_type='lead'
        ).extra(where=[_queueable_where])
        queueable_before_routing = base_qs.count()
        logger.info(
            "[GetNextLead] Step 3: queueable before routing: count=%d",
            queueable_before_routing,
        )

        logger.info("[GetNextLead] Step 3: Routing rule skipped (group/KV-only lead flow).")

        # Filter by eligible lead types (affiliated_party) from lead filter – use party list from DB as-is
        if not eligible_lead_types:
            unassigned = base_qs
            logger.info("[GetNextLead] No party types configured - using all queueable leads (unfiltered by affiliated_party)")
        else:
            unassigned = base_qs.filter(data__affiliated_party__in=eligible_lead_types)
            logger.info("[GetNextLead] Filtered unassigned leads by eligible types (from lead filter): %s", eligible_lead_types)

        # Intersection of all selected: only leads matching party AND lead_source AND lead_status (when each is configured).
        if eligible_lead_sources:
            unassigned = unassigned.filter(data__lead_source__in=eligible_lead_sources)
            logger.info("[GetNextLead] Filtered unassigned leads by eligible lead sources (intersection): %s", eligible_lead_sources)
        if eligible_lead_statuses:
            unassigned = unassigned.filter(data__lead_status__in=eligible_lead_statuses)
            logger.info("[GetNextLead] Filtered unassigned leads by eligible lead statuses (intersection): %s", eligible_lead_statuses)
        if eligible_states:
            unassigned = unassigned.filter(data__state__in=eligible_states)
            logger.info("[GetNextLead] Filtered unassigned leads by eligible states (intersection): %s", eligible_states)

        # Exclude leads assigned to someone else (assigned_to = non-empty and != current user).
        # Treat JSON null, empty, 'null', 'none' as unassigned (exact data shape).
        before_exclude = unassigned.count()
        unassigned = unassigned.extra(
            where=["""
                NOT (
                    (data->>'assigned_to') IS NOT NULL
                    AND TRIM(COALESCE(data->>'assigned_to', '')) != ''
                    AND LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) NOT IN ('null', 'none')
                    AND data->>'assigned_to' != %s
                )
            """],
            params=[user_identifier],
        )
        after_exclude = unassigned.count()
        logger.info(
            "[GetNextLead] Step 3: After excluding leads assigned to other users: count %d -> %d (user_identifier=%s)",
            before_exclude, after_exclude, user_identifier,
        )
        if before_exclude > 0 and after_exclude == 0:
            logger.warning(
                "[GetNextLead] Step 3: All %d leads were excluded by assigned_to filter (none match user_identifier=%s). "
                "Check if data.assigned_to is stored as UUID while user_identifier is email (or vice versa).",
                before_exclude, user_identifier,
            )

        # Filter by call attempt matrix rules (max attempts, SLA, min time between calls)
        # Load call attempt matrices for all eligible lead types - BULK FETCH to avoid N+1 queries
        call_attempt_matrices = {}
        if eligible_lead_types:
            # Single bulk query instead of one query per lead_type
            matrices = CallAttemptMatrix.objects.filter(
                tenant=tenant,
                lead_type__in=eligible_lead_types
            )
            # Build dictionary mapping lead_type -> matrix
            for matrix in matrices:
                call_attempt_matrices[matrix.lead_type] = matrix
                logger.debug(
                    "[GetNextLead] Loaded call attempt matrix for lead_type=%s: max_attempts=%d, sla_days=%d, min_hours=%d",
                    matrix.lead_type, matrix.max_call_attempts, matrix.sla_days, matrix.min_time_between_calls_hours
                )
        
        # Filter out leads that exceed matrix limits
        if call_attempt_matrices:
            excluded_count = 0
            valid_lead_ids = []
            
            # Build Q objects for efficient filtering
            exclusion_filters = Q()
            
            for lead_type, matrix in call_attempt_matrices.items():
                # Filter by lead type (from DB; same party list as lead filter)
                lead_type_filter = Q(data__affiliated_party=lead_type)
                
                # Exclude leads that exceed max call attempts
                max_attempts_exceeded = lead_type_filter & Q(
                    data__call_attempts__gte=matrix.max_call_attempts
                )
                exclusion_filters |= max_attempts_exceeded
                
                # Exclude leads that exceed SLA (days since creation)
                # Calculate cutoff date
                cutoff_date = now - timedelta(days=matrix.sla_days)
                sla_exceeded = lead_type_filter & Q(created_at__lt=cutoff_date)
                exclusion_filters |= sla_exceeded
                
                logger.debug(
                    "[GetNextLead] Added exclusion filters for lead_type=%s: max_attempts>=%d, sla_days=%d",
                    lead_type, matrix.max_call_attempts, matrix.sla_days
                )
            
            # Apply exclusions
            if exclusion_filters:
                before_count = unassigned.count()
                unassigned = unassigned.exclude(exclusion_filters)
                after_count = unassigned.count()
                excluded_count = before_count - after_count
                
                if excluded_count > 0:
                    logger.info(
                        "[GetNextLead] Excluded %d leads based on call attempt matrix rules (max attempts or SLA)",
                        excluded_count
                    )
            
            # Additional check for minimum time between calls (requires per-record evaluation)
            if call_attempt_matrices:
                final_valid_ids = []
                for lead in unassigned[:1000]:  # Limit to first 1000 for performance
                    lead_data = lead.data or {}
                    lead_type = lead_data.get('affiliated_party')
                    # Find matching matrix
                    matrix = None
                    for lt, m in call_attempt_matrices.items():
                        if lead_type == lt:
                            matrix = m
                            break
                    
                    if matrix:
                        should_exclude, reason = self._should_exclude_lead_by_matrix(lead, lead_data, matrix, now)
                        if should_exclude:
                            excluded_count += 1
                            logger.debug(
                                "[GetNextLead] Excluding lead_id=%d lead_type=%s reason=%s",
                                lead.id, lead_type, reason
                            )
                            continue
                    
                    final_valid_ids.append(lead.id)
                
                if final_valid_ids:
                    unassigned = unassigned.filter(id__in=final_valid_ids)
                    logger.info(
                        "[GetNextLead] Step 3: After call attempt matrix (min_time_between_calls): %d leads remaining (valid_ids count=%d)",
                        len(final_valid_ids), len(final_valid_ids),
                    )
                else:
                    unassigned = unassigned.none()
                    logger.warning(
                        "[GetNextLead] Step 3: Call attempt matrix excluded all leads (min_time_between_calls or other matrix rules). unassigned set to none.",
                    )
        after_matrix_cnt = unassigned.count()
        if call_attempt_matrices and after_matrix_cnt == 0:
            logger.info(
                "[GetNextLead] Step 3: unassigned count after call attempt matrix = 0 (matrices applied for types: %s)",
                list(call_attempt_matrices.keys()),
            )

        # --- Enhanced Diagnostics: Log possible unassigned counts for debugging ---
        unassigned_cnt = unassigned.count()
        total_unassigned_cnt = base_qs.count()
        logger.info(
            "[GetNextLead] Step 3 done: total_queueable=%d unassigned_matching_filters=%d user=%s",
            total_unassigned_cnt, unassigned_cnt, user_identifier,
        )

        if unassigned_cnt == 0 and total_unassigned_cnt > 0:
            # There are unassigned queueable leads, but none matching the user's eligible lead types
            lead_types_in_queue = list(base_qs.values_list("data__affiliated_party", flat=True).distinct())
            logger.warning(
                "[GetNextLead] Zero leads after filters but base_qs had %d: no match for user's eligible types. "
                "Present affiliated_party in queue: %s. User eligible_lead_types: %s eligible_lead_sources: %s eligible_lead_statuses: %s",
                total_unassigned_cnt, lead_types_in_queue, eligible_lead_types, eligible_lead_sources or "(none)", eligible_lead_statuses or "(none)",
            )
        elif total_unassigned_cnt == 0:
            logger.info(
                "[GetNextLead] Step 3: total_queueable=0 for tenant=%s (no FRESH unassigned leads).",
                tenant.slug if getattr(tenant, 'slug', None) else tenant,
            )

        # Step 4: Order by priority (expired snoozed first), then lead score (descending: 100, 90, 80, etc.)
        # Log snoozed leads count for debugging (check before affiliated_party filter)
        all_snoozed_count = Record.objects.filter(
            tenant=tenant,
            entity_type='lead',
            data__lead_stage='SNOOZED'
        ).count()
        expired_snoozed_before_filter = Record.objects.filter(
            tenant=tenant,
            entity_type='lead'
        ).extra(
            where=["""
                data->>'lead_stage' = 'SNOOZED'
                AND (data->>'next_call_at') IS NOT NULL
                AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
                AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
                AND (data->>'next_call_at')::timestamptz <= NOW()
            """]
        ).count()
        # Check after affiliated_party filter
        snoozed_count = unassigned.filter(data__lead_stage='SNOOZED').count()
        expired_snoozed_count = unassigned.extra(
            where=["""
                data->>'lead_stage' = 'SNOOZED'
                AND (data->>'next_call_at') IS NOT NULL
                AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
                AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
                AND (data->>'next_call_at')::timestamptz <= NOW()
            """]
        ).count()
        logger.info(
            "[GetNextLead] Snoozed leads: all_snoozed=%d, expired_before_filter=%d, expired_after_affiliated_party_filter=%d, now=%s",
            all_snoozed_count, expired_snoozed_before_filter, expired_snoozed_count, now_iso
        )
        
        # Debug mode: return pipeline counts to diagnose "no leads" issues
        if debug_mode:
            _affiliated_extra = """
                AND data->>'affiliated_party' IS NOT NULL
                AND data->>'affiliated_party' != ''
                AND data->>'affiliated_party' != 'null'
            """
            queueable_with_aff = Record.objects.filter(
                tenant=tenant, entity_type='lead'
            ).extra(where=[_queueable_where + _affiliated_extra]).count()
            queueable_total = Record.objects.filter(
                tenant=tenant, entity_type='lead'
            ).extra(where=[_queueable_where]).count()
            rule = None
            sample_leads = list(
                Record.objects.filter(tenant=tenant, entity_type='lead')[:5]
                .values('id', 'data')
            )
            unassigned_minimal = Record.objects.filter(
                tenant=tenant,
                entity_type='lead'
            ).extra(where=["""
                (data->>'assigned_to' IS NULL OR data->>'assigned_to' = '' OR data->>'assigned_to' = 'null' OR data->>'assigned_to' = 'None')
            """])
            return Response({
                "debug": True,
                "user_identifier": user_identifier,
                "user_uuid": str(user_uuid) if user_uuid else None,
                "tenant_id": str(tenant.id) if tenant else None,
                "eligible_lead_types": eligible_lead_types,
                "daily_limit": daily_limit,
                "counts": {
                    "total_leads_in_tenant": Record.objects.filter(tenant=tenant, entity_type='lead').count(),
                    "unassigned_minimal": unassigned_minimal.count(),
                    "queueable_with_affiliated_party": queueable_with_aff,
                    "queueable_total": queueable_total,
                    "after_routing_and_filter": unassigned_cnt,
                    "base_qs_count": total_unassigned_cnt,
                },
                "routing_rule": {
                    "has_rule": rule is not None,
                    "conditions": rule.conditions if rule else None,
                },
                "sample_leads_data": [
                    {"id": s["id"], "lead_stage": (s.get("data") or {}).get("lead_stage"), "affiliated_party": (s.get("data") or {}).get("affiliated_party"), "assigned_to": (s.get("data") or {}).get("assigned_to"), "call_attempts": (s.get("data") or {}).get("call_attempts"), "next_call_at": (s.get("data") or {}).get("next_call_at")}
                    for s in sample_leads
                ],
                "distinct_lead_stages": list(Record.objects.filter(tenant=tenant, entity_type='lead').values_list('data__lead_stage', flat=True).distinct()[:20]),
                "distinct_affiliated_parties": list(Record.objects.filter(tenant=tenant, entity_type='lead').values_list('data__affiliated_party', flat=True).distinct()[:20]),
            }, status=status.HTTP_200_OK)

        # Only build ordered list and pick from fresh queue when we did not already get a snoozed-due candidate in Step 3a
        if candidate is None:
            logger.info(
                "[GetNextLead] Step 4: Ordering by score (call_attempts asc, score desc, LIFO by subscription_time_stamp)..."
            )
            ordered = self._order_by_score(unassigned, now_iso)
            candidate = None
            checked = 0
            for c in ordered[:50]:
                checked += 1
                if self._lead_is_due_for_call(c.data, now):
                    candidate = c
                    logger.info(
                        "[GetNextLead] Step 4: Selected candidate lead_id=%s (checked %d, call_attempts=%s)",
                        c.id, checked, (c.data or {}).get('call_attempts'),
                    )
                    break
                logger.info(
                    "[GetNextLead] Step 4: Skipping lead_id=%s (not due yet: call_attempts=%s next_call_at=%s)",
                    c.id, (c.data or {}).get('call_attempts'), (c.data or {}).get('next_call_at'),
                )
            if not candidate and checked > 0:
                logger.info("[GetNextLead] Step 4: No candidate due for call among first 50 ordered leads (checked=%d).", checked)

        # Step 5: Return first entry (or empty if none found)
        logger.info("[GetNextLead] Step 5: Lock and assign (candidate=%s)...", candidate.id if candidate else None)

        if not candidate:
            logger.info(
                "[GetNextLead] Step 5: No candidate from main queue — trying not-connected retry (SELF TRIAL). "
                "unassigned_cnt=%d total_unassigned_cnt=%d",
                unassigned_cnt,
                total_unassigned_cnt,
            )
            if not debug_mode:
                resp = self._not_connected_retry_response(
                    tenant=tenant,
                    user=user,
                    tenant_membership=tenant_membership,
                    user_identifier=user_identifier,
                    user_uuid=user_uuid,
                    eligible_lead_types=eligible_lead_types,
                    eligible_lead_sources=eligible_lead_sources,
                    eligible_lead_statuses=eligible_lead_statuses,
                    eligible_states=eligible_states,
                    log_label="[GetNextLead] Step 5a:",
                )
                if resp:
                    return resp
            logger.info(
                "[GetNextLead] Step 5: No candidate - returning empty. unassigned_cnt=%d total_unassigned_cnt=%d",
                unassigned_cnt, total_unassigned_cnt,
            )
            if unassigned_cnt > 0:
                logger.info(
                    "[GetNextLead] Step 5: Unassigned leads existed (%d) but none passed ordering or _lead_is_due_for_call check.",
                    unassigned_cnt,
                )
            logger.info(
                "[GetNextLead] END EMPTY: no lead assigned. Pipeline: total_leads=%d queueable_before_routing=%d total_queueable=%d unassigned_after_filters=%d. "
                "Check logs above for which step reduced count to 0.",
                total_leads_in_tenant, queueable_before_routing, total_unassigned_cnt, unassigned_cnt,
            )
            return Response({}, status=status.HTTP_200_OK)

        # Lock and assign the lead
        logger.info("[GetNextLead] Step 5: Acquiring lock for lead_id=%s...", candidate.pk)
        with transaction.atomic():
            candidate_locked = Record.objects.select_for_update(skip_locked=True).filter(pk=candidate.pk).first()

            if not candidate_locked:
                logger.info("[GetNextLead] Step 5: Lead_id=%s was taken by another request (skip_locked).", candidate.pk)
                logger.info("[GetNextLead] END EMPTY: lead taken by another request.")
                return Response({}, status=status.HTTP_200_OK)

            if not self._lead_is_due_for_call(candidate_locked.data, timezone.now()):
                logger.info(
                    "[GetNextLead] Step 5: After lock lead_id=%s not due (call_attempts=%s next_call_at=%s).",
                    candidate_locked.id,
                    (candidate_locked.data or {}).get("call_attempts"),
                    (candidate_locked.data or {}).get("next_call_at"),
                )
                logger.info("[GetNextLead] END EMPTY: lead not due after lock.")
                return Response({}, status=status.HTTP_200_OK)

            logger.info("[GetNextLead] Step 5: Lock acquired. Assigning lead_id=%s to user=%s...", candidate_locked.id, user_identifier)

            # Update the candidate's data
            data = candidate_locked.data.copy() if candidate_locked.data else {}
            pre_assignment_lead_stage = (data.get("lead_stage") or "").strip().upper()
            previous_assigned_to = data.get('assigned_to')
            is_fresh_assignment = _legacy_get_next_lead_assignee_is_unassigned(previous_assigned_to)

            if not is_fresh_assignment and not _legacy_get_next_lead_assignees_match(
                previous_assigned_to, user_identifier
            ):
                logger.info(
                    "[GetNextLead] Step 5: Lost race — lead already assigned to another user "
                    "lead_id=%s previous_assigned_to=%s requester=%s",
                    candidate_locked.id,
                    previous_assigned_to,
                    user_identifier,
                )
                logger.info("[GetNextLead] END EMPTY: lead claimed by another RM before assign.")
                return Response({}, status=status.HTTP_200_OK)

            data['assigned_to'] = user_identifier
            data['lead_stage'] = self.ASSIGNED_STATUS
            # Ensure call_attempts is always present for downstream logic/UI
            if 'call_attempts' not in data or data.get('call_attempts') in (None, '', 'null'):
                data['call_attempts'] = 0
            
            # Track first_assigned_at for fresh leads (for daily limit tracking)
            # Only set first_assigned_at if this is a fresh assignment (not a retry)
            # EXCEPTION: Don't set first_assigned_to for "not connected" retry leads
            # (they shouldn't count toward new RM's daily limit)
            call_attempts = data.get('call_attempts', 0)
            try:
                call_attempts_int = int(call_attempts) if call_attempts is not None else 0
            except (TypeError, ValueError):
                call_attempts_int = 0
            
            # Use pre-assignment stage (lead_stage was overwritten to ASSIGNED above).
            last_call_outcome = (data.get("last_call_outcome") or "").lower()
            # Check if this is a retry lead (NOT_CONNECTED only)
            # These leads should NOT set first_assigned_to when reassigned to a new RM
            # last_call_outcome in DB is exactly "not_connected"

            is_not_connected_retry = (
                call_attempts_int > 0 or
                last_call_outcome == 'not_connected' or
                pre_assignment_lead_stage == 'NOT_CONNECTED'
            )
            
            if is_fresh_assignment and 'first_assigned_at' not in data and not is_not_connected_retry:
                data['first_assigned_at'] = now.isoformat()
                data['first_assigned_to'] = user_identifier
                logger.info(
                    "[GetNextLead] Set first_assigned_to=%s and first_assigned_at for lead_id=%d (fresh lead assignment)",
                    user_identifier, candidate_locked.id
                )
            elif is_fresh_assignment and is_not_connected_retry:
                logger.info(
                    "[GetNextLead] Skipping first_assigned_to for lead_id=%d (retry lead - call_attempts=%d, "
                    "last_call_outcome=%s, pre_assignment_lead_stage=%s, won't count toward daily limit)",
                    candidate_locked.id, call_attempts_int, last_call_outcome, pre_assignment_lead_stage
                )

            # if is_fresh_assignment:
            #     merge_first_assignment_today_anchor(data, now)

            candidate_locked.data = data
            candidate_locked.updated_at = timezone.now()
            candidate_locked.save(update_fields=['data', 'updated_at'])

            logger.info(
                "[GetNextLead] Step 5 done: Assigned lead record_id=%s user=%s (fresh=%s). Saving and building response.",
                candidate_locked.id,
                user_identifier,
                is_fresh_assignment,
            )

            PostAssignmentActions().run(
                record=candidate_locked,
                tenant=tenant,
                user=user,
                tenant_membership=tenant_membership,
                user_identifier=user_identifier,
                user_uuid=user_uuid,
                lead_data=data,
            )

        # Force refresh from database to ensure we have absolute latest data
        # This bypasses any queryset caching and ensures fresh DB read
        candidate_locked.refresh_from_db()
        
        # Additional safety: Re-query from DB to ensure no stale data
        candidate_locked = Record.objects.select_related().get(pk=candidate_locked.pk)

        # Serialize and flatten for frontend compatibility
        serialized_data = RecordSerializer(candidate_locked).data
        lead_data = candidate_locked.data or {}

        # Flatten the response structure for easier frontend access
        # Map data fields to top-level for backward compatibility with defaults
        flattened_response = {
            "id": candidate_locked.id,
            "name": (candidate_locked.data or {}).get('name', '') if isinstance(candidate_locked.data, dict) else '',
            "phone_no": lead_data.get('phone_number', ''),
            "praja_id": lead_data.get('praja_id'),
            "lead_status": lead_data.get('lead_stage') or '',
            "lead_score": lead_data.get('lead_score'),
            "lead_type": lead_data.get('affiliated_party') or lead_data.get('poster'),  # Prefer affiliated_party, fallback to poster for backward compatibility
            "assigned_to": lead_data.get('assigned_to'),
            "attempt_count": lead_data.get('call_attempts', 0),
            "last_call_outcome": lead_data.get('last_call_outcome'),
            "next_call_at": lead_data.get('next_call_at'),
            "do_not_call": lead_data.get('do_not_call', False),
            "resolved_at": lead_data.get('closure_time'),
            "premium_poster_count": lead_data.get('premium_poster_count'),
            "package_to_pitch": lead_data.get('package_to_pitch'),
            "last_active_date_time": lead_data.get('last_active_date_time'),
            "latest_remarks": lead_data.get('latest_remarks'),
            "lead_description": lead_data.get('lead_description'),
            "affiliated_party": lead_data.get('affiliated_party'),
            "rm_dashboard": lead_data.get('rm_dashboard'),
            "user_profile_link": lead_data.get('user_profile_link'),
            "whatsapp_link": lead_data.get('whatsapp_link'),
            "lead_source": lead_data.get('lead_source'),
            "created_at": serialized_data.get('created_at'),
            "updated_at": serialized_data.get('updated_at'),
            "data": lead_data,
            "record": serialized_data
        }

        logger.info(
            "[GetNextLead] Step 5: Returning lead record_id=%s name=%s phone_no=%s source=%s",
            candidate_locked.id,
            flattened_response.get('name'),
            flattened_response.get('phone_no'),
            flattened_response.get('lead_source'),
        )
        logger.info("[GetNextLead] END SUCCESS: assigned lead_id=%s to user=%s", candidate_locked.id, user_identifier)

        return Response(flattened_response, status=status.HTTP_200_OK)


class GetMyCurrentLeadView(APIView):
    """
    Get the user's currently assigned lead from the database.
    Only returns a lead when lead_stage is "ASSIGNED" (after any of the 4 buttons
    is clicked and stage changes to IN_QUEUE, SNOOZED, etc., it will not appear).
    
    GET /crm-records/leads/current/
    """
    permission_classes = [IsTenantAuthenticated]
    
    @extend_schema(
        summary="Get my current assigned lead",
        description="Returns the lead currently assigned to the user, always fetched fresh from the database. "
                   "This ensures users see the same lead after page refresh or navigation.",
        responses={
            200: OpenApiResponse(
                description="Current lead found",
                examples=[
                    OpenApiExample(
                        name="Lead Found",
                        value={
                            "id": 123,
                            "name": "John Doe",
                            "phone_no": "+919876543210",
                            "lead_status": "ASSIGNED",
                            "assigned_to": "user-uuid-123"
                        }
                    )
                ]
            ),
            200: OpenApiResponse(
                description="No lead assigned",
                examples=[
                    OpenApiExample(
                        name="No Lead",
                        value={}
                    )
                ]
            )
        },
        tags=["Leads", "Current Lead"]
    )
    def get(self, request):
        """
        Get the user's currently assigned lead from the database.
        Always queries DB directly to ensure fresh data.
        """
        user = request.user
        tenant = request.tenant
        
        if not tenant:
            logger.warning("[GetMyCurrentLead] No tenant context available")
            return Response({}, status=status.HTTP_200_OK)
        
        # Get user identifier (supabase_uid or email)
        user_identifier = getattr(user, 'supabase_uid', None) or getattr(user, 'email', None)
        
        if not user_identifier:
            logger.warning("[GetMyCurrentLead] No user identifier available")
            return Response({}, status=status.HTTP_200_OK)
        
        logger.info("[GetMyCurrentLead] Getting current lead for user: %s", user_identifier)
        
        # Only return lead when lead_stage is "ASSIGNED"
        current_lead = Record.objects.filter(
            tenant=tenant,
            entity_type='lead',
            data__assigned_to=user_identifier,
            data__lead_stage='ASSIGNED'
        ).order_by('-updated_at').first()
        
        # Force fresh DB query - refresh from database
        if current_lead:
            current_lead.refresh_from_db()
            # Re-query to ensure absolute latest data
            current_lead = Record.objects.filter(pk=current_lead.pk).first()
        
        if not current_lead:
            logger.info("[GetMyCurrentLead] No current lead found for user: %s", user_identifier)
            return Response({}, status=status.HTTP_200_OK)
        
        # Serialize and flatten for frontend compatibility (same format as GetNextLeadView)
        serialized_data = RecordSerializer(current_lead).data
        lead_data = current_lead.data or {}
        
        flattened_response = {
            "id": current_lead.id,
            "name": (current_lead.data or {}).get('name', '') if isinstance(current_lead.data, dict) else '',
            "phone_no": lead_data.get('phone_number', ''),
            "praja_id": lead_data.get('praja_id'),
            "lead_status": lead_data.get('lead_stage') or '',
            "lead_score": lead_data.get('lead_score'),
            "lead_type": lead_data.get('affiliated_party') or lead_data.get('poster'),
            "assigned_to": lead_data.get('assigned_to'),
            "attempt_count": lead_data.get('call_attempts', 0),
            "last_call_outcome": lead_data.get('last_call_outcome'),
            "next_call_at": lead_data.get('next_call_at'),
            "do_not_call": lead_data.get('do_not_call', False),
            "resolved_at": lead_data.get('closure_time'),
            "premium_poster_count": lead_data.get('premium_poster_count'),
            "package_to_pitch": lead_data.get('package_to_pitch'),
            "last_active_date_time": lead_data.get('last_active_date_time'),
            "latest_remarks": lead_data.get('latest_remarks'),
            "lead_description": lead_data.get('lead_description'),
            "affiliated_party": lead_data.get('affiliated_party'),
            "rm_dashboard": lead_data.get('rm_dashboard'),
            "user_profile_link": lead_data.get('user_profile_link'),
            "whatsapp_link": lead_data.get('whatsapp_link'),
            "lead_source": lead_data.get('lead_source'),
            "created_at": serialized_data.get('created_at'),
            "updated_at": serialized_data.get('updated_at'),
            "data": lead_data,
            "record": serialized_data
        }
        
        logger.info(
            "[GetMyCurrentLead] Returning current lead: record_id=%s name=%s updated_at=%s",
            current_lead.id,
            flattened_response.get('name'),
            current_lead.updated_at
        )
        
        return Response(flattened_response, status=status.HTTP_200_OK)


def _flatten_lead_response(record):
    """Build the same flattened lead response used by GetMyCurrentLeadView / GetNextLeadView."""
    serialized_data = RecordSerializer(record).data
    lead_data = record.data or {}
    return {
        "id": record.id,
        "name": (record.data or {}).get('name', '') if isinstance(record.data, dict) else '',
        "phone_no": lead_data.get('phone_number', ''),
        "praja_id": lead_data.get('praja_id'),
        "lead_status": lead_data.get('lead_stage') or '',
        "lead_score": lead_data.get('lead_score'),
        "lead_type": lead_data.get('affiliated_party') or lead_data.get('poster'),
        "assigned_to": lead_data.get('assigned_to'),
        "attempt_count": lead_data.get('call_attempts', 0),
        "last_call_outcome": lead_data.get('last_call_outcome'),
        "next_call_at": lead_data.get('next_call_at'),
        "do_not_call": lead_data.get('do_not_call', False),
        "resolved_at": lead_data.get('closure_time'),
        "premium_poster_count": lead_data.get('premium_poster_count'),
        "package_to_pitch": lead_data.get('package_to_pitch'),
        "last_active_date_time": lead_data.get('last_active_date_time'),
        "latest_remarks": lead_data.get('latest_remarks'),
        "lead_description": lead_data.get('lead_description'),
        "affiliated_party": lead_data.get('affiliated_party'),
        "rm_dashboard": lead_data.get('rm_dashboard'),
        "user_profile_link": lead_data.get('user_profile_link'),
        "whatsapp_link": lead_data.get('whatsapp_link'),
        "lead_source": lead_data.get('lead_source'),
        "created_at": serialized_data.get('created_at'),
        "updated_at": serialized_data.get('updated_at'),
        "data": lead_data,
        "record": serialized_data
    }


class PartnerEventsView(APIView):
    """
    Partner webhook: accept events (e.g. work_on_lead) from partner systems (Halocom, etc.).
    Authenticated via X-Secret-Pyro. Enqueues assignment as a background job and returns 202.
    """
    authentication_classes = []
    permission_classes = [HasAPISecret]

    def _get_tenant(self, request):
        """
        Resolve tenant: prefer tenant derived from API secret (so partner does not need to send tenant_id).
        When the secret is from ApiSecretKey, that key is mapped to a tenant; otherwise fall back to
        tenant_id in request or default tenant slug.
        """
        from django.conf import settings
        # 1) Derive from API secret: when partner uses a secret from DB, tenant is fixed per key
        api_secret_obj = getattr(request, 'api_secret_obj', None)
        if api_secret_obj and api_secret_obj.tenant:
            return api_secret_obj.tenant, None
        # 2) Optional: tenant_id in request (e.g. when using default PYRO_SECRET without DB mapping)
        tenant_id = request.query_params.get('tenant_id') or request.data.get('tenant_id')
        if tenant_id:
            try:
                tenant = Tenant.objects.get(id=tenant_id)
                return tenant, None
            except Tenant.DoesNotExist:
                return None, Response({'error': f'Tenant with id {tenant_id} not found'}, status=status.HTTP_404_NOT_FOUND)
            except (ValueError, TypeError):
                return None, Response({'error': f'Invalid tenant_id format: {tenant_id}'}, status=status.HTTP_400_BAD_REQUEST)
        # 3) Default tenant from settings
        default_slug = getattr(settings, 'DEFAULT_TENANT_SLUG', 'bibhab-thepyro-ai')
        try:
            return Tenant.objects.get(slug=default_slug), None
        except Tenant.DoesNotExist:
            tenant = Tenant.objects.first()
            if tenant:
                return tenant, None
            return None, Response({'error': 'No tenant found in database'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _get_allowed_partner_slugs(self):
        from django.conf import settings
        return getattr(settings, 'PARTNER_SLUGS', ['halocom'])

    def _resolve_record(self, tenant, praja_id):
        """Resolve Record by praja_id (record.data.praja_id)."""
        if not praja_id:
            return None
        return Record.objects.filter(
            tenant=tenant,
            entity_type='lead',
            data__praja_id=praja_id
        ).first()

    @extend_schema(
        summary="Partner events webhook",
        description="Accept partner events (e.g. work_on_lead). Requires X-Secret-Pyro. Returns 202 and processes assignment asynchronously.",
        request={
            'application/json': {
                'type': 'object',
                'properties': {
                    'event': {'type': 'string', 'example': 'work_on_lead'},
                    'praja_id': {'type': 'string', 'description': 'Lead identifier from partner (record.data.praja_id)'},
                    'email_id': {'type': 'string', 'format': 'email'},
                    'partner_slug': {'type': 'string', 'example': 'halocom'},
                    'tenant_id': {'type': 'string', 'format': 'uuid'},
                },
                'required': ['event', 'praja_id', 'email_id'],
            }
        },
        responses={
            202: OpenApiResponse(description="Event accepted"),
            400: OpenApiResponse(description="Validation error"),
            404: OpenApiResponse(description="Tenant / record / user not found"),
        },
        tags=["Partner", "Webhooks"]
    )
    def post(self, request):
        data = request.data or {}
        event = data.get('event')
        email_id = (data.get('email_id') or '').strip().lower()
        praja_id = data.get('praja_id')
        if isinstance(praja_id, str):
            praja_id = praja_id.strip() or None
        partner_slug = (data.get('partner_slug') or 'halocom').strip().lower()

        if not event:
            return Response({'error': 'event is required'}, status=status.HTTP_400_BAD_REQUEST)
        if not email_id:
            return Response({'error': 'email_id is required'}, status=status.HTTP_400_BAD_REQUEST)
        if not praja_id:
            return Response({'error': 'praja_id is required'}, status=status.HTTP_400_BAD_REQUEST)

        allowed = self._get_allowed_partner_slugs()
        if partner_slug not in [s.lower() for s in allowed]:
            return Response({'error': f'partner_slug "{partner_slug}" is not allowed'}, status=status.HTTP_400_BAD_REQUEST)

        tenant, err = self._get_tenant(request)
        if err:
            return err
        if not tenant:
            return Response({'error': 'Tenant not found'}, status=status.HTTP_404_NOT_FOUND)

        record = self._resolve_record(tenant, praja_id=praja_id)
        if not record:
            return Response(
                {'error': 'Lead record not found for the given praja_id and tenant'},
                status=status.HTTP_404_NOT_FOUND
            )

        membership = TenantMembership.objects.filter(
            tenant=tenant,
            email__iexact=email_id,
            is_active=True
        ).first()
        if not membership:
            return Response(
                {'error': f'No active tenant membership found for email {email_id}'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Store event in DB for audit trail and debugging (independent of background_jobs)
        partner_event = PartnerEvent.objects.create(
            tenant=tenant,
            partner_slug=partner_slug,
            event=event,
            payload=dict(data),
            status='pending',
            record=record,
        )

        queue_service = get_queue_service()
        payload = {
            'tenant_id': str(tenant.id),
            'email_id': email_id,
            'partner_slug': partner_slug,
            'event': event,
            'record_id': record.id,
            'partner_event_id': partner_event.id,
        }
        job = queue_service.enqueue_job(
            job_type=JobType.PARTNER_LEAD_ASSIGN,
            payload=payload,
            priority=5,
            tenant_id=str(tenant.id),
        )
        partner_event.job_id = job.id
        partner_event.save(update_fields=['job_id', 'updated_at'])

        logger.info(
            "[PartnerEvents] Stored partner_event_id=%s enqueued job_id=%s record_id=%s email_id=%s partner_slug=%s",
            partner_event.id, job.id, record.id, email_id, partner_slug
        )
        return Response(
            {'job_id': str(job.id), 'message': 'Event accepted'},
            status=status.HTTP_202_ACCEPTED
        )


class PartnerLeadView(APIView):
    """
    Get the current user's lead assigned by a partner (e.g. Halocom).
    Same response shape as GetMyCurrentLeadView for frontend compatibility.
    GET /crm-records/leads/partner/<partner_slug>/
    """
    permission_classes = [IsTenantAuthenticated]

    @extend_schema(
        summary="Get my partner-assigned lead",
        description="Returns the lead assigned to the current user by the given partner (e.g. halocom). Same shape as get next lead.",
        parameters=[OpenApiParameter(name='partner_slug', type=str, location='path', description='Partner slug (e.g. halocom)')],
        responses={
            200: OpenApiResponse(description="Lead found or empty"),
        },
        tags=["Leads", "Partner"]
    )
    def get(self, request, partner_slug):
        user = request.user
        tenant = request.tenant
        if not tenant:
            logger.warning("[PartnerLead] No tenant context")
            return Response({}, status=status.HTTP_200_OK)
        user_identifier = getattr(user, 'supabase_uid', None) or getattr(user, 'email', None)
        if not user_identifier:
            logger.warning("[PartnerLead] No user_identifier (supabase_uid or email)")
            return Response({}, status=status.HTTP_200_OK)

        partner_slug = (partner_slug or '').strip().lower()
        if not partner_slug:
            logger.warning("[PartnerLead] Empty partner_slug")
            return Response({}, status=status.HTTP_200_OK)

        logger.info(
            "[PartnerLead] GET partner_slug=%s tenant=%s user_identifier=%s",
            partner_slug, tenant.slug, user_identifier
        )

        # Partner source can be in data or pyro_data
        current_lead = Record.objects.filter(
            tenant=tenant,
            entity_type='lead',
            data__assigned_to=user_identifier,
            data__lead_stage='ASSIGNED',
        ).filter(
            Q(data__partner_source=partner_slug) | Q(pyro_data__partner_source=partner_slug)
        ).order_by('-updated_at').first()

        if not current_lead:
            # Diagnostic: count partner leads for this tenant and how many match assigned+stage
            partner_leads_count = Record.objects.filter(
                tenant=tenant,
                entity_type='lead',
            ).filter(
                Q(data__partner_source=partner_slug) | Q(pyro_data__partner_source=partner_slug)
            ).count()
            assigned_stage_count = Record.objects.filter(
                tenant=tenant,
                entity_type='lead',
                data__assigned_to=user_identifier,
                data__lead_stage='ASSIGNED',
            ).count()
            logger.info(
                "[PartnerLead] No lead returned. partner_slug=%s tenant=%s user_identifier=%s | "
                "Leads with partner_source=%s: %d | Leads with assigned_to=%s and lead_stage=ASSIGNED: %d. "
                "Record must have assigned_to=user_identifier, lead_stage='ASSIGNED', and partner_source=%s.",
                partner_slug, tenant.slug, user_identifier,
                partner_slug, partner_leads_count, user_identifier, assigned_stage_count, partner_slug
            )
            return Response({}, status=status.HTTP_200_OK)
        current_lead.refresh_from_db()
        logger.info("[PartnerLead] Returning record_id=%s for user_identifier=%s", current_lead.id, user_identifier)
        return Response(_flatten_lead_response(current_lead), status=status.HTTP_200_OK)


class ApiSecretKeySetView(APIView):
    """
    POST: Store an API secret for the current tenant. Value is saved as-is (no hashing).
    Used for /entity/ (X-Secret-Pyro). Requires tenant auth.
    """
    permission_classes = [IsTenantAuthenticated]

    @extend_schema(
        request=OpenApiTypes.OBJECT,
        responses={201: OpenApiResponse(description="Secret saved."), 400: OpenApiResponse(description="Bad request.")},
        description="Store an API secret for the current tenant. Secret is stored as-is (no hashing).",
    )
    def post(self, request):
        secret = (request.data.get("secret") or "").strip()
        if not secret:
            return Response(
                {"error": "Field 'secret' is required and cannot be empty."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        tenant = getattr(request, "tenant", None)
        if not tenant:
            return Response(
                {"error": "Tenant context required."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        description = (request.data.get("description") or "").strip()
        key_id = request.data.get("id")
        if key_id:
            key = ApiSecretKey.objects.filter(id=key_id, tenant=tenant).first()
            if not key:
                return Response(
                    {"error": f"ApiSecretKey id={key_id} not found for this tenant."},
                    status=status.HTTP_404_NOT_FOUND,
                )
            key.secret = secret
            key.secret_key_last4 = secret[-4:] if len(secret) >= 4 else ""
            if description:
                key.description = description
            key.save()
            return Response(
                {
                    "id": key.id,
                    "tenant_slug": tenant.slug,
                    "message": "Secret updated. Use X-Secret-Pyro with your secret for /entity/.",
                },
                status=status.HTTP_200_OK,
            )
        key = ApiSecretKey.objects.create(
            tenant=tenant,
            description=description or "",
            is_active=True,
        )
        key.secret = secret
        key.secret_key_last4 = secret[-4:] if len(secret) >= 4 else ""
        key.save()
        return Response(
            {
                "id": key.id,
                "tenant_slug": tenant.slug,
                "message": "Secret saved. Use X-Secret-Pyro with your secret when calling /entity/.",
            },
            status=status.HTTP_201_CREATED,
        )


class ApiSecretKeyUpdateView(APIView):
    """
    PUT: Update an existing API secret key by id. Secret is stored as-is (no hashing).
    """
    permission_classes = [IsTenantAuthenticated]

    @extend_schema(
        request=OpenApiTypes.OBJECT,
        responses={200: OpenApiResponse(description="Secret updated."), 400: OpenApiResponse(description="Bad request."), 404: OpenApiResponse(description="Key not found.")},
        description="Update secret for an existing API secret key. Secret is stored as-is (no hashing).",
    )
    def put(self, request, pk):
        secret = (request.data.get("secret") or "").strip()
        if not secret:
            return Response(
                {"error": "Field 'secret' is required and cannot be empty."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        tenant = getattr(request, "tenant", None)
        if not tenant:
            return Response(
                {"error": "Tenant context required."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        key = ApiSecretKey.objects.filter(id=pk, tenant=tenant).first()
        if not key:
            return Response(
                {"error": f"ApiSecretKey id={pk} not found for this tenant."},
                status=status.HTTP_404_NOT_FOUND,
            )
        key.secret = secret
        key.secret_key_last4 = secret[-4:] if len(secret) >= 4 else ""
        key.save()
        return Response(
            {
                "id": key.id,
                "tenant_slug": tenant.slug,
                "message": "Secret updated. Use X-Secret-Pyro with your secret for /entity/.",
            },
            status=status.HTTP_200_OK,
        )


class PrajaLeadsAPIView(APIView):
    """
    Single API endpoint for all lead CRUD operations.
    
    Supports 5 operations via different methods:
    - POST: CREATE a new lead
    - GET: READ all leads (with optional filters)
    - PATCH: UPDATE lead fields (partial update, requires praja_id in query or body)
    - PUT: UPDATE lead fields (full/partial update, requires praja_id in query or body)
    - DELETE: DELETE a lead (requires praja_id in query or body)
    
    Note: praja_id should be stored in the data JSON field when creating leads.
    
    Lead scoring (Praja): POST creates and PATCH/PUT updates recalculate ``data.lead_score`` from rules here only—not on generic Record saves or other CRM APIs.
    
    Requires X-Secret-Pyro header for authentication.
    Automatically uses DEFAULT_TENANT_SLUG from settings (no X-Tenant-Slug header needed).
    Does NOT require IsTenantAuthenticated - uses HasAPISecret instead.
    """
    authentication_classes = []  # No authentication required - only secret header
    permission_classes = [HasAPISecret]
    
    def get_entity_type(self, request):
        """Get entity_type from query params, request body, or default to 'lead'"""
        entity_type = request.query_params.get('entity') or request.data.get('entity_type')
        return entity_type if entity_type else 'lead'
    
    def _get_tenant(self, request):
        """
        Helper to get tenant based on priority:
        1. tenant_id from request (query params or body) - highest priority
        2. Tenant from ApiSecretKey database lookup (if secret key is in database)
        3. Fallback to default tenant from settings
        """
        from django.conf import settings
        
        # Priority 1: Check if tenant_id is provided in query params or request data
        tenant_id = request.query_params.get('tenant_id') or request.data.get('tenant_id')
        if tenant_id:
            try:
                tenant = Tenant.objects.get(id=tenant_id)
                logger.info(f"[PrajaLeadsAPI] Using tenant from request: {tenant.slug} (id={tenant_id})")
                return tenant, None
            except Tenant.DoesNotExist:
                return None, Response(
                    {'error': f'Tenant with id {tenant_id} not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            except (ValueError, TypeError):
                return None, Response(
                    {'error': f'Invalid tenant_id format: {tenant_id}'},
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        # Priority 2: Check if secret key maps to a tenant in database
        api_secret_obj = getattr(request, 'api_secret_obj', None)
        if api_secret_obj and api_secret_obj.tenant:
            tenant = api_secret_obj.tenant
            logger.info(f"[PrajaLeadsAPI] Using tenant from database secret key mapping: {tenant.slug} (id={tenant.id})")
            return tenant, None
        
        # Priority 3: Fallback to default tenant from settings
        default_slug = getattr(settings, 'DEFAULT_TENANT_SLUG', 'bibhab-thepyro-ai')
        
        try:
            tenant = Tenant.objects.get(slug=default_slug)
            logger.info(f"[PrajaLeadsAPI] Using default tenant: {default_slug}")
            return tenant, None
        except Tenant.DoesNotExist:
            # Fallback to first tenant if default doesn't exist
            tenant = Tenant.objects.first()
            if tenant:
                logger.warning(f"[PrajaLeadsAPI] Default tenant '{default_slug}' not found, using first tenant: {tenant.slug}")
                return tenant, None
            else:
                return None, Response(
                    {'error': 'No tenant found in database'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

    def _maybe_recalculate_lead_score(self, record, tenant, *, context: str):
        """Apply scoring rules synchronously for lead records only (no background job)."""
        if record.entity_type != "lead":
            return
        try:
            score = calculate_and_update_lead_score(record, tenant_id=tenant.id, save=True)
            logger.info(
                "[PrajaLeadsAPI] %s: id=%s tenant=%s score=%s",
                context,
                record.id,
                tenant.slug,
                score,
            )
        except Exception as e:
            logger.error(
                "[PrajaLeadsAPI] Error calculating lead score for record %s (%s): %s",
                record.id,
                context,
                e,
            )

    def _prepare_entity_create_request(self, request):
        """
        Normalize POST body like CREATE on /entity/. Returns a dict with tenant, entity_type,
        request_data, praja_id; or a Response on tenant resolution failure.
        """
        tenant, error_response = self._get_tenant(request)
        if error_response:
            return error_response

        entity_type = self.get_entity_type(request)

        request_data = request.data.copy()
        request_data.pop('tenant_id', None)
        if 'name' in request_data:
            if 'data' not in request_data:
                request_data['data'] = {}
            elif not isinstance(request_data['data'], dict):
                request_data['data'] = {}
            request_data['data']['name'] = request_data.pop('name')

        if entity_type == "lead":
            if 'data' not in request_data or not isinstance(request_data.get('data'), dict):
                request_data['data'] = {}
            if 'lead_stage' not in request_data['data'] or request_data['data'].get('lead_stage') in (None, '', 'null'):
                request_data['data']['lead_stage'] = 'FRESH'
            if 'call_attempts' not in request_data['data'] or request_data['data'].get('call_attempts') in (None, '', 'null'):
                request_data['data']['call_attempts'] = 0

        request_data['entity_type'] = entity_type

        request_lead_data = request_data.get('data') if isinstance(request_data.get('data'), dict) else {}
        praja_id = request_lead_data.get('praja_id')
        logger.info(
            "[PrajaLeadsAPI] Incoming CREATE: tenant=%s entity_type=%s praja_id=%s",
            getattr(tenant, "slug", None), entity_type, praja_id,
        )
        return {
            "tenant": tenant,
            "entity_type": entity_type,
            "request_data": request_data,
            "praja_id": praja_id,
        }

    def _execute_entity_create(self, prepared):
        """
        Upsert entity from output of _prepare_entity_create_request.

        If a record with the same (tenant, entity_type, praja_id) already exists,
        merge the incoming data into it and return 200. Otherwise create a new
        record and return 201.
        """
        tenant = prepared["tenant"]
        entity_type = prepared["entity_type"]
        request_data = prepared["request_data"]
        praja_id = prepared["praja_id"]

        incoming_data = request_data.get('data') if isinstance(request_data.get('data'), dict) else {}

        if praja_id:
            existing_record = Record.objects.filter(
                data__praja_id=praja_id,
                tenant=tenant,
                entity_type=entity_type,
            ).first()

            if existing_record:
                logger.info(
                    "[PrajaLeadsAPI] Upsert – updating existing %s: id=%s praja_id=%s tenant=%s",
                    entity_type, existing_record.id, praja_id, tenant.slug,
                )
                data = existing_record.data.copy() if existing_record.data else {}
                data.update(incoming_data)
                existing_record.data = data
                existing_record.updated_at = timezone.now()
                existing_record.save(update_fields=['data', 'updated_at'])

                self._maybe_recalculate_lead_score(
                    existing_record,
                    tenant,
                    context=f"POST upsert praja_id={praja_id}",
                )
                existing_record.refresh_from_db()

                return Response(
                    RecordSerializer(existing_record).data,
                    status=status.HTTP_200_OK,
                )

        serializer = RecordSerializer(data=request_data)
        if serializer.is_valid():
            try:
                with transaction.atomic():
                    record = serializer.save(
                        tenant=tenant,
                        entity_type=entity_type
                    )
            except IntegrityError:
                return Response(
                    {'error': 'Duplicate record (conflict on unique constraint)'},
                    status=status.HTTP_409_CONFLICT,
                )

            record_name = (record.data or {}).get('name', '')

            logger.info(
                "[PrajaLeadsAPI] Created %s: id=%s tenant=%s name=%s",
                entity_type,
                record.id,
                tenant.slug,
                record_name
            )
            self._maybe_recalculate_lead_score(
                record,
                tenant,
                context=f"POST create praja_id={praja_id}",
            )

            if entity_type == 'lead':
                try:
                    from background_jobs.queue_service import get_queue_service
                    from background_jobs.models import JobType

                    lead_data = record.data or {}
                    user_id = lead_data.get('praja_id') or lead_data.get('user_id') or str(record.id)
                    event_name = 'pyro_crm_lead_created'

                    logger.info("=" * 80)
                    logger.info(f"🚀 [Mixpanel] Creating lead {record.id} via PrajaLeadsAPI, sending to Mixpanel")
                    logger.info(f"   Lead ID: {record.id}")
                    logger.info(f"   Tenant: {tenant.slug} ({tenant.id})")
                    logger.info(f"   User ID: {user_id} (from praja_id={lead_data.get('praja_id')} or user_id={lead_data.get('user_id')})")
                    logger.info(f"   Event: {event_name}")
                    logger.info(f"   Lead Name: {lead_data.get('name', 'N/A')}")
                    logger.info(f"   Phone: {lead_data.get('phone_number', 'N/A')}")
                    logger.info(f"   Lead Stage: {lead_data.get('lead_stage', 'N/A')}")
                    logger.info(f"   Lead Score: {lead_data.get('lead_score', 'N/A')}")
                    logger.info("=" * 80)

                    properties = {
                        'lead_id': record.id,
                        'tenant_id': str(record.tenant.id) if record.tenant else None,
                        'entity_type': record.entity_type,
                        'created_at': record.created_at.isoformat() if record.created_at else None,
                        'updated_at': record.updated_at.isoformat() if record.updated_at else None,
                    }
                    properties.update(lead_data)
                    if record.pyro_data:
                        properties.update(record.pyro_data)

                    queue_service = get_queue_service()
                    queue_service.enqueue_job(
                        job_type=JobType.SEND_MIXPANEL_EVENT,
                        payload={
                            "user_id": str(user_id),
                            "event_name": event_name,
                            "properties": properties
                        },
                        priority=0,
                        tenant_id=str(record.tenant.id) if record.tenant else None,
                        max_attempts=3
                    )
                except Exception as e:
                    logger.error(f"❌ [Mixpanel] Error sending lead {record.id}: {e}")

            record.refresh_from_db()

            return Response(
                RecordSerializer(record).data,
                status=status.HTTP_201_CREATED
            )

        return Response(
            serializer.errors,
            status=status.HTTP_400_BAD_REQUEST
        )

    def post(self, request):
        """
        UPSERT - Create a new record, or update it if one with the same praja_id already exists.

        If a record matching (tenant, entity_type, data.praja_id) is found, its data
        fields are merged with the incoming payload and HTTP 200 is returned.
        Otherwise a new record is created and HTTP 201 is returned.

        Body:
        {
            "name": "Customer Name",
            "tenant_id": "optional-tenant-uuid",
            "data": {
                "praja_id": "PRAJA123",
                "phone_number": "+1234567890",
                "lead_score": 85,
                "lead_stage": "FRESH",
                "poster": "free"
            }
        }

        Defaults for new leads: lead_stage=FRESH, call_attempts=0 (when not provided).
        Note: tenant_id is optional. If not provided, uses DEFAULT_TENANT_SLUG from settings.
        """
        prepared = self._prepare_entity_create_request(request)
        if isinstance(prepared, Response):
            return prepared
        return self._execute_entity_create(prepared)
    
    def get(self, request):
        """
        READ - Get all entity records for the specified tenant.
        
        Query parameters:
        - entity: Entity type (e.g., 'lead', 'ticket') - defaults to 'lead' if not provided
        - record_id or lead_id: Get specific record by ID (optional)
        - page: Page number for pagination
        - page_size: Items per page
        - lead_stage: Filter by lead_stage (optional)
        - affiliated_party: Filter by affiliated_party/lead_type (optional)
        """
        tenant, error_response = self._get_tenant(request)
        if error_response:
            return error_response
        
        entity_type = self.get_entity_type(request)
        
        # If record_id or lead_id is provided, return single record
        record_id = request.query_params.get('record_id') or request.query_params.get('lead_id')
        if record_id:
            try:
                record = Record.objects.get(
                    id=record_id,
                    tenant=tenant,
                    entity_type=entity_type
                )
                return Response(
                    RecordSerializer(record).data,
                    status=status.HTTP_200_OK
                )
            except Record.DoesNotExist:
                return Response(
                    {'error': f'{entity_type.capitalize()} with id {record_id} not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
        
        # Get all records for this tenant and entity type
        queryset = Record.objects.filter(
            tenant=tenant,
            entity_type=entity_type
        )
        
        # Optional filters
        lead_stage = request.query_params.get('lead_stage')
        if lead_stage:
            queryset = queryset.filter(data__lead_stage=lead_stage)
        
        affiliated_party = request.query_params.get('affiliated_party')
        if affiliated_party:
            queryset = queryset.filter(data__affiliated_party=affiliated_party)

        # Order by creation date (newest first)
        queryset = queryset.order_by('-created_at')
        
        # Pagination
        paginator = MetaPageNumberPagination()
        page = paginator.paginate_queryset(queryset, request)
        
        if page is not None:
            serializer = RecordSerializer(page, many=True)
            return paginator.get_paginated_response(serializer.data)
        
        # No pagination
        serializer = RecordSerializer(queryset, many=True)
        return Response({
            'count': queryset.count(),
            'results': serializer.data
        }, status=status.HTTP_200_OK)
    
    def patch(self, request):
        """
        UPDATE - Update lead fields.
        
        Query parameter or body: praja_id (required) - uses praja_id from data field to identify lead
        Body:
        {
            "praja_id": "PRAJA123",  # or use ?praja_id=PRAJA123 in URL
            "lead_score": 95,  # Optional: update lead_score
            "lead_stage": "ASSIGNED",  # Optional: update lead_stage
            "name": "Updated Name",  # Optional: update name
            "data": {  # Optional: update any fields in data JSON
                "lead_score": 95,
                "lead_stage": "ASSIGNED",
                "latest_remarks": "Updated remarks",
                "next_call_at": "2025-12-15T10:00:00Z"
            }
        }
        
        Note: You can update any fields in the data JSON. Fields provided in the root level
        (like lead_score, lead_stage) will be merged into the data JSON. If both root level
        and data object are provided, data object takes precedence.
        """
        tenant, error_response = self._get_tenant(request)
        if error_response:
            return error_response
        
        entity_type = self.get_entity_type(request)
        
        # Get praja_id from query params or body
        praja_id = request.query_params.get('praja_id') or request.data.get('praja_id')
        if not praja_id:
            return Response(
                {'error': 'praja_id is required (in query param or body)'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            record = Record.objects.get(
                data__praja_id=praja_id,
                tenant=tenant,
                entity_type=entity_type
            )
        except Record.DoesNotExist:
            return Response(
                {'error': f'{entity_type.capitalize()} with praja_id {praja_id} not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        except Record.MultipleObjectsReturned:
            return Response(
                {'error': f'Multiple {entity_type}s found with praja_id {praja_id}. Please ensure praja_id is unique.'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Start with existing data
        data = record.data.copy() if record.data else {}
        
        # If 'data' object is provided in request, merge it (takes precedence)
        if 'data' in request.data and isinstance(request.data.get('data'), dict):
            data.update(request.data['data'])
        
        # Handle single task update before processing other fields
        if 'update_task' in request.data:
            update_task_data = request.data['update_task']
            if isinstance(update_task_data, dict) and 'task_name' in update_task_data:
                task_name = update_task_data['task_name']
                new_status = update_task_data.get('status')
                
                # Get existing tasks or initialize empty list
                tasks = data.get('tasks', [])
                if not isinstance(tasks, list):
                    tasks = []
                
                # Find and update the specific task
                task_found = False
                for i, task in enumerate(tasks):
                    if isinstance(task, dict) and task.get('task') == task_name:
                        if new_status is not None:
                            tasks[i]['status'] = new_status
                        task_found = True
                        break
                
                # If task not found, add it
                if not task_found and new_status is not None:
                    tasks.append({'task': task_name, 'status': new_status})
                
                data['tasks'] = tasks
        
        # Also allow root-level fields to be merged into data
        # Common fields that should go into data JSON
        root_fields_to_data = ['name', 'lead_score', 'lead_stage', 'latest_remarks', 'next_call_at', 
                               'assigned_to', 'call_attempts', 'last_active_date_time',
                               'disqualification_reason', 'poster', 'phone_number', 'tasks']
        
        for field in root_fields_to_data:
            if field in request.data:
                data[field] = request.data[field]
        
        # Update the data JSONB field
        record.data = data
        record.updated_at = timezone.now()
        
        # Determine which fields to update
        update_fields = ['data', 'updated_at']
        
        record.save(update_fields=update_fields)
        
        logger.info(
            "[PrajaLeadsAPI] Updated %s: id=%s praja_id=%s tenant=%s fields=%s",
            record.entity_type,
            record.id,
            praja_id,
            tenant.slug,
            list(request.data.keys())
        )
        self._maybe_recalculate_lead_score(
            record,
            tenant,
            context=f"PATCH praja_id={praja_id} fields={list(request.data.keys())}",
        )
        
        # Refresh record from DB to get updated score
        record.refresh_from_db()
        
        return Response(
            RecordSerializer(record).data,
            status=status.HTTP_200_OK
        )
    
    def put(self, request):
        """
        UPDATE - Update entity fields (PUT method).
        
        PUT now performs partial updates with merging, same as PATCH.
        
        Query parameter or body: praja_id (required) - uses praja_id from data field to identify entity
        Body:
        {
            "praja_id": "PRAJA123",  # or use ?praja_id=PRAJA123 in URL
            "lead_score": 95,  # Optional: update lead_score
            "lead_stage": "ASSIGNED",  # Optional: update lead_stage
            "name": "Updated Name",  # Optional: update name
            "data": {  # Optional: update any fields in data JSON
                "lead_score": 95,
                "lead_stage": "ASSIGNED",
                "latest_remarks": "Updated remarks",
                "next_call_at": "2025-12-15T10:00:00Z"
            }
        }
        
        Note: You can update any fields in the data JSON. Fields provided in the root level
        (like lead_score, lead_stage) will be merged into the data JSON. If both root level
        and data object are provided, data object takes precedence.
        """
        tenant, error_response = self._get_tenant(request)
        if error_response:
            return error_response
        
        entity_type = self.get_entity_type(request)
        
        # Get praja_id from query params or body
        praja_id = request.query_params.get('praja_id') or request.data.get('praja_id')
        if not praja_id:
            return Response(
                {'error': 'praja_id is required (in query param or body)'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            record = Record.objects.get(
                data__praja_id=praja_id,
                tenant=tenant,
                entity_type=entity_type
            )
        except Record.DoesNotExist:
            return Response(
                {'error': f'{entity_type.capitalize()} with praja_id {praja_id} not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        except Record.MultipleObjectsReturned:
            return Response(
                {'error': f'Multiple {entity_type}s found with praja_id {praja_id}. Please ensure praja_id is unique.'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Start with existing data
        data = record.data.copy() if record.data else {}
        
        # If 'data' object is provided in request, merge it (takes precedence)
        if 'data' in request.data and isinstance(request.data.get('data'), dict):
            data.update(request.data['data'])
        
        # Handle single task update before processing other fields
        if 'update_task' in request.data:
            update_task_data = request.data['update_task']
            if isinstance(update_task_data, dict) and 'task_name' in update_task_data:
                task_name = update_task_data['task_name']
                new_status = update_task_data.get('status')
                
                # Get existing tasks or initialize empty list
                tasks = data.get('tasks', [])
                if not isinstance(tasks, list):
                    tasks = []
                
                # Find and update the specific task
                task_found = False
                for i, task in enumerate(tasks):
                    if isinstance(task, dict) and task.get('task') == task_name:
                        if new_status is not None:
                            tasks[i]['status'] = new_status
                        task_found = True
                        break
                
                # If task not found, add it
                if not task_found and new_status is not None:
                    tasks.append({'task': task_name, 'status': new_status})
                
                data['tasks'] = tasks
        
        # Also allow root-level fields to be merged into data
        # Common fields that should go into data JSON
        root_fields_to_data = ['name', 'lead_score', 'lead_stage', 'latest_remarks', 'next_call_at', 
                               'assigned_to', 'call_attempts', 'last_active_date_time',
                               'disqualification_reason', 'poster', 'phone_number', 'tasks']
        
        for field in root_fields_to_data:
            if field in request.data:
                data[field] = request.data[field]
        
        # Update the data JSONB field
        record.data = data
        record.updated_at = timezone.now()
        
        # Determine which fields to update
        update_fields = ['data', 'updated_at']
        
        record.save(update_fields=update_fields)
        
        logger.info(
            "[PrajaLeadsAPI] Updated %s: id=%s praja_id=%s tenant=%s fields=%s",
            record.entity_type,
            record.id,
            praja_id,
            tenant.slug,
            list(request.data.keys())
        )
        self._maybe_recalculate_lead_score(
            record,
            tenant,
            context=f"PUT praja_id={praja_id} fields={list(request.data.keys())}",
        )
        
        # Refresh record from DB to get updated score
        record.refresh_from_db()
        
        return Response(
            RecordSerializer(record).data,
            status=status.HTTP_200_OK
        )
    
    def delete(self, request):
        """
        DELETE - Delete a lead remotely.
        
        Query parameter or body: praja_id (required) - uses praja_id from data field to identify lead
        Body:
        {
            "praja_id": "PRAJA123"  # or use ?praja_id=PRAJA123 in URL
        }
        """
        tenant, error_response = self._get_tenant(request)
        if error_response:
            return error_response
        
        entity_type = self.get_entity_type(request)
        
        # Get praja_id from query params or body
        praja_id = request.query_params.get('praja_id') or request.data.get('praja_id')
        if not praja_id:
            return Response(
                {'error': 'praja_id is required (in query param or body)'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            record = Record.objects.get(
                data__praja_id=praja_id,
                tenant=tenant,
                entity_type=entity_type
            )
        except Record.DoesNotExist:
            return Response(
                {'error': f'{entity_type.capitalize()} with praja_id {praja_id} not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        except Record.MultipleObjectsReturned:
            return Response(
                {'error': f'Multiple {entity_type}s found with praja_id {praja_id}. Please ensure praja_id is unique.'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        record_id_for_log = record.id
        record_name = (record.data or {}).get('name', '') if isinstance(record.data, dict) else ''
        record.delete()
        
        logger.info(
            "[PrajaLeadsAPI] Deleted %s: id=%s praja_id=%s tenant=%s name=%s",
            entity_type,
            record_id_for_log,
            praja_id,
            tenant.slug,
            record_name
        )
        
        return Response(
            {'message': f'{entity_type.capitalize()} with praja_id {praja_id} deleted successfully'},
            status=status.HTTP_200_OK
        )


class PrajaLeadEntityBackfillAPIView(PrajaLeadsAPIView):
    """
    Idempotent CREATE for Praja entity payloads: same auth and body as POST /entity/, but if a record
    already exists for (tenant, entity_type, data.praja_id), returns 200 with that record and does not
    create or enqueue Mixpanel. Otherwise creates exactly like POST /entity/.
    """

    http_method_names = ["post", "options"]

    def post(self, request):
        prepared = self._prepare_entity_create_request(request)
        if isinstance(prepared, Response):
            return prepared

        tenant = prepared["tenant"]
        entity_type = prepared["entity_type"]
        praja_id = prepared["praja_id"]
        if isinstance(praja_id, str):
            praja_id = praja_id.strip() or None
        if not praja_id:
            return Response(
                {"error": "praja_id is required in data for entity backfill"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        matching_ids = list(
            Record.objects.filter(
                data__praja_id=praja_id,
                tenant=tenant,
                entity_type=entity_type,
            ).values_list("id", flat=True)[:2]
        )
        if len(matching_ids) > 1:
            return Response(
                {
                    "error": (
                        f"Multiple {entity_type}s found with praja_id {praja_id}. "
                        "Please ensure praja_id is unique."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        if len(matching_ids) == 1:
            record = Record.objects.get(pk=matching_ids[0])
            logger.info(
                "[PrajaLeadsAPI] Backfill skip (already exists): id=%s praja_id=%s tenant=%s",
                record.id,
                praja_id,
                tenant.slug,
            )
            payload = dict(RecordSerializer(record).data)
            payload["backfill_skipped"] = True
            return Response(payload, status=status.HTTP_200_OK)

        return self._execute_entity_create(prepared)


class EntityTypeSchemaListCreateView(TenantScopedMixin, generics.ListCreateAPIView):
    """
    List all entity type schemas for the current tenant, or create a new one.
    
    GET /crm-records/entity-schemas/
    POST /crm-records/entity-schemas/
    """
    permission_classes = [IsTenantAuthenticated]
    serializer_class = EntityTypeSchemaSerializer
    pagination_class = MetaPageNumberPagination
    
    def get_queryset(self):
        """Return schemas filtered by tenant."""
        return EntityTypeSchema.objects.filter(tenant=self.request.tenant).order_by('entity_type')
    
    def perform_create(self, serializer):
        """Set tenant automatically on create."""
        serializer.save(tenant=self.request.tenant)


class EntityTypeSchemaDetailView(TenantScopedMixin, generics.RetrieveUpdateDestroyAPIView):
    """
    Retrieve, update, or delete an entity type schema.
    
    GET /crm-records/entity-schemas/{id}/
    PUT /crm-records/entity-schemas/{id}/
    PATCH /crm-records/entity-schemas/{id}/
    DELETE /crm-records/entity-schemas/{id}/
    """
    permission_classes = [IsTenantAuthenticated]
    serializer_class = EntityTypeSchemaSerializer
    
    def get_queryset(self):
        """Return schemas filtered by tenant."""
        return EntityTypeSchema.objects.filter(tenant=self.request.tenant)


class EntityTypeSchemaByTypeView(TenantScopedMixin, APIView):
    """
    Get or create/update entity type schema by entity_type.
    
    GET /crm-records/entity-schemas/by-type/?entity_type=lead
    POST /crm-records/entity-schemas/by-type/ - with entity_type and attributes in body
    PUT /crm-records/entity-schemas/by-type/ - update existing schema
    """
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request):
        """Get schema by entity_type."""
        entity_type = request.query_params.get('entity_type')
        
        if not entity_type:
            return Response({
                'error': 'entity_type query parameter is required'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            schema = EntityTypeSchema.objects.get(
                tenant=request.tenant,
                entity_type=entity_type.strip()
            )
            serializer = EntityTypeSchemaSerializer(schema)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except EntityTypeSchema.DoesNotExist:
            return Response({
                'error': f'Schema not found for entity_type "{entity_type}"'
            }, status=status.HTTP_404_NOT_FOUND)
    
    def post(self, request):
        """Create a new schema."""
        serializer = EntityTypeSchemaSerializer(data=request.data)
        if serializer.is_valid():
            # Check if schema already exists
            entity_type = serializer.validated_data.get('entity_type')
            existing = EntityTypeSchema.objects.filter(
                tenant=request.tenant,
                entity_type=entity_type
            ).first()
            
            if existing:
                return Response({
                    'error': f'Schema already exists for entity_type "{entity_type}". Use PUT to update.'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            serializer.save(tenant=request.tenant)
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def put(self, request):
        """Update existing schema or create if not exists."""
        entity_type = request.data.get('entity_type')
        
        if not entity_type:
            return Response({
                'error': 'entity_type is required in request body'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            schema = EntityTypeSchema.objects.get(
                tenant=request.tenant,
                entity_type=entity_type.strip()
            )
            serializer = EntityTypeSchemaSerializer(schema, data=request.data, partial=False)
        except EntityTypeSchema.DoesNotExist:
            serializer = EntityTypeSchemaSerializer(data=request.data)
        
        if serializer.is_valid():
            serializer.save(tenant=request.tenant)
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def patch(self, request):
        """Partially update existing schema."""
        entity_type = request.data.get('entity_type')
        
        if not entity_type:
            return Response({
                'error': 'entity_type is required in request body'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            schema = EntityTypeSchema.objects.get(
                tenant=request.tenant,
                entity_type=entity_type.strip()
            )
            serializer = EntityTypeSchemaSerializer(schema, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_200_OK)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except EntityTypeSchema.DoesNotExist:
            return Response({
                'error': f'Schema not found for entity_type "{entity_type}"'
            }, status=status.HTTP_404_NOT_FOUND)


class EntityTypeAttributesView(TenantScopedMixin, APIView):
    """
    Get attributes list for an entity type.
    
    GET /crm-records/entity-attributes/?entity_type=lead
    
    Returns a simple list of attributes for the specified entity_type.
    """
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request):
        """
        Get attributes list by entity_type.
        
        Query Parameters:
        - entity_type: Required. The entity type to get attributes for (e.g., 'lead', 'ticket')
        
        Returns:
        {
            "entity_type": "lead",
            "attributes": [
                "id",
                "tenant_id",
                "entity_type",
                "data",
                "data.name",
                "data.praja_id",
                "data.lead_score",
                ...
            ],
            "total_count": 29
        }
        """
        entity_type = request.query_params.get('entity_type')
        
        if not entity_type:
            return Response({
                'error': 'entity_type query parameter is required. Example: ?entity_type=lead'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        entity_type = entity_type.strip()
        
        try:
            schema = EntityTypeSchema.objects.get(
                tenant=request.tenant,
                entity_type=entity_type
            )
            
            return Response({
                'entity_type': schema.entity_type,
                'attributes': schema.attributes,
                'total_count': len(schema.attributes)
            }, status=status.HTTP_200_OK)
            
        except EntityTypeSchema.DoesNotExist:
            return Response({
                'error': f'Schema not found for entity_type "{entity_type}"',
                'entity_type': entity_type,
                'suggestion': 'Create a schema first using POST /crm-records/entity-schemas/'
            }, status=status.HTTP_404_NOT_FOUND)


class LeadScoringView(TenantScopedMixin, APIView):
    """
    POST endpoint to save scoring rules and queue a background job to score all leads.
    
    POST /crm-records/leads/score/
    
    Saves the rules to EntityTypeSchema table and enqueues SCORE_LEADS for the worker.
    
    Payload:
    {
        "rules": [
            {
                "attr": "data.assigned_to",
                "operator": "==",
                "value": "ami",
                "weight": 19900
            },
            {
                "attr": "data.affiliated_party",
                "operator": "==",
                "value": "bjp",
                "weight": 1233
            }
        ]
    }
    
    For each lead, checks all rules and sums up weights for matching rules.
    Updates data.lead_score with the total weight.
    """
    permission_classes = [IsTenantAuthenticated]
    
    def _get_nested_value(self, data, attr_path):
        """
        Get nested value from data dict using dot notation path.
        Example: data.assigned_to -> data['assigned_to']
        Example: data.user.profile.name -> data['user']['profile']['name']
        """
        if not attr_path or not data:
            return None
        
        # Remove 'data.' prefix if present
        if attr_path.startswith('data.'):
            attr_path = attr_path[5:]  # Remove 'data.' prefix
        
        keys = attr_path.split('.')
        value = data
        
        try:
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return None
            return value
        except (TypeError, KeyError, AttributeError):
            return None
    
    def _evaluate_rule(self, lead_data, rule):
        """
        Evaluate if a rule matches the lead data.
        
        Args:
            lead_data: The data dict from the lead record
            rule: Dict with 'attr', 'operator', 'value', 'weight'
        
        Returns:
            True if rule matches, False otherwise
        """
        attr_path = rule.get('attr', '')
        operator = rule.get('operator', '==')
        expected_value = rule.get('value', '')
        
        # Get the actual value from lead data
        actual_value = self._get_nested_value(lead_data, attr_path)
        
        if actual_value is None:
            return False
        
        # Convert to string for comparison (handles different types)
        actual_str = str(actual_value).lower() if actual_value is not None else ''
        expected_str = str(expected_value).lower() if expected_value is not None else ''
        
        try:
            if operator == '==':
                return actual_str == expected_str
            elif operator == '!=':
                return actual_str != expected_str
            elif operator == '>':
                return float(actual_value) > float(expected_value)
            elif operator == '<':
                return float(actual_value) < float(expected_value)
            elif operator == '>=':
                return float(actual_value) >= float(expected_value)
            elif operator == '<=':
                return float(actual_value) <= float(expected_value)
            elif operator == 'contains':
                return expected_str in actual_str
            elif operator == 'in':
                # expected_value should be a comma-separated list or list
                if isinstance(expected_value, list):
                    return actual_str in [str(v).lower() for v in expected_value]
                else:
                    values = [v.strip().lower() for v in str(expected_value).split(',')]
                    return actual_str in values
            else:
                return False
        except (ValueError, TypeError):
            # If conversion fails, fall back to string comparison
            if operator in ['==', '!=']:
                return actual_str == expected_str if operator == '==' else actual_str != expected_str
            return False
    
    def post(self, request):
        """
        Save scoring rules and enqueue SCORE_LEADS to apply them to all leads in the background.
        Saves/updates the rules in EntityTypeSchema table for the entity_type.
        """
        entity_type = 'lead'  # Default entity type for lead scoring
        
        # Check if rules exist in ScoringRule table first
        scoring_rules_count = ScoringRule.objects.filter(
            tenant=request.tenant,
            entity_type=entity_type,
            is_active=True
        ).count()
        
        if scoring_rules_count > 0:
            # Rules are already saved individually in ScoringRule table, use them
            logger.info(f"LeadScoringView: Found {scoring_rules_count} active rules in ScoringRule table, using them for scoring")
            rules = []  # Empty rules array - will be read from ScoringRule table by scoring logic
        else:
            # No rules in ScoringRule table, check if rules provided in request (backward compatibility)
            serializer = LeadScoringRequestSerializer(data=request.data)
            
            if not serializer.is_valid():
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
            
            rules = serializer.validated_data['rules']
            
            if not rules or len(rules) == 0:
                return Response(
                    {'error': 'No scoring rules found. Please create rules first.'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Save rules to ScoringRule table (for backward compatibility when rules sent in request)
            created_rules = []
            try:
                # Delete existing rules for this tenant/entity_type
                deleted_count = ScoringRule.objects.filter(
                    tenant=request.tenant,
                    entity_type=entity_type
                ).delete()[0]
                logger.info(f"LeadScoringView: Deleted {deleted_count} existing rules from ScoringRule table")
                
                # Create new rules from request
                for idx, rule in enumerate(rules):
                    try:
                        scoring_rule = ScoringRule.objects.create(
                            tenant=request.tenant,
                            entity_type=entity_type,
                            attribute=rule.get('attr', ''),
                            data={
                                'operator': rule.get('operator', '=='),
                                'value': rule.get('value', ''),
                            },
                            weight=rule.get('weight', 0),
                            order=idx,
                            is_active=True
                        )
                        created_rules.append(scoring_rule)
                        logger.debug(f"LeadScoringView: Created ScoringRule {scoring_rule.id}: {scoring_rule.attribute}")
                    except Exception as e:
                        logger.error(f"LeadScoringView: Error creating ScoringRule for rule {idx}: {e}", exc_info=True)
                        continue
                
                logger.info(f"LeadScoringView: Saved {len(created_rules)}/{len(rules)} rules to ScoringRule table from request")
            except Exception as e:
                logger.error(f"LeadScoringView: Error saving rules to ScoringRule table: {e}", exc_info=True)
            
            # Also save to EntityTypeSchema for backward compatibility
            EntityTypeSchema.objects.update_or_create(
                tenant=request.tenant,
                entity_type=entity_type,
                defaults={
                    'rules': rules
                }
            )
            
            logger.info(f"LeadScoringView: Saved {len(rules)} rules to EntityTypeSchema for entity_type '{entity_type}'")
        
        total_leads = Record.objects.filter(
            tenant=request.tenant,
            entity_type='lead',
        ).count()
        
        from background_jobs.models import BackgroundJob, JobStatus

        existing_job = (
            BackgroundJob.objects.filter(
                tenant_id=str(request.tenant.id),
                job_type=JobType.SCORE_LEADS,
                status__in=[JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.RETRYING],
                payload__entity_type=entity_type,
            )
            .order_by("-created_at")
            .first()
        )

        queue_service = get_queue_service()
        if existing_job:
            logger.info(
                "LeadScoringView: SCORE_LEADS already in progress for tenant=%s entity_type=%s (job_id=%s)",
                request.tenant.id,
                entity_type,
                existing_job.id,
            )
            job = existing_job
        else:
            job = queue_service.enqueue_job(
                job_type=JobType.SCORE_LEADS,
                payload={
                    "entity_type": entity_type,
                    "batch_size": 100,
                },
                priority=0,
                tenant_id=str(request.tenant.id),
            )
        
        logger.info(
            "LeadScoringView: Enqueued background job %s for %s leads (tenant=%s)",
            job.id,
            total_leads,
            request.tenant.id,
        )
        
        return Response(
            {
                'message': f'Rules saved. Background job created to score {total_leads} leads',
                'job_id': job.id,
                'status': job.status,
                'total_leads': total_leads,
                'progress': 0,
            },
            status=status.HTTP_202_ACCEPTED,
        )
    
    def get(self, request):
        """
        Get status of lead scoring (SCORE_LEADS) background jobs.
        
        Query params:
        - job_id: Get specific job status (optional)
        """
        from background_jobs.models import BackgroundJob, JobType, JobStatus
        
        job_id = request.query_params.get('job_id')

        def _aggregate_parent_job(parent_job: BackgroundJob) -> dict:
            """
            Aggregate progress across SCORE_LEADS_CHUNK jobs when present.
            Falls back to the parent job.result fields for backward compatibility.
            """
            result = parent_job.result or {}

            chunk_job_ids = result.get("chunk_job_ids") or []
            total_chunks = result.get("total_chunks") or len(chunk_job_ids)

            if not chunk_job_ids:
                # Backward compatible behavior for older SCORE_LEADS jobs.
                progress = result.get("progress_percentage", 0)
                return {
                    "status": parent_job.status.lower() if isinstance(parent_job.status, str) else parent_job.status,
                    "progress_percentage": progress,
                    "total_leads": result.get("total_leads", 0),
                    "processed_leads": result.get("processed_leads", 0),
                    "updated_leads": result.get("updated_leads", 0),
                    "total_score_added": result.get("total_score_added", 0.0),
                }

            chunk_rows = BackgroundJob.objects.filter(
                tenant_id=request.tenant.id,
                job_type=JobType.SCORE_LEADS_CHUNK,
                id__in=chunk_job_ids,
            ).values("status", "result")

            completed_chunks = 0
            processed_sum = 0
            updated_sum = 0
            total_score_sum = 0.0

            for row in chunk_rows:
                if row.get("status") == JobStatus.COMPLETED:
                    completed_chunks += 1
                    r = row.get("result") or {}
                    processed_sum += int(r.get("processed_leads", 0) or 0)
                    updated_sum += int(r.get("updated_leads", 0) or 0)
                    total_score_sum += float(r.get("total_score_added", 0.0) or 0.0)

            if total_chunks:
                progress_percentage = int((completed_chunks / total_chunks) * 100)
            else:
                progress_percentage = 100

            overall_status = "completed" if total_chunks and completed_chunks >= total_chunks else "processing"

            return {
                "status": overall_status,
                "progress_percentage": progress_percentage,
                "total_leads": result.get("total_leads", 0),
                "processed_leads": processed_sum,
                "updated_leads": updated_sum,
                "total_score_added": total_score_sum,
            }
        
        if job_id:
            try:
                job = BackgroundJob.objects.get(
                    id=job_id,
                    tenant_id=request.tenant.id,
                    job_type=JobType.SCORE_LEADS
                )
                
                aggregated = _aggregate_parent_job(job)
                return Response(
                    {
                        "job_id": job.id,
                        "status": aggregated["status"],
                        "total_leads": aggregated["total_leads"],
                        "processed_leads": aggregated["processed_leads"],
                        "updated_leads": aggregated["updated_leads"],
                        "total_score_added": aggregated["total_score_added"],
                        "progress_percentage": aggregated["progress_percentage"],
                        "error_message": job.last_error,
                        "attempts": job.attempts,
                        "max_attempts": job.max_attempts,
                        "created_at": job.created_at.isoformat(),
                        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                    },
                    status=status.HTTP_200_OK,
                )
            except BackgroundJob.DoesNotExist:
                return Response({
                    'error': f'Job with id {job_id} not found'
                }, status=status.HTTP_404_NOT_FOUND)
        
        # Get all lead scoring jobs for tenant
        jobs = BackgroundJob.objects.filter(
            tenant_id=request.tenant.id,
            job_type=JobType.SCORE_LEADS
        ).order_by('-created_at')[:10]  # Latest 10 jobs
        
        jobs_data = []
        for job in jobs:
            aggregated = _aggregate_parent_job(job)
            jobs_data.append({
                'job_id': job.id,
                'status': aggregated['status'],
                'total_leads': aggregated['total_leads'],
                'processed_leads': aggregated['processed_leads'],
                'updated_leads': aggregated['updated_leads'],
                'progress_percentage': aggregated['progress_percentage'],
                'created_at': job.created_at.isoformat(),
                'completed_at': job.completed_at.isoformat() if job.completed_at else None
            })
        
        return Response({
            'jobs': jobs_data,
            'count': len(jobs_data)
        }, status=status.HTTP_200_OK)


class CallAttemptMatrixListCreateView(TenantScopedMixin, generics.ListCreateAPIView):
    """
    List and create Call Attempt Matrix configurations.
    """
    queryset = CallAttemptMatrix.objects.all()
    serializer_class = CallAttemptMatrixSerializer
    permission_classes = [IsTenantAuthenticated]
    
    @extend_schema(
        summary="List or create call attempt matrix configurations",
        description="Get all call attempt matrix configurations for the current tenant, or create a new one.",
        tags=["Call Attempt Matrix"]
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)
    
    @extend_schema(
        summary="Create call attempt matrix configuration",
        description="Create a new call attempt matrix configuration for a lead type.",
        tags=["Call Attempt Matrix"]
    )
    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)


class CallAttemptMatrixDetailView(TenantScopedMixin, generics.RetrieveUpdateDestroyAPIView):
    """
    Retrieve, update, or delete a specific Call Attempt Matrix configuration.
    """
    queryset = CallAttemptMatrix.objects.all()
    serializer_class = CallAttemptMatrixSerializer
    permission_classes = [IsTenantAuthenticated]
    lookup_field = 'pk'
    
    @extend_schema(
        summary="Get call attempt matrix configuration",
        description="Retrieve a specific call attempt matrix configuration by ID.",
        tags=["Call Attempt Matrix"]
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)
    
    @extend_schema(
        summary="Update call attempt matrix configuration",
        description="Update a specific call attempt matrix configuration.",
        tags=["Call Attempt Matrix"]
    )
    def put(self, request, *args, **kwargs):
        return super().put(request, *args, **kwargs)
    
    @extend_schema(
        summary="Partially update call attempt matrix configuration",
        description="Partially update a specific call attempt matrix configuration.",
        tags=["Call Attempt Matrix"]
    )
    def patch(self, request, *args, **kwargs):
        return super().patch(request, *args, **kwargs)
    
    @extend_schema(
        summary="Delete call attempt matrix configuration",
        description="Delete a specific call attempt matrix configuration.",
        tags=["Call Attempt Matrix"]
    )
    def delete(self, request, *args, **kwargs):
        return super().delete(request, *args, **kwargs)


class CallAttemptMatrixByLeadTypeView(TenantScopedMixin, APIView):
    """
    Get call attempt matrix configuration by lead type.
    """
    permission_classes = [IsTenantAuthenticated]
    
    @extend_schema(
        summary="Get call attempt matrix by lead type",
        description="Retrieve call attempt matrix configuration for a specific lead type.",
        parameters=[
            OpenApiParameter(
                name="lead_type",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Lead type (e.g., 'BJP', 'AAP', 'Congress')"
            )
        ],
        tags=["Call Attempt Matrix"]
    )
    def get(self, request):
        tenant = request.tenant
        if not tenant:
            return Response(
                {'error': 'No tenant context available'},
                status=status.HTTP_403_FORBIDDEN
            )
        
        lead_type = request.query_params.get('lead_type')
        if not lead_type:
            return Response(
                {'error': 'lead_type parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            matrix = CallAttemptMatrix.objects.get(
                tenant=tenant,
                lead_type=lead_type.strip()
            )
            serializer = CallAttemptMatrixSerializer(matrix)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except CallAttemptMatrix.DoesNotExist:
            return Response(
                {'error': f'Call attempt matrix not found for lead type: {lead_type}'},
                status=status.HTTP_404_NOT_FOUND
            )


class LeadAssignmentWebhookProxyView(TenantScopedMixin, APIView):
    """
    Proxy endpoint for lead assignment webhooks.
    Forwards webhook requests from frontend to external webhook URLs to avoid CORS issues.
    """
    permission_classes = [IsTenantAuthenticated]
    
    @extend_schema(
        summary="Forward lead assignment webhook",
        description="Proxy endpoint that forwards lead assignment events to external webhook URLs. "
                    "This endpoint avoids CORS issues by making the webhook request from the backend.",
        request={
            'application/json': {
                'type': 'object',
                'properties': {
                    'webhook_url': {
                        'type': 'string',
                        'format': 'uri',
                        'description': 'The external webhook URL to forward the payload to'
                    },
                    'payload': {
                        'type': 'object',
                        'description': 'The payload to send to the webhook URL'
                    }
                },
                'required': ['webhook_url', 'payload']
            }
        },
        responses={
            200: OpenApiResponse(
                description="Webhook forwarded successfully",
                response={
                    'type': 'object',
                    'properties': {
                        'success': {'type': 'boolean'},
                        'status_code': {'type': 'integer'},
                        'message': {'type': 'string'}
                    }
                }
            ),
            400: OpenApiResponse(description="Bad request - missing webhook_url or payload"),
            500: OpenApiResponse(description="Internal server error")
        },
        tags=["Webhooks"]
    )
    def post(self, request, *args, **kwargs):
        """
        Forward webhook request to external URL.
        
        Expected request body:
        {
            "webhook_url": "https://webhook.site/...",
            "payload": {
                "event": "lead.assigned",
                "timestamp": "...",
                "lead": {...},
                "user": {...},
                "assignment_time": "..."
            }
        }
        """
        try:
            webhook_url = request.data.get('webhook_url')
            payload = request.data.get('payload')
            
            if not webhook_url:
                return Response(
                    {'error': 'webhook_url is required'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            if not payload:
                return Response(
                    {'error': 'payload is required'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Validate webhook_url is a valid URL
            if not (webhook_url.startswith('http://') or webhook_url.startswith('https://')):
                return Response(
                    {'error': 'webhook_url must be a valid HTTP/HTTPS URL'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Forward the request to the external webhook URL
            try:
                response = requests.post(
                    webhook_url,
                    json=payload,
                    headers={
                        'Content-Type': 'application/json',
                    },
                    timeout=10  # 10 second timeout
                )
                
                # Log the webhook attempt
                logger.info(
                    f"Webhook forwarded to {webhook_url}: "
                    f"status={response.status_code}, "
                    f"tenant={request.tenant.id if hasattr(request, 'tenant') else 'unknown'}"
                )
                
                # Send Mixpanel event for lead assignment
                try:
                    lead_data = payload.get('lead', {}) if isinstance(payload, dict) else {}
                    user_data = payload.get('user', {}) if isinstance(payload, dict) else {}
                    user_id = user_data.get('id') if isinstance(user_data, dict) else None
                    
                    logger.info(f"[Mixpanel] Preparing to send lead assignment event: user_id={user_id}, lead_id={lead_data.get('id') if isinstance(lead_data, dict) else None}")
                    
                    if user_id and lead_data:
                        # For lead events, use praja_id from lead_data as user_id instead of tenant_membership.id
                        praja_id = lead_data.get('praja_id') if isinstance(lead_data, dict) else None
                        
                        if praja_id:
                            # Convert praja_id to appropriate format for Mixpanel
                            # Handle different formats: integer, numeric string, or string like "PRAJA123"
                            try:
                                if isinstance(praja_id, int):
                                    # Already an integer, use directly
                                    mixpanel_user_id = str(praja_id)
                                elif isinstance(praja_id, str):
                                    # Try to extract numeric part from strings like "PRAJA123", "PRAJA-123", or just "123"
                                    cleaned = praja_id.upper().replace("PRAJA", "").replace("-", "").replace("_", "").strip()
                                    if cleaned.isdigit():
                                        # Convert to integer string if it's all digits
                                        mixpanel_user_id = cleaned
                                    else:
                                        # If not all digits, use as-is
                                        mixpanel_user_id = praja_id
                                else:
                                    # Other types, convert to string
                                    mixpanel_user_id = str(praja_id)
                            except (ValueError, TypeError, AttributeError) as e:
                                logger.warning(f"[Mixpanel] Failed to convert praja_id={praja_id}: {e}, using as-is")
                                mixpanel_user_id = str(praja_id) if praja_id else praja_id
                            
                            logger.info(f"[Mixpanel] Using praja_id={mixpanel_user_id} as user_id for lead assignment event (original user_id={user_id})")
                        else:
                            # Fallback to original logic if praja_id not available
                            mixpanel_user_id = user_id
                            try:
                                # If user_id is a UUID, look up TenantMembership to get integer id
                                user_uuid = uuid.UUID(str(user_id))
                                tenant = getattr(request, 'tenant', None)
                                if tenant:
                                    tenant_membership = TenantMembership.objects.filter(
                                        tenant=tenant,
                                        user_id=user_uuid
                                    ).first()
                                    if tenant_membership:
                                        mixpanel_user_id = str(tenant_membership.id)  # Use integer id from TenantMembership
                                        logger.info(f"[Mixpanel] Resolved UUID {user_id} to TenantMembership.id {mixpanel_user_id}")
                                    else:
                                        logger.warning(f"[Mixpanel] TenantMembership not found for UUID {user_id} in tenant {tenant.id}, using UUID as-is")
                                else:
                                    logger.warning(f"[Mixpanel] No tenant found on request, using UUID as-is")
                            except (ValueError, AttributeError, TypeError) as e:
                                # user_id is not a UUID, use as-is (might be integer string)
                                logger.debug(f"[Mixpanel] user_id {user_id} is not a UUID ({e}), using as-is")
                            logger.warning(f"[Mixpanel] No praja_id found in lead_data, falling back to user_id={mixpanel_user_id}")
                        
                        mixpanel_service = MixpanelService()
                        mixpanel_properties = {
                            'lead_id': lead_data.get('id'),
                            'lead_name': lead_data.get('name'),
                            'lead_status': lead_data.get('lead_status'),
                            'lead_score': lead_data.get('lead_score'),
                            'lead_type': lead_data.get('lead_type'),
                            'assigned_to': lead_data.get('assigned_to'),
                            'assignment_time': payload.get('assignment_time'),
                            'timestamp': payload.get('timestamp'),
                        }
                        # Add all lead attributes to Mixpanel properties
                        mixpanel_properties.update(lead_data)
                        
                        logger.info(f"[Mixpanel] Calling send_to_mixpanel_sync with event='pyro_crm_rm_assigned_backend', user_id={mixpanel_user_id} (original={user_id})")
                        mixpanel_result = mixpanel_service.send_to_mixpanel_sync(
                            str(mixpanel_user_id),
                            'pyro_crm_rm_assigned_backend',
                            mixpanel_properties
                        )
                        
                        if mixpanel_result:
                            logger.info(f"✅ [Mixpanel] Event sent successfully for lead assignment: lead_id={lead_data.get('id')}, user_id={mixpanel_user_id}")
                        else:
                            logger.warning(f"⚠️ [Mixpanel] Event sending returned False for lead assignment: lead_id={lead_data.get('id')}, user_id={mixpanel_user_id}")
                    else:
                        logger.warning(f"[Mixpanel] Skipping Mixpanel event - missing user_id or lead_data: user_id={user_id}, has_lead_data={bool(lead_data)}")
                except Exception as mixpanel_error:
                    # Don't fail the webhook if Mixpanel fails
                    logger.error(f"❌ [Mixpanel] Exception while sending lead assignment event: {str(mixpanel_error)}", exc_info=True)
                
                # Return success response
                return Response({
                    'success': response.ok,
                    'status_code': response.status_code,
                    'message': 'Webhook forwarded successfully' if response.ok else 'Webhook forwarding completed with non-2xx status'
                }, status=status.HTTP_200_OK)
                
            except requests.exceptions.Timeout:
                logger.error(f"Webhook timeout for {webhook_url}")
                return Response(
                    {'error': 'Webhook request timed out'},
                    status=status.HTTP_504_GATEWAY_TIMEOUT
                )
            except requests.exceptions.RequestException as e:
                logger.error(f"Webhook forwarding error for {webhook_url}: {str(e)}")
                return Response(
                    {'error': f'Failed to forward webhook: {str(e)}'},
                    status=status.HTTP_502_BAD_GATEWAY
                )
                
        except Exception as e:
            logger.error(f"Unexpected error in webhook proxy: {str(e)}")
            return Response(
                {'error': f'Internal error: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class RMAssignedMixpanelView(TenantScopedMixin, APIView):
    """
    API endpoint to send RM assigned events to Mixpanel
    """
    permission_classes = [IsTenantAuthenticated]
    
    @extend_schema(
        summary="Send RM assigned event to Mixpanel",
        description="Sends an event to Mixpanel via the rm_assigned endpoint when a lead is assigned to an RM",
        request={
            'application/json': {
                'type': 'object',
                'properties': {
                    'praja_id': {
                        'type': 'integer',
                        'description': 'Praja ID for the Mixpanel event',
                        'example': 123
                    },
                    'rm_email': {
                        'type': 'string',
                        'description': 'RM email address',
                        'example': 'sai.venkat@praja.buzz'
                    }
                },
                'required': ['praja_id', 'rm_email']
            }
        },
        responses={
            200: OpenApiResponse(
                description="Event sent successfully",
                examples=[
                    OpenApiExample(
                        name="Success",
                        value={"success": True, "message": "Event sent to Mixpanel"}
                    )
                ]
            ),
            400: OpenApiResponse(
                description="Bad request",
                examples=[
                    OpenApiExample(
                        name="Missing Fields",
                        value={"error": "praja_id and rm_email are required"}
                    )
                ]
            )
        },
        tags=["Mixpanel"]
    )
    def post(self, request):
        """
        Send RM assigned event to Mixpanel
        """
        try:
            user = request.user
            
            # Get praja_id and rm_email from request data
            praja_id = request.data.get('praja_id')
            rm_email = request.data.get('rm_email')
            
            # If rm_email not provided, get from authenticated user
            if not rm_email:
                rm_email = getattr(user, 'email', None)
            
            if not praja_id or not rm_email:
                return Response(
                    {'error': 'praja_id and rm_email are required'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Ensure praja_id is an integer
            try:
                praja_id_int = int(praja_id)
            except (ValueError, TypeError):
                return Response(
                    {'error': 'praja_id must be a valid integer'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Send to Mixpanel - praja_id is sent as user_id in the payload
            service = RMAssignedMixpanelService()
            success = service.send_to_mixpanel_sync(
                praja_id_int,
                rm_email
            )
            
            if success:
                return Response(
                    {'success': True, 'message': 'Event sent to Mixpanel'},
                    status=status.HTTP_200_OK
                )
            else:
                return Response(
                    {'success': False, 'message': 'Failed to send event to Mixpanel'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
                
        except Exception as e:
            logger.error(f"Error sending RM assigned Mixpanel event: {str(e)}", exc_info=True)
            return Response(
                {'error': f'Internal error: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class ScoringRuleListCreateView(TenantScopedMixin, generics.ListCreateAPIView):
    """
    List all scoring rules for the current tenant, or create a new one.
    
    GET /crm-records/scoring-rules/?entity_type=lead
    POST /crm-records/scoring-rules/
    """
    permission_classes = [IsTenantAuthenticated]
    serializer_class = ScoringRuleModelSerializer
    # pagination_class = MetaPageNumberPagination
    
    def get_queryset(self):
        """Return rules filtered by tenant and optionally by entity_type."""
        queryset = ScoringRule.objects.filter(tenant=self.request.tenant)
        
        # Filter by entity_type if provided in query params
        entity_type = self.request.query_params.get('entity_type')
        if entity_type:
            queryset = queryset.filter(entity_type=entity_type)
        
        # Filter by is_active if provided
        is_active = self.request.query_params.get('is_active')
        if is_active is not None:
            is_active_bool = is_active.lower() in ('true', '1', 'yes')
            queryset = queryset.filter(is_active=is_active_bool)
        
        return queryset.order_by('order', 'created_at')
    
    def perform_create(self, serializer):
        """Set tenant automatically on create and enqueue SCORE_LEADS to re-score records."""
        rule = serializer.save(tenant=self.request.tenant)
        logger.info(f"ScoringRuleListCreateView: Created rule {rule.id} ({rule.attribute}) for tenant {self.request.tenant.id}")
        self._trigger_scoring_job(rule.entity_type)
    
    def _trigger_scoring_job(self, entity_type: str):
        try:
            total_leads = Record.objects.filter(
                tenant=self.request.tenant,
                entity_type=entity_type,
            ).count()
            if total_leads == 0:
                logger.debug(
                    "ScoringRuleListCreateView: No records to score for entity_type '%s'",
                    entity_type,
                )
                return

            from background_jobs.models import BackgroundJob, JobStatus

            existing_job = (
                BackgroundJob.objects.filter(
                    tenant_id=str(self.request.tenant.id),
                    job_type=JobType.SCORE_LEADS,
                    status__in=[JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.RETRYING],
                    payload__entity_type=entity_type,
                )
                .order_by("-created_at")
                .first()
            )
            if existing_job:
                logger.info(
                    "ScoringRuleListCreateView: SCORE_LEADS already in progress for tenant=%s entity_type=%s (job_id=%s)",
                    self.request.tenant.id,
                    entity_type,
                    existing_job.id,
                )
                return

            queue_service = get_queue_service()
            job = queue_service.enqueue_job(
                job_type=JobType.SCORE_LEADS,
                payload={
                    'entity_type': entity_type,
                    'batch_size': 100,
                },
                priority=0,
                tenant_id=str(self.request.tenant.id),
            )
            logger.info(
                "ScoringRuleListCreateView: Enqueued job %s to re-score %s records (%s)",
                job.id,
                total_leads,
                entity_type,
            )
        except Exception as e:
            logger.error(
                "ScoringRuleListCreateView: Error enqueueing scoring job: %s",
                e,
                exc_info=True,
            )


class ScoringRuleDetailView(TenantScopedMixin, generics.RetrieveUpdateDestroyAPIView):
    """
    Retrieve, update, or delete a scoring rule.
    
    GET /crm-records/scoring-rules/<id>/
    PUT /crm-records/scoring-rules/<id>/
    PATCH /crm-records/scoring-rules/<id>/
    DELETE /crm-records/scoring-rules/<id>/
    """
    permission_classes = [IsTenantAuthenticated]
    serializer_class = ScoringRuleModelSerializer
    
    def get_queryset(self):
        """Return rules filtered by tenant."""
        return ScoringRule.objects.filter(tenant=self.request.tenant)
    
    def update(self, request, *args, **kwargs):
        """Handle PUT/PATCH requests with better error handling."""
        try:
            partial = kwargs.pop('partial', False)
            instance = self.get_object()
            serializer = self.get_serializer(instance, data=request.data, partial=partial)
            serializer.is_valid(raise_exception=True)
            self.perform_update(serializer)
            
            if getattr(instance, '_prefetched_objects_cache', None):
                instance._prefetched_objects_cache = {}
            
            return Response(serializer.data)
        except Exception as e:
            logger.error(f"ScoringRuleDetailView.update: Error updating rule: {e}", exc_info=True)
            return Response(
                {'error': f'Failed to update rule: {str(e)}'},
                status=status.HTTP_400_BAD_REQUEST
            )
    
    def destroy(self, request, *args, **kwargs):
        """Handle DELETE requests with better error handling."""
        try:
            instance = self.get_object()
            self.perform_destroy(instance)
            return Response(status=status.HTTP_204_NO_CONTENT)
        except Exception as e:
            logger.error(f"ScoringRuleDetailView.destroy: Error deleting rule: {e}", exc_info=True)
            return Response(
                {'error': f'Failed to delete rule: {str(e)}'},
                status=status.HTTP_400_BAD_REQUEST
            )
    
    def perform_update(self, serializer):
        """Update rule and ensure tenant is preserved; enqueue SCORE_LEADS to re-score records."""
        rule = serializer.save(tenant=self.request.tenant)
        logger.info(f"ScoringRuleDetailView: Updated rule {rule.id} ({rule.attribute}) for tenant {self.request.tenant.id}")
        self._trigger_scoring_job(rule.entity_type)
    
    def perform_destroy(self, instance):
        """Delete rule and enqueue SCORE_LEADS to re-score records."""
        rule_id = instance.id
        rule_attribute = instance.attribute
        entity_type = instance.entity_type
        instance.delete()
        logger.info(f"ScoringRuleDetailView: Deleted rule {rule_id} ({rule_attribute}) for tenant {self.request.tenant.id}")
        self._trigger_scoring_job(entity_type)
    
    def _trigger_scoring_job(self, entity_type: str):
        try:
            total_leads = Record.objects.filter(
                tenant=self.request.tenant,
                entity_type=entity_type,
            ).count()
            if total_leads == 0:
                logger.debug(
                    "ScoringRuleDetailView: No records to score for entity_type '%s'",
                    entity_type,
                )
                return

            from background_jobs.models import BackgroundJob, JobStatus

            existing_job = (
                BackgroundJob.objects.filter(
                    tenant_id=str(self.request.tenant.id),
                    job_type=JobType.SCORE_LEADS,
                    status__in=[JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.RETRYING],
                    payload__entity_type=entity_type,
                )
                .order_by("-created_at")
                .first()
            )
            if existing_job:
                logger.info(
                    "ScoringRuleDetailView: SCORE_LEADS already in progress for tenant=%s entity_type=%s (job_id=%s)",
                    self.request.tenant.id,
                    entity_type,
                    existing_job.id,
                )
                return

            queue_service = get_queue_service()
            job = queue_service.enqueue_job(
                job_type=JobType.SCORE_LEADS,
                payload={
                    'entity_type': entity_type,
                    'batch_size': 100,
                },
                priority=0,
                tenant_id=str(self.request.tenant.id),
            )
            logger.info(
                "ScoringRuleDetailView: Enqueued job %s to re-score %s records (%s)",
                job.id,
                total_leads,
                entity_type,
            )
        except Exception as e:
            logger.error(
                "ScoringRuleDetailView: Error enqueueing scoring job: %s",
                e,
                exc_info=True,
            )


