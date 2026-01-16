from rest_framework import generics, status, serializers
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import AllowAny
from authz.permissions import IsTenantAuthenticated
from core.pagination import MetaPageNumberPagination
from core.models import Tenant
from django.utils import timezone
from datetime import datetime, time, timedelta
from django.db.models import Q, F
from django.db import transaction
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiExample, OpenApiParameter
from drf_spectacular.types import OpenApiTypes
import logging

logger = logging.getLogger(__name__)
from .models import Record, EventLog, RuleSet, RuleExecutionLog, EntityTypeSchema
from .serializers import RecordSerializer, EventLogSerializer, RuleSetSerializer, RuleExecutionLogSerializer, EntityTypeSchemaSerializer, LeadScoringRequestSerializer
from .mixins import TenantScopedMixin
from .events import dispatch_event
from .scoring import calculate_and_update_lead_score
from user_settings.models import UserSettings
from .permissions import HasAPISecret
from support_ticket.services import MixpanelService
from user_settings.routing import apply_routing_rule_to_queryset


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
        - resolution_status: Filter by resolution_status in data JSON
        - affiliated_party: Filter by affiliated_party in data JSON (supports comma-separated values: ?affiliated_party=value1,value2)
        - Any other field: Will be searched in the data JSON field
        
        Examples:
        - ?entity_type=lead&resolution_status=WIP
        - ?entity_type=ticket&resolution_status=Scheduled
        - ?affiliated_party=Channel Partner,Direct
        - ?priority=high&status=active
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
        model_fields = {'entity_type', 'search', 'search_fields', 'page', 'page_size', 'ordering', 'created_at__gte', 'created_at__lte'}
        data_filters = {k: v for k, v in query_params.items() if k not in model_fields}
        
        # Build Q objects for JSON field filtering
        q_objects = Q()
        for field_name, field_value in data_filters.items():
            # Support multiple values for the same field (comma-separated)
            if ',' in field_value:
                values = [v.strip() for v in field_value.split(',')]
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
            
        return queryset
    
    def perform_create(self, serializer):
        """
        Create record with tenant and entity_type assignment.
        entity_type can come from query params or request body.
        Automatically calculates and saves lead score if entity_type is 'lead'.
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
        
        # Calculate and save lead score if entity_type is 'lead'
        if entity_type == 'lead':
            try:
                from .scoring import calculate_and_update_lead_score
                score = calculate_and_update_lead_score(record, tenant_id=self.request.tenant.id, save=True)
                logger.debug(f"RecordListCreateView: Calculated lead score {score} for new lead {record.id}")
            except Exception as e:
                logger.error(f"RecordListCreateView: Error calculating lead score for new lead {record.id}: {e}")
                # Don't fail the request if scoring fails, just log the error
    
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
        updated_record = serializer.save(tenant=self.request.tenant)
        
        # Calculate and save lead score if entity_type is 'lead'
        if updated_record.entity_type == 'lead':
            try:
                from .scoring import calculate_and_update_lead_score
                score = calculate_and_update_lead_score(updated_record, tenant_id=self.request.tenant.id, save=True)
                logger.debug(f"RecordListCreateView: Calculated lead score {score} for updated lead {updated_record.id}")
                # Refresh serializer data to include updated score
                serializer = self.get_serializer(updated_record)
            except Exception as e:
                logger.error(f"RecordListCreateView: Error calculating lead score for updated lead {updated_record.id}: {e}")
                # Don't fail the request if scoring fails, just log the error
        
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
        updated_record = serializer.save(tenant=self.request.tenant)
        
        # Calculate and save lead score if entity_type is 'lead'
        if updated_record.entity_type == 'lead':
            try:
                from .scoring import calculate_and_update_lead_score
                score = calculate_and_update_lead_score(updated_record, tenant_id=self.request.tenant.id, save=True)
                logger.debug(f"RecordListCreateView: Calculated lead score {score} for patched lead {updated_record.id}")
                # Refresh serializer data to include updated score
                serializer = self.get_serializer(updated_record)
            except Exception as e:
                logger.error(f"RecordListCreateView: Error calculating lead score for patched lead {updated_record.id}: {e}")
                # Don't fail the request if scoring fails, just log the error
        
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
        Update record and calculate lead score if entity_type is 'lead'.
        """
        updated_record = serializer.save()
        
        # Calculate and save lead score if entity_type is 'lead'
        if updated_record.entity_type == 'lead':
            try:
                from .scoring import calculate_and_update_lead_score
                score = calculate_and_update_lead_score(updated_record, tenant_id=self.request.tenant.id, save=True)
                logger.debug(f"RecordDetailView: Calculated lead score {score} for updated lead {updated_record.id}")
            except Exception as e:
                logger.error(f"RecordDetailView: Error calculating lead score for updated lead {updated_record.id}: {e}")
                # Don't fail the request if scoring fails, just log the error


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
        """Create record with tenant and the specific entity type. Automatically calculates lead score if entity_type is 'lead'."""
        record = serializer.save(
            tenant=self.request.tenant,
            entity_type=self.entity_type
        )
        
        # Calculate and save lead score if entity_type is 'lead'
        if self.entity_type == 'lead':
            try:
                from .scoring import calculate_and_update_lead_score
                score = calculate_and_update_lead_score(record, tenant_id=self.request.tenant.id, save=True)
                logger.debug(f"EntityProxyView: Calculated lead score {score} for new lead {record.id}")
            except Exception as e:
                logger.error(f"EntityProxyView: Error calculating lead score for new lead {record.id}: {e}")
                # Don't fail the request if scoring fails, just log the error


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
                # If this is a lead record, increment call_attempts when "Not Connected" is clicked
                # (call_attempts is stored inside the record.data JSON).
                payload_event = payload or {}
                event_name_norm = (event_name or "").strip().lower()
                call_status_norm = str(payload_event.get("call_status", "")).strip().lower()
                last_call_outcome_norm = str(payload_event.get("last_call_outcome", "")).strip().lower()
                button_type_norm = str(payload_event.get("button_type", "")).strip().lower()

                is_not_connected_event = (
                    ("not_connected" in event_name_norm)
                    or event_name_norm in {
                        "not_connected",
                        "not_connected_clicked",
                        "not-connected",
                        "not connected",
                    }
                    or button_type_norm in {"not_connected", "not connected", "not-connected"}
                    or call_status_norm in {"not connected", "not_connected", "notconnected"}
                    or last_call_outcome_norm in {"not connected", "not_connected", "notconnected"}
                )

                if record.entity_type == "lead" and is_not_connected_event:
                    record_locked = Record.objects.select_for_update().get(id=record.id, tenant=tenant)
                    data = record_locked.data.copy() if record_locked.data else {}

                    prev_attempts = data.get("call_attempts", 0)
                    try:
                        prev_attempts_int = int(prev_attempts) if prev_attempts is not None else 0
                    except (TypeError, ValueError):
                        prev_attempts_int = 0

                    data["call_attempts"] = prev_attempts_int + 1
                    record_locked.data = data
                    record_locked.updated_at = timezone.now()
                    record_locked.save(update_fields=["data", "updated_at"])

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
                            "in_queue": 27,
                            "assigned": 26,
                            "call_later": 22,
                            "scheduled": 15,
                            "won": 6,
                            "lost": 2,
                            "closed": 2
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
                "in_queue": 0,
                "assigned": 0,
                "call_later": 0,
                "scheduled": 0,
                "won": 0,
                "lost": 0,
                "closed": 0
            }, status=status.HTTP_200_OK)
        
        # Get all leads for this tenant
        leads = Record.objects.filter(tenant=tenant, entity_type='lead')
        
        # Count by stage
        stats = {
            "total_leads": leads.count(),
            "in_queue": 0,
            "assigned": 0,
            "call_later": 0,
            "scheduled": 0,
            "won": 0,
            "lost": 0,
            "closed": 0
        }
        
        # Count by stage
        for lead in leads:
            stage = lead.data.get('lead_stage') if lead.data else None
            if stage in stats:
                stats[stage] += 1
        
        return Response(stats, status=status.HTTP_200_OK)


class GetNextLeadView(APIView):
    """
    Get and assign the next available lead from the queue for CRM records.
    Atomically fetches and assigns the highest-scoring unassigned lead to the caller.
    """
    permission_classes = [IsTenantAuthenticated]
    
    QUEUEABLE_STATUSES = ('in_queue', 'assigned', 'call_later', 'scheduled')
    ASSIGNED_STATUS = 'assigned'
    
    def _affiliated_party_aliases(self, lead_type: str):
        """
        Normalize known affiliated party type typos/synonyms so filtering matches real data.
        Keep both canonical and legacy spellings to be safe.
        """
        aliases = {
            # common typo observed in data/user settings
            'in_trail': ['in_trial', 'in_trail'],
            'in_trial': ['in_trial', 'in_trail'],
        }
        return aliases.get(lead_type, [lead_type])
    
    def _order_by_score(self, qs, now_iso=None):
        """
        Order queryset with priority: expired snoozed leads first, then by lead score, then creation date.
        Higher scores first (100, 90, 80, etc. - descending), then older creation dates.
        """
        # Priority 1: Expired snoozed leads (lead_stage='SNOOZED' AND next_call_at <= now)
        # Priority 2: Regular leads ordered by score
        # Order by score if it exists in the data field, then by creation date
        # Using PostgreSQL JSONB operators for ordering
        # Score ordering: 100, 90, 80, 70, etc. (descending)
        if now_iso:
            qs = qs.extra(
                select={
                    'lead_score': "COALESCE((data->>'lead_score')::float, -1)",
                    'is_expired_snoozed': """
                        CASE 
                            WHEN data->>'lead_stage' = 'SNOOZED' 
                            AND data->>'next_call_at' IS NOT NULL 
                            AND data->>'next_call_at' != '' 
                            AND data->>'next_call_at' != 'null'
                            AND (data->>'next_call_at')::timestamptz <= NOW()
                            THEN 0
                            ELSE 1
                        END
                    """,
                }
            ).order_by(
                'is_expired_snoozed',  # Expired snoozed leads first (0), then others (1)
                F('lead_score').desc(nulls_last=True),  # Descending: 100, 90, 80, etc.
                'created_at',
                'id'
            )
        else:
            # Fallback when now_iso is not provided - just order by score
            qs = qs.extra(
                select={
                    'lead_score': "COALESCE((data->>'lead_score')::float, -1)",
                }
            ).order_by(
                F('lead_score').desc(nulls_last=True),  # Descending: 100, 90, 80, etc.
                'created_at',
                'id'
            )
        return qs
    
    @extend_schema(
        summary="Get next lead from queue",
        description="Atomically fetches and assigns the next available lead from the queue for CRM records. "
                   "Logic: 1) Get user's info from request 2) Check RM's eligible lead types from user settings "
                   "3) Filter leads by eligible lead types (affiliated_party field) 4) Order by lead score (100, 90, 80 descending) "
                   "5) Return first entry.",
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
                                    "lead_stage": "assigned",
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

        if not tenant:
            logger.warning("[GetNextLead] No tenant context available")
            return Response({}, status=status.HTTP_200_OK)

        # Step 1: Get user identifier (supabase_uid or email)
        user_identifier = getattr(user, 'supabase_uid', None) or getattr(user, 'email', None)

        if not user_identifier:
            logger.warning("[GetNextLead] No user identifier available")
            return Response({}, status=status.HTTP_200_OK)

        logger.info("[GetNextLead] Getting next lead for user: %s", user_identifier)

        # Get current time for checking snoozed leads expiration
        from django.utils import timezone
        now = timezone.now()
        now_iso = now.isoformat()

        # Step 2: Check the RM is eligible for what leads - get from user settings
        eligible_lead_types = []
        user_uuid = None
        daily_limit = None
        try:
            import uuid
            try:
                user_uuid = uuid.UUID(str(user_identifier))
                logger.debug("[GetNextLead] User identifier %s parsed as UUID: %s", user_identifier, user_uuid)
            except (ValueError, AttributeError):
                from accounts.models import LegacyUser
                legacy_user = LegacyUser.objects.filter(
                    tenant=tenant,
                    email=user_identifier
                ).first()
                user_uuid = legacy_user.uid if legacy_user and legacy_user.uid else None
                logger.debug("[GetNextLead] Resolved user_uuid from LegacyUser: %s", user_uuid)

            if user_uuid:
                # Find TenantMembership for this user
                from authz.models import TenantMembership
                import uuid
                tenant_membership = TenantMembership.objects.filter(
                    tenant=tenant,
                    user_id=uuid.UUID(str(user_uuid))
                ).first()
                
                if tenant_membership:
                    # Daily limit is a user-level attribute; fetch from any user_settings row for this user
                    any_setting = UserSettings.objects.filter(tenant=tenant, tenant_membership=tenant_membership).first()
                    daily_limit = getattr(any_setting, "daily_limit", None) if any_setting else None

                    try:
                        setting = UserSettings.objects.get(
                            tenant=tenant,
                            tenant_membership=tenant_membership,
                            key='LEAD_TYPE_ASSIGNMENT'
                        )
                        eligible_lead_types = setting.value if isinstance(setting.value, list) else []
                        logger.info("[GetNextLead] Found eligible lead types for user %s: %s", user_identifier, eligible_lead_types)
                    except UserSettings.DoesNotExist:
                        logger.info("[GetNextLead] No lead type assignment found for user %s - will return no leads", user_identifier)
                        eligible_lead_types = []
                else:
                    logger.warning("[GetNextLead] TenantMembership not found for user UUID %s", user_uuid)
                    eligible_lead_types = []
                    daily_limit = None
            else:
                logger.warning("[GetNextLead] Could not resolve user UUID for %s", user_identifier)
        except Exception as e:
            logger.error("[GetNextLead] Error fetching user settings: %s", str(e))
            eligible_lead_types = []
            daily_limit = None

        # If user has no eligible lead types assigned, return empty
        if not eligible_lead_types:
            logger.info("[GetNextLead] User %s has no eligible lead types assigned", user_identifier)
            
            # --- Enhanced Logging Block ---
            from django.db.models import Q
            # Count all leads in any queueable state regardless of assignment (including expired snoozed)
            possible_leads_cnt = Record.objects.filter(
                tenant=tenant,
                entity_type='lead'
            ).extra(
                where=["""
                    data->>'lead_stage' IN ('in_queue', 'assigned', 'call_later', 'scheduled')
                    OR data->>'lead_stage' IS NULL
                    OR (
                        data->>'lead_stage' = 'SNOOZED'
                        AND data->>'next_call_at' IS NOT NULL
                        AND data->>'next_call_at' != ''
                        AND data->>'next_call_at' != 'null'
                        AND (data->>'next_call_at')::timestamptz <= NOW()
                    )
                """]
            ).count()
            # Count those unassigned
            possible_unassigned_cnt = Record.objects.filter(
                tenant=tenant,
                entity_type='lead'
            ).extra(
                where=["""
                    (
                        (data->>'assigned_to' IS NULL OR 
                         data->>'assigned_to' = '' OR
                         data->>'assigned_to' = 'null' OR
                         data->>'assigned_to' = 'None')
                        OR data->>'lead_stage' = 'in_queue'
                    )
                    AND data->>'affiliated_party' IS NOT NULL
                    AND data->>'affiliated_party' != ''
                    AND data->>'affiliated_party' != 'null'
                    AND (
                        data->>'lead_stage' IN ('in_queue', 'assigned', 'call_later', 'scheduled')
                        OR data->>'lead_stage' IS NULL
                        OR (
                            data->>'lead_stage' = 'SNOOZED'
                            AND data->>'next_call_at' IS NOT NULL
                            AND data->>'next_call_at' != ''
                            AND data->>'next_call_at' != 'null'
                            AND (data->>'next_call_at')::timestamptz <= NOW()
                        )
                    )
                """]
            ).count()
            logger.info("[GetNextLead] Diagnostic: queueable leads for tenant=%s: count_in_queueable_state=%d, count_unassigned=%d, eligible_lead_types=None", tenant, possible_leads_cnt, possible_unassigned_cnt)
            # --- End Enhanced Logging Block ---
            
            return Response({}, status=status.HTTP_200_OK)

        # Step 2.5: Enforce daily lead pull limit (if configured)
        # Count how many leads this user has been assigned today for this tenant.
        # We use Record.updated_at (assignment updates updated_at) as the time signal.
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
                assigned_today = Record.objects.filter(
                    tenant=tenant,
                    entity_type='lead',
                    data__assigned_to=user_identifier,
                    updated_at__gte=start_of_day,
                ).count()

                if assigned_today >= daily_limit_int:
                    logger.info(
                        "[GetNextLead] Daily limit reached for user=%s (assigned_today=%d, daily_limit=%d). Returning empty.",
                        user_identifier,
                        assigned_today,
                        daily_limit_int,
                    )
                    # Fallback: still allow "not connected" follow-up leads, prioritized by call attempts
                    # (attempt 1 first, then 2, then 3...), without assigning new leads.
                    retry_candidate = Record.objects.filter(
                        tenant=tenant,
                        entity_type="lead",
                        data__assigned_to=user_identifier,
                    ).extra(
                        select={
                            "call_attempts_int": "COALESCE((data->>'call_attempts')::int, 0)",
                            "lead_stage_norm": "UPPER(COALESCE(data->>'lead_stage',''))",
                            "last_call_outcome_norm": "LOWER(COALESCE(data->>'last_call_outcome',''))",
                        },
                        where=[
                            """
                            COALESCE((data->>'call_attempts')::int, 0) >= 1
                            AND COALESCE((data->>'call_attempts')::int, 0) <= 3
                            AND (
                                UPPER(COALESCE(data->>'lead_stage','')) = 'NOT_CONNECTED'
                                OR LOWER(COALESCE(data->>'last_call_outcome','')) IN ('not connected', 'not_connected', 'notconnected')
                            )
                            AND (
                                data->>'lead_stage' IN ('assigned', 'call_later', 'scheduled', 'SNOOZED', 'in_queue', 'NOT_CONNECTED')
                                OR data->>'lead_stage' IS NULL
                            )
                            """
                        ],
                    ).order_by(
                        "call_attempts_int",
                        "updated_at",
                        "id",
                    ).first()

                    if not retry_candidate:
                        logger.info(
                            "[GetNextLead] Daily limit reached and no retryable not-connected leads found for user=%s",
                            user_identifier,
                        )
                        return Response({}, status=status.HTTP_200_OK)

                    # Serialize and flatten for frontend compatibility (same format as normal GetNextLead)
                    serialized_data = RecordSerializer(retry_candidate).data
                    lead_data = retry_candidate.data or {}
                    flattened_response = {
                        "id": retry_candidate.id,
                        "name": (retry_candidate.data or {}).get('name', '') if isinstance(retry_candidate.data, dict) else '',
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
                        "record": serialized_data,
                    }

                    logger.info(
                        "[GetNextLead] Daily limit fallback returning not-connected lead: record_id=%s user=%s call_attempts=%s",
                        retry_candidate.id,
                        user_identifier,
                        flattened_response.get("attempt_count"),
                    )
                    return Response(flattened_response, status=status.HTTP_200_OK)

        # Step 3: Filter leads by eligible lead types (affiliated_party field) and unassigned status
        from django.db.models import Q
        
        base_qs = Record.objects.filter(
            tenant=tenant,
            entity_type='lead'
        ).extra(
            where=["""
                (
                    -- Unassigned leads
                    (data->>'assigned_to' IS NULL OR 
                     data->>'assigned_to' = '' OR
                     data->>'assigned_to' = 'null' OR
                     data->>'assigned_to' = 'None')
                    -- OR in_queue status (can be assigned or unassigned)
                    OR data->>'lead_stage' = 'in_queue'
                    -- OR expired snoozed leads (should be available regardless of assigned_to)
                    OR (
                        data->>'lead_stage' = 'SNOOZED'
                        AND data->>'next_call_at' IS NOT NULL
                        AND data->>'next_call_at' != ''
                        AND data->>'next_call_at' != 'null'
                        AND (data->>'next_call_at')::timestamptz <= NOW()
                    )
                )
                AND data->>'affiliated_party' IS NOT NULL
                AND data->>'affiliated_party' != ''
                AND data->>'affiliated_party' != 'null'
                AND (
                    -- Regular queueable statuses
                    data->>'lead_stage' IN ('in_queue', 'assigned', 'call_later', 'scheduled')
                    OR data->>'lead_stage' IS NULL
                    -- OR snoozed leads where next_call_at has passed
                    OR (
                        data->>'lead_stage' = 'SNOOZED'
                        AND data->>'next_call_at' IS NOT NULL
                        AND data->>'next_call_at' != ''
                        AND data->>'next_call_at' != 'null'
                        AND (data->>'next_call_at')::timestamptz <= NOW()
                    )
                )
                """]
        )

        # Apply optional per-user routing rule for leads (e.g. by state)
        if user_uuid:
            base_qs = apply_routing_rule_to_queryset(
                base_qs,
                tenant=tenant,
                user_id=user_uuid,
                queue_type="lead",
        )

        # Filter by eligible lead types (affiliated_party field must match one of the eligible types)
        affiliated_party_filter = Q()
        for lead_type in eligible_lead_types:
            for alias in self._affiliated_party_aliases(lead_type):
                affiliated_party_filter |= Q(data__affiliated_party=alias)
        unassigned = base_qs.filter(affiliated_party_filter)

        logger.info("[GetNextLead] Filtered unassigned leads by eligible types: %s", eligible_lead_types)

        # --- Enhanced Diagnostics: Log possible unassigned counts for debugging ---
        unassigned_cnt = unassigned.count()
        total_unassigned_cnt = base_qs.count()
        logger.info("[GetNextLead] Diagnostic: total_unassigned_in_queueable=%d, unassigned_matching_types=%d for user=%s",
                    total_unassigned_cnt, unassigned_cnt, user_identifier)

        if unassigned_cnt == 0 and total_unassigned_cnt > 0:
            # There are unassigned queueable leads, but none matching the user's eligible lead types
            lead_types_in_queue = list(base_qs.values_list("data__affiliated_party", flat=True).distinct())
            logger.info(
                "[GetNextLead] No unassigned leads matching user's eligible types. Present types in queueable/unassigned leads: %s. User eligible types: %s",
                lead_types_in_queue, eligible_lead_types
            )
        elif total_unassigned_cnt == 0:
            logger.info("[GetNextLead] There are currently no unassigned leads in any queueable status for tenant=%s", tenant)
            # Relaxed fallback: drop lead_stage filter to recover from inconsistent/missing stages
            # But still include snoozed leads where next_call_at has passed
            relaxed_qs = Record.objects.filter(
                tenant=tenant,
                entity_type='lead'
            ).extra(
                where=["""
                    (
                        -- Unassigned leads
                        (data->>'assigned_to' IS NULL OR 
                         data->>'assigned_to' = '' OR
                         data->>'assigned_to' = 'null' OR
                         data->>'assigned_to' = 'None')
                        -- OR in_queue status (can be assigned or unassigned)
                        OR data->>'lead_stage' = 'in_queue'
                        -- OR expired snoozed leads (should be available regardless of assigned_to)
                        OR (
                            data->>'lead_stage' = 'SNOOZED'
                            AND data->>'next_call_at' IS NOT NULL
                            AND data->>'next_call_at' != ''
                            AND data->>'next_call_at' != 'null'
                            AND (data->>'next_call_at')::timestamptz <= NOW()
                        )
                    )
                    AND data->>'affiliated_party' IS NOT NULL
                    AND data->>'affiliated_party' != ''
                    AND data->>'affiliated_party' != 'null'
                    AND (
                        -- Regular queueable statuses
                        data->>'lead_stage' IN ('in_queue', 'assigned', 'call_later', 'scheduled')
                        OR data->>'lead_stage' IS NULL
                        -- OR snoozed leads where next_call_at has passed
                        OR (
                            data->>'lead_stage' = 'SNOOZED'
                            AND data->>'next_call_at' IS NOT NULL
                            AND data->>'next_call_at' != ''
                            AND data->>'next_call_at' != 'null'
                            AND (data->>'next_call_at')::timestamptz <= NOW()
                        )
                    )
                """]
            )
            relaxed_unassigned = relaxed_qs.filter(affiliated_party_filter)
            relaxed_cnt = relaxed_unassigned.count()
            if relaxed_cnt > 0:
                logger.info("[GetNextLead] Relaxed fallback found %d unassigned leads ignoring lead_stage filter", relaxed_cnt)
                unassigned = relaxed_unassigned
                unassigned_cnt = relaxed_cnt

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
                AND data->>'next_call_at' IS NOT NULL
                AND data->>'next_call_at' != ''
                AND data->>'next_call_at' != 'null'
                AND (data->>'next_call_at')::timestamptz <= NOW()
            """]
        ).count()
        # Check after affiliated_party filter
        snoozed_count = unassigned.filter(data__lead_stage='SNOOZED').count()
        expired_snoozed_count = unassigned.extra(
            where=["""
                data->>'lead_stage' = 'SNOOZED'
                AND data->>'next_call_at' IS NOT NULL
                AND data->>'next_call_at' != ''
                AND data->>'next_call_at' != 'null'
                AND (data->>'next_call_at')::timestamptz <= NOW()
            """]
        ).count()
        logger.info(
            "[GetNextLead] Snoozed leads: all_snoozed=%d, expired_before_filter=%d, expired_after_affiliated_party_filter=%d, now=%s",
            all_snoozed_count, expired_snoozed_before_filter, expired_snoozed_count, now_iso
        )
        
        candidate = self._order_by_score(unassigned, now_iso).first()

        # Step 5: Return first entry (or empty if none found)

        if not candidate:
            logger.info("[GetNextLead] No unassigned leads available after filtering and sorting by score")
            # --- Extra Diagnostics ---
            if unassigned_cnt > 0:
                logger.info("[GetNextLead] Unassigned leads exist but none passed the lead score ordering filter")
            return Response({}, status=status.HTTP_200_OK)

        # Lock and assign the lead
        with transaction.atomic():
            candidate_locked = Record.objects.select_for_update(skip_locked=True).filter(pk=candidate.pk).first()

            if not candidate_locked:
                logger.info("[GetNextLead] Lead was taken by another request")
                return Response({}, status=status.HTTP_200_OK)

            # Update the candidate's data
            data = candidate_locked.data.copy() if candidate_locked.data else {}
            data['assigned_to'] = user_identifier
            data['lead_stage'] = self.ASSIGNED_STATUS
            # Ensure call_attempts is always present for downstream logic/UI
            if 'call_attempts' not in data or data.get('call_attempts') in (None, '', 'null'):
                data['call_attempts'] = 0

            candidate_locked.data = data
            candidate_locked.updated_at = timezone.now()
            candidate_locked.save(update_fields=['data', 'updated_at'])

            # Log get_next_lead event for analytics
            try:
                EventLog.objects.create(
                    record=candidate_locked,
                    tenant=tenant,
                    event='lead.get_next_lead',
                    payload={
                        'user_id': str(user_uuid) if user_uuid else user_identifier,
                        'lead_id': candidate_locked.id,
                        'record_id': candidate_locked.id,
                    },
                    timestamp=timezone.now()
                )
                logger.debug(
                    "[GetNextLead] Logged get_next_lead event: record_id=%s user_id=%s",
                    candidate_locked.id,
                    str(user_uuid) if user_uuid else user_identifier
                )
            except Exception as e:
                logger.warning(
                    "[GetNextLead] Failed to log get_next_lead event: record_id=%s error=%s",
                    candidate_locked.id,
                    str(e)
                )
                # Don't fail the request if event logging fails

            logger.info(
                "[GetNextLead] Assigned new lead: record_id=%s user=%s",
                candidate_locked.id,
                user_identifier
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
            "[GetNextLead] Returning lead data: record_id=%s name=%s phone_no=%s source=%s last_active=%s",
            candidate_locked.id,
            flattened_response.get('name'),
            flattened_response.get('phone_no'),
            flattened_response.get('lead_source'),
            flattened_response.get('last_active_date_time')
        )

        return Response(flattened_response, status=status.HTTP_200_OK)


class GetMyCurrentLeadView(APIView):
    """
    Get the user's currently assigned lead from the database.
    Always queries the database directly (no cache) to ensure fresh data.
    Returns the same lead the user was working on, even after page refresh.
    
    GET /crm-records/leads/current/
    """
    permission_classes = [IsTenantAuthenticated]
    
    QUEUEABLE_STATUSES = ('in_queue', 'assigned', 'call_later', 'scheduled')
    
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
                            "lead_status": "assigned",
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
        
        # Always query database directly - get leads assigned to this user
        # Filter by queueable statuses (assigned, call_later, scheduled, in_queue)
        # Order by updated_at descending to get the most recently worked on lead
        current_lead = Record.objects.filter(
            tenant=tenant,
            entity_type='lead',
            data__assigned_to=user_identifier
        ).filter(
            Q(data__lead_stage__in=self.QUEUEABLE_STATUSES) | 
            Q(data__lead_stage__isnull=True)  # Include leads without explicit stage
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
    
    def post(self, request):
        """
        CREATE - Create a new lead record.
        
        Body:
        {
            "name": "Customer Name",
            "tenant_id": "optional-tenant-uuid",  # Optional: if provided, uses this tenant; otherwise uses default tenant
            "data": {
                "praja_id": "PRAJA123",  # Required: unique identifier for Praja system
                "phone_number": "+1234567890",
                "lead_score": 85,
                "lead_stage": "in_queue",
                "poster": "free"
            }
        }
        
        Note: praja_id in the data field is required for UPDATE and DELETE operations.
        Note: tenant_id is optional. If not provided, uses DEFAULT_TENANT_SLUG from settings.
        """
        tenant, error_response = self._get_tenant(request)
        if error_response:
            return error_response
        
        entity_type = self.get_entity_type(request)
        
        # Move name from root level to data if provided (root level takes precedence over data.name)
        # Also remove tenant_id from request_data since it's read-only in serializer and handled separately
        request_data = request.data.copy()
        request_data.pop('tenant_id', None)  # Remove tenant_id if present, handled separately
        if 'name' in request_data:
            # Ensure data is a dict
            if 'data' not in request_data:
                request_data['data'] = {}
            elif not isinstance(request_data['data'], dict):
                request_data['data'] = {}
            # Move name from root to data (overwrites if name already exists in data)
            request_data['data']['name'] = request_data.pop('name')

        # Normalize defaults for leads
        # call_attempts lives inside data JSON; ensure it's present for consistency.
        if entity_type == "lead":
            if 'data' not in request_data or not isinstance(request_data.get('data'), dict):
                request_data['data'] = {}
            if 'call_attempts' not in request_data['data'] or request_data['data'].get('call_attempts') in (None, '', 'null'):
                request_data['data']['call_attempts'] = 0
        
        # Check for duplicate praja_id before creating
        praja_id = None
        if 'data' in request_data and isinstance(request_data.get('data'), dict):
            praja_id = request_data['data'].get('praja_id')
        
        if praja_id:
            existing_record = Record.objects.filter(
                data__praja_id=praja_id,
                tenant=tenant,
                entity_type=entity_type
            ).first()
            
            if existing_record:
                logger.warning(
                    "[PrajaLeadsAPI] Duplicate praja_id blocked: praja_id=%s tenant=%s entity_type=%s existing_record_id=%s",
                    praja_id,
                    tenant.slug,
                    entity_type,
                    existing_record.id
                )
                return Response(
                    {
                        'error': f'{entity_type.capitalize()} with praja_id "{praja_id}" already exists',
                        'praja_id': praja_id,
                        'existing_record_id': existing_record.id
                    },
                    status=status.HTTP_409_CONFLICT
                )
        
        serializer = RecordSerializer(data=request_data)
        if serializer.is_valid():
            record = serializer.save(
                tenant=tenant,
                entity_type=entity_type
            )
            
            # Get name from data for logging
            record_name = (record.data or {}).get('name', '')
            
            logger.info(
                "[PrajaLeadsAPI] Created %s: id=%s tenant=%s name=%s",
                entity_type,
                record.id,
                tenant.slug,
                record_name
            )
            # Calculate and save lead score automatically
            try:
                from .scoring import calculate_and_update_lead_score
                score = calculate_and_update_lead_score(record, tenant_id=tenant.id, save=True)
                logger.info(
                    "[PrajaLeadsAPI] Created lead: id=%s tenant=%s name=%s score=%s",
                    record.id,
                    tenant.slug,
                    record_name,
                    score
                )
            except Exception as e:
                logger.error(f"[PrajaLeadsAPI] Error calculating lead score for lead {record.id}: {e}")
                # Don't fail the request if scoring fails, just log the error
                logger.info(
                    "[PrajaLeadsAPI] Created lead: id=%s tenant=%s name=%s (scoring failed)",
                    record.id,
                    tenant.slug,
                    record_name
                )
            
            # Refresh record from DB to get updated score
            record.refresh_from_db()
            
            return Response(
                RecordSerializer(record).data,
                status=status.HTTP_201_CREATED
            )
        
        return Response(
            serializer.errors,
            status=status.HTTP_400_BAD_REQUEST
        )
    
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
            "lead_stage": "assigned",  # Optional: update lead_stage
            "name": "Updated Name",  # Optional: update name
            "data": {  # Optional: update any fields in data JSON
                "lead_score": 95,
                "lead_stage": "assigned",
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
                entity_type='lead'
            )
        except Record.DoesNotExist:
            return Response(
                {'error': f'Lead with praja_id {praja_id} not found'},
                status=status.HTTP_404_NOT_FOUND
            )
        except Record.MultipleObjectsReturned:
            return Response(
                {'error': f'Multiple leads found with praja_id {praja_id}. Please ensure praja_id is unique.'},
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
            entity_type,
            record.id,
            praja_id,
            tenant.slug,
            list(request.data.keys())
        )
        # Recalculate and save lead score automatically (overwrites any manually set score)
        try:
            from .scoring import calculate_and_update_lead_score
            score = calculate_and_update_lead_score(record, tenant_id=tenant.id, save=True)
            logger.info(
                "[PrajaLeadsAPI] Updated lead: id=%s praja_id=%s tenant=%s score=%s fields=%s",
                record.id,
                praja_id,
                tenant.slug,
                score,
                list(request.data.keys())
            )
        except Exception as e:
            logger.error(f"[PrajaLeadsAPI] Error calculating lead score for updated lead {record.id}: {e}")
            # Don't fail the request if scoring fails, just log the error
            logger.info(
                "[PrajaLeadsAPI] Updated lead: id=%s praja_id=%s tenant=%s fields=%s (scoring failed)",
                record.id,
                praja_id,
                tenant.slug,
                list(request.data.keys())
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
            "lead_stage": "assigned",  # Optional: update lead_stage
            "name": "Updated Name",  # Optional: update name
            "data": {  # Optional: update any fields in data JSON
                "lead_score": 95,
                "lead_stage": "assigned",
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
            entity_type,
            record.id,
            praja_id,
            tenant.slug,
            list(request.data.keys())
        )
        # Recalculate and save lead score automatically (overwrites any manually set score)
        try:
            from .scoring import calculate_and_update_lead_score
            score = calculate_and_update_lead_score(record, tenant_id=tenant.id, save=True)
            logger.info(
                "[PrajaLeadsAPI] Updated lead: id=%s praja_id=%s tenant=%s score=%s fields=%s",
                record.id,
                praja_id,
                tenant.slug,
                score,
                list(request.data.keys())
            )
        except Exception as e:
            logger.error(f"[PrajaLeadsAPI] Error calculating lead score for updated lead {record.id}: {e}")
            # Don't fail the request if scoring fails, just log the error
            logger.info(
                "[PrajaLeadsAPI] Updated lead: id=%s praja_id=%s tenant=%s fields=%s (scoring failed)",
                record.id,
                praja_id,
                tenant.slug,
                list(request.data.keys())
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
    POST endpoint to save scoring rules and apply them to leads.
    
    POST /crm-records/leads/score/
    
    Saves the rules to EntityTypeSchema table and applies them to score all leads.
    
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
        Save scoring rules and queue background job to apply them to all leads.
        Saves/updates the rules in EntityTypeSchema table for the entity_type.
        Returns immediately with job ID - scoring happens in background.
        """
        from background_jobs.queue_service import get_queue_service
        from background_jobs.models import JobType
        
        serializer = LeadScoringRequestSerializer(data=request.data)
        
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        rules = serializer.validated_data['rules']
        entity_type = 'lead'  # Default entity type for lead scoring
        
        # Save/update rules in EntityTypeSchema table (replace previous rules)
        EntityTypeSchema.objects.update_or_create(
            tenant=request.tenant,
            entity_type=entity_type,
            defaults={
                'rules': rules
            }
        )
        
        logger.info(f"LeadScoringView: Saved {len(rules)} rules to EntityTypeSchema for entity_type '{entity_type}'")
        
        # Count total leads for the job
        total_leads = Record.objects.filter(
            tenant=request.tenant,
            entity_type='lead'
        ).count()
        
        # Enqueue background job using the queue service
        queue_service = get_queue_service()
        job = queue_service.enqueue_job(
            job_type=JobType.SCORE_LEADS,
            payload={
                'entity_type': entity_type,
                'batch_size': 100  # Process 100 leads per batch
            },
            priority=0,  # Normal priority
            tenant_id=str(request.tenant.id)
        )
        
        logger.info(
            f"LeadScoringView: Enqueued background job {job.id} for {total_leads} leads. "
            f"Job will be processed by background worker."
        )
        
        # Return immediately with job info
        return Response({
            'message': f'Rules saved. Background job created to score {total_leads} leads',
            'job_id': job.id,
            'status': job.status,
            'total_leads': total_leads,
            'progress': 0
        }, status=status.HTTP_202_ACCEPTED)
    
    def get(self, request):
        """
        Get status of lead scoring jobs.
        
        Query params:
        - job_id: Get specific job status (optional)
        """
        from background_jobs.models import BackgroundJob, JobType
        
        job_id = request.query_params.get('job_id')
        
        if job_id:
            try:
                job = BackgroundJob.objects.get(
                    id=job_id,
                    tenant_id=request.tenant.id,
                    job_type=JobType.SCORE_LEADS
                )
                
                # Extract progress from result
                result = job.result or {}
                progress = result.get('progress_percentage', 0)
                
                return Response({
                    'job_id': job.id,
                    'status': job.status,
                    'total_leads': result.get('total_leads', 0),
                    'processed_leads': result.get('processed_leads', 0),
                    'updated_leads': result.get('updated_leads', 0),
                    'total_score_added': result.get('total_score_added', 0.0),
                    'progress_percentage': progress,
                    'error_message': job.last_error,
                    'attempts': job.attempts,
                    'max_attempts': job.max_attempts,
                    'created_at': job.created_at.isoformat(),
                    'completed_at': job.completed_at.isoformat() if job.completed_at else None
                }, status=status.HTTP_200_OK)
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
            result = job.result or {}
            jobs_data.append({
                'job_id': job.id,
                'status': job.status,
                'total_leads': result.get('total_leads', 0),
                'processed_leads': result.get('processed_leads', 0),
                'updated_leads': result.get('updated_leads', 0),
                'progress_percentage': result.get('progress_percentage', 0),
                'created_at': job.created_at.isoformat(),
                'completed_at': job.completed_at.isoformat() if job.completed_at else None
            })
        
        return Response({
            'jobs': jobs_data,
            'count': len(jobs_data)
        }, status=status.HTTP_200_OK)


