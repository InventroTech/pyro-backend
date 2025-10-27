from rest_framework import generics, status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError, NotFound
from authz.permissions import IsTenantAuthenticated
from core.pagination import MetaPageNumberPagination
from django.utils import timezone
from django.db.models import Q
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiExample
from drf_spectacular.types import OpenApiTypes
import logging

logger = logging.getLogger(__name__)
from .models import Record, EventLog, RuleSet, RuleExecutionLog
from .serializers import RecordSerializer, EventLogSerializer, RuleSetSerializer, RuleExecutionLogSerializer
from .mixins import TenantScopedMixin
from .events import dispatch_event


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
        - Any other field: Will be searched in the data JSON field
        
        Examples:
        - ?entity_type=lead&resolution_status=WIP
        - ?entity_type=ticket&resolution_status=Scheduled
        - ?priority=high&status=active
        """
        queryset = super().get_queryset()
        
        # Get all query parameters
        query_params = self.request.query_params
        
        # Filter by entity_type (direct model field)
        entity_type = query_params.get('entity_type')
        if entity_type:
            queryset = queryset.filter(entity_type=entity_type)
        
        # Filter by name (direct model field)
        name = query_params.get('name')
        if name:
            queryset = queryset.filter(name__icontains=name)
        
        # Dynamic filtering on data JSON field
        # Get all query params except known model fields
        model_fields = {'entity_type', 'name', 'search', 'search_fields', 'page', 'page_size', 'ordering', 'created_at__gte', 'created_at__lte'}
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
                    if field in ['name', 'entity_type', 'created_at', 'updated_at']:
                        # Normal model fields
                        q_search |= Q(**{f"{field}__icontains": search_term})
                    else:
                        # JSONB fields in data column
                        q_search |= Q(**{f"data__{field}__icontains": search_term})
            else:
                # Fallback: search across all available fields
                # Search in normal model fields
                q_search |= Q(name__icontains=search_term)
                
                # Search in JSONB fields - we'll search in common fields and any existing data
                # Get all unique keys from existing data to search in
                # Dynamically collect all unique JSON fields from existing records for this queryset
                from itertools import chain
                all_data_keys = set(chain.from_iterable(
                    record.data.keys() for record in queryset if isinstance(record.data, dict)
                ))
                common_json_fields = list(all_data_keys)
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
        """
        # Get entity_type from query params or request data
        entity_type = self.request.query_params.get('entity_type')
        if not entity_type:
            entity_type = self.request.data.get('entity_type')
        
        if not entity_type:
            raise ValidationError({
                'entity_type': 'This field is required. Provide it in query params or request body.'
            })
        
        serializer.save(
            tenant=self.request.tenant,
            entity_type=entity_type
        )


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
                return self.get_queryset().get(id=record_id_int)
            except Record.DoesNotExist:
                raise NotFound('Record not found or access denied')

        # Fallback: use default URL kwarg (pk)
        return super().get_object()


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
        """Create record with tenant and the specific entity type."""
        serializer.save(
            tenant=self.request.tenant,
            entity_type=self.entity_type
        )


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

        # Validate payload is a dictionary
        if not isinstance(payload, dict):
            logger.error("[EventAPI] Invalid payload type for record_id=%s: type=%s", record_id, type(payload))
            return Response(
                {"error": "Payload must be a valid JSON object"}, 
                status=status.HTTP_400_BAD_REQUEST
            )

        # Create the event log entry
        try:
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
                   "Supports filtering by record ID and event name. Includes summary statistics.",
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
        
        # Order by most recent first
        return queryset.order_by('-timestamp')
