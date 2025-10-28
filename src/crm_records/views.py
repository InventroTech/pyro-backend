from rest_framework import generics, status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.exceptions import ValidationError
from authz.permissions import IsTenantAuthenticated
from core.pagination import MetaPageNumberPagination
from django.utils import timezone
from django.db.models import Q, F
from django.db import transaction
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiExample, OpenApiParameter
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
        
        # Date range filtering on created_at (model field)
        created_at_gte = query_params.get('created_at__gte')
        if created_at_gte:
            queryset = queryset.filter(created_at__gte=created_at_gte)
        
        created_at_lte = query_params.get('created_at__lte')
        if created_at_lte:
            queryset = queryset.filter(created_at__lte=created_at_lte)
        
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
    def post(self, request):
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
            record = Record.objects.get(id=record_id)
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
    
    def _order_by_score(self, qs):
        """
        Order queryset by lead score (if exists in data), then creation date.
        Higher scores and older creation dates take priority.
        """
        # Order by score if it exists in the data field, then by creation date
        # Using PostgreSQL JSONB operators for ordering
        qs = qs.extra(
            select={
                'lead_score': "COALESCE((data->>'lead_score')::float, -1)",
            }
        ).order_by(
            F('lead_score').desc(nulls_last=True),
            'created_at',
            'id'
        )
        return qs
    
    @extend_schema(
        summary="Get next lead from queue",
        description="Atomically fetches and optionally assigns the next available lead from the queue for CRM records. "
                   "First checks for leads already assigned to the current user, then finds unassigned leads in queue. "
                   "Leads are ordered by score (if available) and creation date. "
                   "Use query parameter 'assign=false' to preview the next lead without assigning it.",
        parameters=[
            OpenApiParameter(
                name='assign',
                description='Whether to assign the lead to the current user (default: true). Set to false to preview without assigning.',
                required=False,
                type=OpenApiTypes.BOOL,
                location=OpenApiParameter.QUERY,
                default=True
            )
        ],
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
                                    "customer_full_name": "John Doe",
                                    "user_id": "USR123456",
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
        Get next lead from the queue and optionally assign it to the current user.
        
        Query Parameters:
            assign: bool - Whether to assign the lead (default: true). Set to false to preview without assigning.
        
        Logic:
        1. First check for leads already assigned to the user with queueable status
        2. If no such leads, atomically select and assign an unassigned lead
        3. Update the lead's data with assigned_to and status (unless assign=false)
        """
        # Check if we should assign the lead
        assign = request.query_params.get('assign', 'true').lower() not in ('false', '0', 'no', 'n')
        
        user = request.user
        tenant = request.tenant
        
        if not tenant:
            logger.warning("[GetNextLead] No tenant context available")
            return Response({}, status=status.HTTP_200_OK)
        
        # Get user identifier (supabase_uid or email)
        user_identifier = getattr(user, 'supabase_uid', None) or getattr(user, 'email', None)
        
        if not user_identifier:
            logger.warning("[GetNextLead] No user identifier available")
            return Response({}, status=status.HTTP_200_OK)
        
        # 1. First, check if there are any leads already assigned to this user with queueable status
        mine = Record.objects.filter(
            tenant=tenant,
            entity_type='lead',
            data__assigned_to=user_identifier,
            data__lead_stage__in=self.QUEUEABLE_STATUSES
        )
        
        mine_candidate = self._order_by_score(mine).first()
        
        if mine_candidate:
            logger.info(
                "[GetNextLead] Found existing lead for user: record_id=%s user=%s assign=%s",
                mine_candidate.id,
                user_identifier,
                assign
            )
            
            if assign:
                # Lock and assign
                with transaction.atomic():
                    locked = Record.objects.select_for_update(skip_locked=True).filter(pk=mine_candidate.pk)
                    
                    if tenant:
                        locked = locked.filter(tenant=tenant)
                    
                    locked_obj = locked.first()
                    
                    if not locked_obj:
                        logger.debug("[GetNextLead] Mine vanished/raced, falling through to unassigned fetch")
                    else:
                        # Update lead_stage if not already assigned
                        data = locked_obj.data.copy() if locked_obj.data else {}
                        if data.get('lead_stage') != self.ASSIGNED_STATUS:
                            data['lead_stage'] = self.ASSIGNED_STATUS
                            data['assigned_to'] = user_identifier
                            locked_obj.data = data
                            locked_obj.updated_at = timezone.now()
                            locked_obj.save(update_fields=['data', 'updated_at'])
                        
                        # Flatten response for frontend
                        lead_data = locked_obj.data or {}
                        serialized_data = RecordSerializer(locked_obj).data
                        flattened_response = {
                            "id": locked_obj.id,
                            "name": locked_obj.name or lead_data.get('customer_full_name') or '',
                            "phone_no": lead_data.get('phone_number', ''),
                            "user_id": lead_data.get('user_id'),
                            "lead_status": lead_data.get('lead_stage') or '',
                            "lead_score": lead_data.get('lead_score'),
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
                        
                        return Response(flattened_response, status=status.HTTP_200_OK)
            else:
                # Just return the existing lead without updating
                lead_data = mine_candidate.data or {}
                serialized_data = RecordSerializer(mine_candidate).data
                flattened_response = {
                    "id": mine_candidate.id,
                    "name": mine_candidate.name or lead_data.get('customer_full_name') or '',
                    "phone_no": lead_data.get('phone_number', ''),
                    "user_id": lead_data.get('user_id'),
                    "lead_status": lead_data.get('lead_stage') or '',
                    "lead_score": lead_data.get('lead_score'),
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
                
                return Response(flattened_response, status=status.HTTP_200_OK)
        
        # 2. Find unassigned queued lead
        # Note: For JSONB, when assigned_to is null in JSON, data->>'assigned_to' returns None
        from django.db.models import Q
        unassigned = Record.objects.filter(
            tenant=tenant,
            entity_type='lead'
        ).extra(
            where=["""
                (data->>'assigned_to' IS NULL OR 
                 data->>'assigned_to' = '' OR
                 data->>'assigned_to' = 'null' OR
                 data->>'assigned_to' = 'None')
            """]
        ).filter(
            Q(data__lead_stage__in=self.QUEUEABLE_STATUSES) | Q(data__lead_stage__isnull=True)
        )
        
        candidate = self._order_by_score(unassigned).first()
        
        if not candidate:
            logger.info("[GetNextLead] No unassigned leads available")
            return Response({}, status=status.HTTP_200_OK)
        
        if assign:
            # Lock and assign
            with transaction.atomic():
                candidate = Record.objects.select_for_update(skip_locked=True).filter(pk=candidate.pk).first()
                
                if not candidate:
                    logger.info("[GetNextLead] Lead was taken by another request")
                    return Response({}, status=status.HTTP_200_OK)
                
                # Update the candidate's data
                data = candidate.data.copy() if candidate.data else {}
                data['assigned_to'] = user_identifier
                data['lead_stage'] = self.ASSIGNED_STATUS
                
                candidate.data = data
                candidate.updated_at = timezone.now()
                candidate.save(update_fields=['data', 'updated_at'])
                
                logger.info(
                    "[GetNextLead] Assigned new lead: record_id=%s user=%s",
                    candidate.id,
                    user_identifier
                )
        else:
            logger.info(
                "[GetNextLead] Preview mode - found lead without assigning: record_id=%s",
                candidate.id
            )
        
        # Refresh from database to ensure we have latest data
        candidate.refresh_from_db()
        
        # Serialize and flatten for frontend compatibility
        serialized_data = RecordSerializer(candidate).data
        lead_data = candidate.data or {}
        
        # Flatten the response structure for easier frontend access
        # Map data fields to top-level for backward compatibility with defaults
        flattened_response = {
            "id": candidate.id,
            "name": candidate.name or lead_data.get('customer_full_name') or '',
            "phone_no": lead_data.get('phone_number', ''),
            "user_id": lead_data.get('user_id'),
            "lead_status": lead_data.get('lead_stage') or '',
            "lead_score": lead_data.get('lead_score'),
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
            # Include full nested structure for detailed data
            "data": lead_data,
            # Include full record for compatibility
            "record": serialized_data
        }
        
        logger.info(
            "[GetNextLead] Returning lead data: record_id=%s name=%s phone_no=%s source=%s last_active=%s",
            candidate.id,
            flattened_response.get('name'),
            flattened_response.get('phone_no'),
            flattened_response.get('lead_source'),
            flattened_response.get('last_active_date_time')
        )
        
        return Response(flattened_response, status=status.HTTP_200_OK)
