"""
Admin views for rule management and debugging.
Provides endpoints for tenant implementors to manage rules and view execution logs.
"""

from rest_framework import generics, status
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiResponse, OpenApiExample
from authz.permissions import IsTenantAuthenticated
from core.pagination import MetaPageNumberPagination
from .models import RuleSet, RuleExecutionLog
from .serializers import RuleSetSerializer, RuleExecutionLogSerializer
from .mixins import TenantScopedMixin


class RuleSetListCreateView(TenantScopedMixin, generics.ListCreateAPIView):
    """
    Admin endpoint for managing rule configurations.
    GET /rules/ - List all rules for tenant
    POST /rules/ - Create new rule
    """
    queryset = RuleSet.objects.all()
    serializer_class = RuleSetSerializer
    permission_classes = [IsTenantAuthenticated]
    pagination_class = MetaPageNumberPagination

    @extend_schema(
        summary="List all rules for tenant",
        description="Retrieves all rule configurations for the current tenant. "
                   "Useful for debugging and rule management.",
        responses={
            200: OpenApiResponse(
                description="List of rules",
                examples=[
                    OpenApiExample(
                        name="Rules List",
                        value={
                            "count": 2,
                            "next": None,
                            "previous": None,
                            "results": [
                                {
                                    "id": 1,
                                    "tenant_id": "123e4567-e89b-12d3-a456-426614174000",
                                    "event_name": "lead.win_clicked",
                                    "condition": {"==": [{"var": "record_data.status"}, "open"]},
                                    "actions": [{"action": "update_fields", "args": {"updates": {"status": "won"}}}],
                                    "enabled": True,
                                    "description": "Rule for lead win button",
                                    "created_at": "2025-01-01T00:00:00Z",
                                    "updated_at": "2025-01-01T00:00:00Z"
                                }
                            ]
                        }
                    )
                ]
            )
        },
        tags=["Admin - Rules"]
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    @extend_schema(
        summary="Create new rule",
        description="Creates a new rule configuration for the current tenant. "
                   "Rules define what actions to take when specific events occur.",
        request=RuleSetSerializer,
        responses={
            201: OpenApiResponse(
                description="Rule created successfully",
                examples=[
                    OpenApiExample(
                        name="Created Rule",
                        value={
                            "id": 1,
                            "tenant_id": "123e4567-e89b-12d3-a456-426614174000",
                            "event_name": "lead.win_clicked",
                            "condition": {"==": [{"var": "record_data.status"}, "open"]},
                            "actions": [{"action": "update_fields", "args": {"updates": {"status": "won"}}}],
                            "enabled": True,
                            "description": "Rule for lead win button",
                            "created_at": "2025-01-01T00:00:00Z",
                            "updated_at": "2025-01-01T00:00:00Z"
                        }
                    )
                ]
            ),
            400: OpenApiResponse(
                description="Bad request - invalid rule data",
                examples=[
                    OpenApiExample(
                        name="Validation Error",
                        value={"error": "Event name cannot be empty"}
                    )
                ]
            )
        },
        tags=["Admin - Rules"]
    )
    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)


class RuleExecutionLogListView(TenantScopedMixin, generics.ListAPIView):
    """
    Admin endpoint for viewing rule execution logs.
    GET /rule-logs/ - List rule execution history for debugging
    """
    queryset = RuleExecutionLog.objects.all()
    serializer_class = RuleExecutionLogSerializer
    permission_classes = [IsTenantAuthenticated]
    pagination_class = MetaPageNumberPagination

    @extend_schema(
        summary="List rule execution logs",
        description="Retrieves rule execution history for the current tenant. "
                   "Useful for debugging rule behavior and performance analysis.",
        parameters=[
            {
                'name': 'event_name',
                'in': 'query',
                'description': 'Filter by event name',
                'required': False,
                'schema': {'type': 'string'},
                'example': 'lead.win_clicked'
            },
            {
                'name': 'matched',
                'in': 'query',
                'description': 'Filter by match status (true/false)',
                'required': False,
                'schema': {'type': 'boolean'},
                'example': True
            },
            {
                'name': 'record_id',
                'in': 'query',
                'description': 'Filter by record ID',
                'required': False,
                'schema': {'type': 'integer'},
                'example': 123
            }
        ],
        responses={
            200: OpenApiResponse(
                description="List of rule execution logs",
                examples=[
                    OpenApiExample(
                        name="Execution Logs",
                        value={
                            "count": 5,
                            "next": None,
                            "previous": None,
                            "results": [
                                {
                                    "id": 1,
                                    "tenant_id": "123e4567-e89b-12d3-a456-426614174000",
                                    "record_id": 123,
                                    "rule_id": 1,
                                    "event_name": "lead.win_clicked",
                                    "matched": True,
                                    "actions": [{"action": "update_fields", "result": {"updated_fields": {"status": "won"}}}],
                                    "errors": [],
                                    "duration_ms": 15.5,
                                    "created_at": "2025-01-01T00:00:00Z"
                                }
                            ]
                        }
                    )
                ]
            )
        },
        tags=["Admin - Rule Logs"]
    )
    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        """
        Filter rule execution logs by tenant and optional query parameters.
        """
        queryset = super().get_queryset()
        
        # Filter by event name
        event_name = self.request.query_params.get('event_name')
        if event_name:
            queryset = queryset.filter(event_name=event_name)
        
        # Filter by match status
        matched = self.request.query_params.get('matched')
        if matched is not None:
            matched_bool = matched.lower() in ('true', '1', 'yes')
            queryset = queryset.filter(matched=matched_bool)
        
        # Filter by record ID
        record_id = self.request.query_params.get('record_id')
        if record_id:
            try:
                queryset = queryset.filter(record_id=int(record_id))
            except ValueError:
                pass  # Ignore invalid record_id
        
        return queryset.order_by('-created_at')
