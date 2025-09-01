from uuid import UUID

import os
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.generics import ListAPIView, ListCreateAPIView, RetrieveUpdateAPIView
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, serializers
from django.db import transaction
from django.db.models import Avg, Count, F, ExpressionWrapper, DurationField, FloatField, Value, Q
from django.db.models.functions import TruncDate, Cast, Coalesce
from django.shortcuts import get_object_or_404
from django.utils import timezone
from datetime import datetime, time, timedelta
from django.conf import settings

from .models import Lead
from .serializers import (
    LeadSerializer, LeadCreateSerializer, LeadUpdateSerializer,
     ALLOWED_STATUSES
)
from drf_spectacular.utils import (
    extend_schema,
    OpenApiParameter,
    OpenApiExample,
    OpenApiResponse,
    OpenApiTypes,
    extend_schema_view
)
from drf_spectacular.types import OpenApiTypes
from authz.permissions import IsTenantAuthenticated, HasTenantRole







# ALL STATUSES = IN_QUEUE, ASSIGNED, WON, LOST, CALL_LATER, SCHEDULED, CLOSED
CLOSED_STATUSES = ("won", "lost", "closed")
UPDATABLE_STATUSES = ("call_later", "scheduled")        
QUEUEABLE = ("in_queue",) 
ASSIGNED = "assigned"
VALID_STATUSES = {
    "won", "lost", "closed", "call_later",
    "scheduled", "in_queue",
    "assigned"
    }

WRITABLE_FIELDS = {
    "reason",
    "badge",
    "lead_description",
    "other_description",
    "lead_score",
    "praja_dashboard_user_link",
}



# @extend_schema_view(
#         get=extend_schema(
#             summary="List all leads",
#             description="Returns a paginated list of leads for the current tenant.",
#             tags=["Leads"],
#             parameters=[
#                 OpenApiParameter(name="search", description="Search by name/phone/email", required=False, type=OpenApiTypes.STR),
#                 OpenApiParameter(name="lead_status", description="Filter by status", required=False, type=OpenApiTypes.STR),
#                 OpenApiParameter(name="ordering", description="Order by field (e.g. -created_at, lead_score)", required=False, type=OpenApiTypes.STR),
#             ],
#             responses={
#                 200: LeadSerializer(many=True),   # drf-spectacular will wrap in your pagination schema automatically
#                 401: OpenApiResponse(description="Unauthorized"),
#                 403: OpenApiResponse(description="Forbidden"),
#             },
#             operation_id="leads_list",  # stable, nice for clients
#         )
#         )

def _tenant_scoped_qs(request):
    if not getattr(request, "tenant", None):
        return Lead.objects.none()
    return (
        Lead.objects
        .filter(tenant_id=request.tenant.id) 
        .select_related("assigned_to")
    )


class CallLaterLeadsView(ListAPIView):
    serializer_class = LeadSerializer
    permission_classes = [IsTenantAuthenticated, HasTenantRole("GM")]

    def get_queryset(self):
        print(self.request)
        user = self.request.user
        return (
            _tenant_scoped_qs(self.request)
            .filter(assigned_to=user, 
                    lead_status="call_later")
            .order_by('-created_at')
        )

class AllLeadsView(ListAPIView):
    serializer_class = LeadSerializer
    permission_classes = [IsTenantAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return _tenant_scoped_qs(self.request).order_by('-created_at')


class AllMyLeadsView(ListCreateAPIView):
    """
    GET  -> list my leads (assigned_to = me), newest first
    POST -> create a new lead (server controls tenant/assignee)
    """
    permission_classes = [IsTenantAuthenticated]

    def get_serializer_class(self):
        return LeadSerializer if self.request.method == "GET" else LeadCreateSerializer

    def get_queryset(self):
        user = self.request.user
        
        qs = (
            _tenant_scoped_qs(self.request)
            .filter(assigned_to=user)
            .order_by("-created_at")
        )
        return qs

    def perform_create(self, serializer):
        user = self.request.user
        tenant_uuid = None
        if user.tenant_id:
            try:
                tenant_uuid = UUID(str(user.tenant_id))
            except (ValueError, TypeError):
                pass
        assigned_to = user
        serializer.save(
            tenant_id=tenant_uuid,
            assigned_to=user
        )

class MyLeadDetailView(RetrieveUpdateAPIView):
    """
    GET    /leads/mine/<id>/
    PUT    /leads/mine/<id>/
    PATCH  /leads/mine/<id>/
    """
    permission_classes = [IsTenantAuthenticated]
    serializer_class = LeadUpdateSerializer
    lookup_field = "pk"

    def get_queryset(self):
        u = self.request.user
        try:
            uid = str(UUID(str(u.supabase_uid)))
        except (ValueError, TypeError):
            return Lead.objects.none()

        qs = _tenant_scoped_qs(self.request).filter(assigned_to_id=uid)
        return qs

    def perform_update(self, serializer):
        user = self.request.user

        tenant_uuid = None
        if user.tenant_id:
            try:
                tenant_uuid = UUID(str(user.tenant_id))
            except (ValueError, TypeError):
                pass

        try:
            assignee_uuid = str(UUID(str(user.supabase_uid)))
        except (ValueError, TypeError):
            assignee_uuid = None

        instance = serializer.instance
        new_status = serializer.validated_data.get("lead_status", instance.lead_status)

        if new_status == "WIP":
            serializer.save(tenant_id=tenant_uuid, assigned_to_id=assignee_uuid)
        else:
            serializer.save(tenant_id=tenant_uuid, assigned_to_id=None)



class LeadStatsView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        # 1) Parse days
        try:
            days = int(request.query_params.get('days', 1))
            assert days > 0
        except Exception:
            return Response({'error': "'days' must be a positive integer."}, status=400)

        DAYS_HARD_CAP = 365
        days = min(days, DAYS_HARD_CAP)

        now = timezone.now()
        created_since = now - timedelta(days=days)
        print(created_since)

        # 2) Tenant-scoped QSes
        base_qs = _tenant_scoped_qs(request)
        created_qs = base_qs.filter(created_at__gte=created_since)

        # 3) Build start/end of today safely
        if settings.USE_TZ:
            tz = timezone.get_current_timezone()
            today = timezone.localdate()
            start_of_today = timezone.make_aware(datetime.combine(today, time.min), tz)
            start_of_tomorrow = start_of_today + timedelta(days=1)
        else:
            today = datetime.today().date()
            start_of_today = datetime.combine(today, time.min)
            start_of_tomorrow = start_of_today + timedelta(days=1)
            
        agg_created = created_qs.aggregate(
            fresh_leads=Count("id"),
            call_later=Count("id", filter=Q(lead_status__iexact="call_later"))
        )

        agg_resolved = base_qs.aggregate(
            won_today=Count("id", filter=Q(
                lead_status__iexact="won",
                resolved_at__gte=start_of_today,
                resolved_at__lt=start_of_tomorrow,
            )),
            lost_today=Count("id", filter=Q(
                lead_status__iexact="lost",
                resolved_at__gte=start_of_today,
                resolved_at__lt=start_of_tomorrow,
            ))
        )

        return Response({
            "fresh_leads": agg_created.get("fresh_leads") or 0,
            "leads_won":  agg_resolved.get("won_today") or 0,
            "call_later": agg_created.get("call_later") or 0,
            "leads_lost": agg_resolved.get("lost_today") or 0,
        })
    

class GetNextLead(APIView):
    """
    Atomically fetch & assign the highest-scoring unassigned lead to the caller.
    """
  
    permission_classes = [IsTenantAuthenticated]
    def _order_by_score(self, qs):
        qs = qs.order_by(
            F("lead_score").desc(nulls_last=True),
            F("lead_creation_date").asc(nulls_last=True),
            "id",
        )
        return qs

    def get(self, request):
        user = request.user
        tenant_uuid = None
        if getattr(user, "tenant_id", None):
            try:
                from uuid import UUID
                tenant_uuid = UUID(str(user.tenant_id))
            except Exception as e:
                print("[GetNextLead] invalid tenant_id:", e)

        mine = Lead.objects.filter(assigned_to=user, lead_status__in=QUEUEABLE)
        if tenant_uuid:
            mine = mine.filter(tenant_id=tenant_uuid)

        mine_candidate = self._order_by_score(mine).first()
        
        if mine_candidate:
            
            with transaction.atomic():
                locked = (Lead.objects
                          .select_for_update(skip_locked=True, of=("self",))
                          .filter(pk=mine_candidate.pk, assigned_to=user))
                if tenant_uuid:
                    locked = locked.filter(tenant_id=tenant_uuid)

                locked_obj = locked.first()

                if not locked_obj:
                    print("[GetNextLead] mine vanished/raced, falling through to unassigned fetch")
                else:
                    if locked_obj.lead_status != ASSIGNED:
                        prev = locked_obj.lead_status
                        locked_obj.lead_status = ASSIGNED
                        locked_obj.updated_at = timezone.now()
                        locked_obj.save(update_fields=["lead_status", "updated_at"])

                    lead = Lead.objects.select_related("assigned_to").get(pk=locked_obj.pk)
                    return Response({"lead": LeadSerializer(lead).data}, status=200)

        # 2) Atomically pick & assign an unassigned queued lead
        with transaction.atomic():
            unassigned = Lead.objects.filter(assigned_to__isnull=True, lead_status__in=QUEUEABLE)
            if tenant_uuid:
                unassigned = unassigned.filter(tenant_id=tenant_uuid)
            unassigned = unassigned.select_for_update(skip_locked=True, of=("self",))
            candidate = self._order_by_score(unassigned).first()
            if not candidate:
                return Response({}, status=200)
            candidate.assigned_to = user
            candidate.updated_at = timezone.now()
            candidate.lead_status = ASSIGNED
            candidate.save(update_fields=["assigned_to", "updated_at", "lead_status"])

        lead = Lead.objects.select_related("assigned_to").get(pk=candidate.pk)
        return Response({"lead": LeadSerializer(lead).data}, status=200)



class SaveAndContinueLeadView(APIView):
    """
    Update a lead (status, fields)
    - If lead_status == 'assigned' => assign to current user.
    - Else => unassign.
    """
    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        user = request.user
        data = request.data or {}
        lead_id = data.get("leadId")
        lead_status = data.get("leadStatus")
        

        if not lead_id:
            return Response({"error": "leadId is required."}, status=status.HTTP_400_BAD_REQUEST)
        
        normalized_status = None
        if lead_status is not None:
            normalized_status = str(lead_status).strip().lower()
            if normalized_status not in VALID_STATUSES:
                return Response(
                    {
                        "error":"Invalid leadStatus",
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )


        tenant_uuid = None
        if getattr(request.user, "tenant_id", None):
            try:
                tenant_uuid = UUID(str(request.user.tenant_id))
            except Exception:
                tenant_uuid = None 

        with transaction.atomic():
            qs = Lead.objects.select_for_update(skip_locked=True, of=("self",)).filter(id=lead_id)
            if tenant_uuid:
                qs = qs.filter(tenant_id=tenant_uuid)

            lead = qs.first()
            if not lead:
                return Response({"error": "Lead not found."}, status=status.HTTP_404_NOT_FOUND)
            
            fields_to_update = []

            if normalized_status is not None:
                lead.lead_status = normalized_status
                fields_to_update.append("lead_status")

                # assignment rule
                if normalized_status=="assigned":
                    if lead.assigned_to != user:
                        lead.assigned_to = user
                        fields_to_update.append("assigned_to")
                else:
                    if lead.assigned_to is not None:
                        lead.assigned_to = None
                        fields_to_update.append("assigned_to")


            for field in WRITABLE_FIELDS:
                if field in data:
                    setattr(lead, field, data[field])
                    fields_to_update.append(field)

            lead.updated_at = timezone.now()
            fields_to_update.append("updated_at")
            fields_to_update = list(dict.fromkeys(fields_to_update))

            if fields_to_update:
                lead.save(update_fields=fields_to_update)

        lead = Lead.objects.select_related("assigned_to").get(pk=lead.pk)
        return Response(
            {
                "success": True,
                "message": "Lead updated successfully",
                "lead": LeadSerializer(lead).data,
                "user": getattr(user, "email", None)
            },
            status=status.HTTP_200_OK,
        )


class TakeBreakLeadView(APIView):
    """
    - If target lead is WIP (either provided or current), DO NOT unassign.
    - Otherwise, set assigned_to = NULL.
    - Only the current assignee can take a break on this lead.
    """
    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        data = request.data or {}
        lead_id = data.get("leadId")

        # Optional tenant scope
        tenant_uuid = None
        if getattr(request.user, "tenant_id", None):
            try:
                tenant_uuid = UUID(str(request.user.tenant_id))
            except Exception:
                tenant_uuid = None 

        with transaction.atomic():

            qs = Lead.objects.select_for_update(of=("self",))
            if lead_id is not None:
                qs = qs.filter(id=lead_id)
            else:
                qs = qs.filter(assigned_to=request.user).order_by("-updated_at", "-created_at")

            if tenant_uuid:
                qs = qs.filter(tenant_id=tenant_uuid)

            lead = qs.first()
            if not lead:
                return Response({"error": "Lead not found."}, status=status.HTTP_404_NOT_FOUND)

            if lead.assigned_to is None or lead.assigned_to != request.user:
                    return Response(
                        {"error": "You are not the assignee of this lead."},
                        status=status.HTTP_403_FORBIDDEN,
                    )

            current_status = (lead.lead_status or "").strip()
            # if current_status == "WIP":
            #     return Response(
            #         {
            #             "success": True,
            #             "message": "Lead is in progress. Taking a break without unassigning.",
            #             "leadUnassigned": False,
            #             "lead": LeadSerializer(lead).data,
            #             "userId": getattr(request.user, "pk", None),
            #             "userEmail": getattr(request.user, "email", None),
            #         },
            #         status=status.HTTP_200_OK,
            #     )
            # Unassign
            updated = Lead.objects.filter(id=lead.id).update(
                assigned_to=None,
                lead_status = "in_queue",
                updated_at=timezone.now()
            )
            lead.refresh_from_db()
            return Response(
                {
                    "success": True,
                    "message": "Lead unassigned. Taking a break.",
                    "leadUnassigned": True,
                    "lead": LeadSerializer(lead).data,
                    "userId": getattr(request.user, "pk", None),
                    "userEmail": getattr(request.user, "email", None),
                },
                status=status.HTTP_200_OK,
            )
        
class LeadDetailUpdateView(RetrieveUpdateAPIView):
    """
    GET   /leads/<id>/
    PUT   /leads/<id>/
    PATCH /leads/<id>/
    """
    permission_classes = [IsTenantAuthenticated]
    lookup_field = "pk"

    def get_queryset(self):
        # Tenant scope + join user
        return _tenant_scoped_qs(self.request)

    def get_serializer_class(self):
        return LeadSerializer if self.request.method == "GET" else LeadUpdateSerializer


class LeadScoreUpdateView(APIView):
    """
    PATCH/PUT /leads/<id>/score/ -> update lead_score only
    """
    permission_classes = [AllowAny]

    def post(self, request, pk):
        # The api will contain user_id and the corresponding lead_score field. update the lead_score field for the user_id
        # Check the x-webhook-secret in the request headers
        webhook_secret = request.headers.get("x-webhook-secret")
        if webhook_secret != os.environ.get('WEBHOOK_SECRET'):
            return Response({"error": "Invalid webhook secret"}, status=status.HTTP_401_UNAUTHORIZED)
        user_id = request.data.get("user_id")
        lead_score = request.data.get("lead_score")
        lead = Lead.objects.get(user_id=user_id)
        lead.lead_score = lead_score
        lead.save()
        return Response({"success": True}, status=status.HTTP_200_OK)

class LeadPushWebhookView(APIView):
    permission_classes = [AllowAny]

    serializer_class = LeadCreateSerializer
    def post(self, request):
        webhook_secret = request.headers.get("x-webhook-secret")
        if webhook_secret != os.environ.get('WEBHOOK_SECRET'):
            return Response({"error": "Invalid webhook secret"}, status=status.HTTP_401_UNAUTHORIZED)
        serializer = self.serializer_class(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response({"success": True}, status=status.HTTP_200_OK)
        else:
            return Response({"error": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
