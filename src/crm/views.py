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

from .models import Lead
from .serializers import (
    LeadSerializer, LeadCreateSerializer, LeadUpdateSerializer,
    LeadScoreUpdateSerializer, ALLOWED_STATUSES
)
# ALL STATUSES = IN_QUEUE, ASSIGNED, WON, LOST, CALL_LATER, SCHEDULED, CLOSED
CLOSED_STATUSES = ("won", "lost", "closed")
UPDATABLE_STATUSES = ("call_later", "scheduled")        
QUEUEABLE = ("in_queue",) 
ASSIGNED = "assigned"


def _tenant_scoped_qs(user):
    qs = Lead.objects.all()
    tenant_id = getattr(user, "tenant_id", None)
    if tenant_id:
        try:
            qs = qs.filter(tenant_id=UUID(str(tenant_id)))
        except Exception:
            return Lead.objects.none()
    return qs.select_related("assigned_to")


class WIPLeadsView(ListAPIView):
    serializer_class = LeadSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return (
            _tenant_scoped_qs(user)
            .filter(assigned_to_id=user.supabase_uid, lead_status__in=UPDATABLE_STATUSES)
            .order_by('-created_at')
        )


class AllLeadsView(ListAPIView):
    serializer_class = LeadSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return _tenant_scoped_qs(user).order_by('-created_at')


class AllMyLeadsView(ListCreateAPIView):
    """
    GET  -> list my leads (assigned_to = me), newest first
    POST -> create a new lead (server controls tenant/assignee)
    """
    permission_classes = [IsAuthenticated]

    def get_serializer_class(self):
        return LeadSerializer if self.request.method == "GET" else LeadCreateSerializer

    def get_queryset(self):
        u = self.request.user
        try:
            uid = str(UUID(str(u.supabase_uid)))
        except (ValueError, TypeError):
            return Lead.objects.none()

        qs = (
            _tenant_scoped_qs(u)
            .filter(assigned_to_id=uid)
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

        try:
            assignee_uuid = str(UUID(str(user.supabase_uid)))
        except (ValueError, TypeError):
            assignee_uuid = None

        # Assign only if created as WIP; else leave unassigned
        assigned_to_id = assignee_uuid if serializer.validated_data.get("lead_status") == "WIP" else None

        serializer.save(
            tenant_id=tenant_uuid,
            assigned_to_id=assigned_to_id,
        )


class MyLeadDetailView(RetrieveUpdateAPIView):
    """
    GET    /leads/mine/<id>/
    PUT    /leads/mine/<id>/
    PATCH  /leads/mine/<id>/
    """
    permission_classes = [IsAuthenticated]
    serializer_class = LeadUpdateSerializer
    lookup_field = "pk"

    def get_queryset(self):
        u = self.request.user
        try:
            uid = str(UUID(str(u.supabase_uid)))
        except (ValueError, TypeError):
            return Lead.objects.none()

        qs = _tenant_scoped_qs(u).filter(assigned_to_id=uid)
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
    permission_classes = [IsAuthenticated]
    def get(self, request):
        try:
            days = int(request.query_params.get('days', 1))
            assert days > 0
        except Exception:
            return Response({'error': "'days' must be a positive integer."}, status=400)
        DAYS_HARD_CAP = 365
        if days > DAYS_HARD_CAP:
            days = DAYS_HARD_CAP
        since = timezone.now() - timezone.timedelta(days=days)
        qs = _tenant_scoped_qs(request.user).filter(lead_creation_date__gte=since)

        agg = qs.aggregate(
            fresh_leads=Count("id"),
            leads_won=Count("id", filter=Q(lead_status="won")),
            call_later=Count("id", filter=Q(lead_status="call_later")),
            leads_lost=Count("id", filter=Q(lead_status="lost")),
        )

        return Response({
            "fresh_leads": agg.get("fresh_leads", 0) or 0,
            "leads_won": agg.get("leads_won", 0) or 0,
            "call_later": agg.get("call_later", 0) or 0,
            "leads_lost": agg.get("leads_lost", 0) or 0,
        })



class GetNextLead(APIView):
    """
    Atomically fetch & assign the highest-scoring unassigned lead to the caller.
    """
  
    permission_classes = [IsAuthenticated]
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
    - If lead_status == 'WIP' => assign to current user.
    - Else => unassign.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        data = request.data or {}
        lead_id = data.get("leadId") or data.get("lead_id")
        lead_status = data.get("lead_status")
        is_read_only = bool(data.get("isReadOnly", False))

        if not lead_id:
            return Response({"error": "leadId is required."}, status=status.HTTP_400_BAD_REQUEST)
        if is_read_only:
            return Response({"error": "This lead is read-only."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            assignee_uuid = str(UUID(str(request.user.supabase_uid)))
        except Exception:
            return Response(
                {"error": "Authenticated user does not have a valid supabase_uid UUID."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        tenant_uuid = None
        if getattr(request.user, "tenant_id", None):
            try:
                tenant_uuid = UUID(str(request.user.tenant_id))
            except Exception:
                pass

        with transaction.atomic():
            qs = Lead.objects.select_for_update().filter(id=lead_id)
            if tenant_uuid:
                qs = qs.filter(tenant_id=tenant_uuid)

            lead = qs.select_related("assigned_to").first()
            if not lead:
                return Response({"error": "Lead not found."}, status=status.HTTP_404_NOT_FOUND)

            updates = {}

            if lead_status is not None:
                updates["lead_status"] = lead_status
                updates["assigned_to_id"] = assignee_uuid if lead_status == "WIP" else None

            for field in (
                "reason",
                "badge",
                "lead_description",
                "other_description",
                "lead_score",
                "praja_dashboard_user_link",
                "display_pic_url",
                "lead_creation_date",
            ):
                if field in data:
                    updates[field] = data[field]

            updates["updated_at"] = timezone.now()

            Lead.objects.filter(id=lead.id).update(**updates)
            lead.refresh_from_db()  # picks up FK

            return Response(
                {
                    "success": True,
                    "message": "Lead updated successfully",
                    "lead": LeadSerializer(lead).data,
                    "userId": assignee_uuid,
                    "userEmail": getattr(request.user, "email", None),
                },
                status=status.HTTP_200_OK,
            )

class TakeBreakLeadView(APIView):
    """
    - If target lead is WIP (either provided or current), DO NOT unassign.
    - Otherwise, set assigned_to = NULL.
    - Only the current assignee can take a break on this lead.
    """
    permission_classes = [IsAuthenticated]

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
            if current_status == "WIP":
                return Response(
                    {
                        "success": True,
                        "message": "Lead is in progress. Taking a break without unassigning.",
                        "leadUnassigned": False,
                        "lead": LeadSerializer(lead).data,
                        "userId": getattr(request.user, "pk", None),
                        "userEmail": getattr(request.user, "email", None),
                    },
                    status=status.HTTP_200_OK,
                )
            # Unassign
            updated = Lead.objects.filter(id=lead.id).update(
                assigned_to=None,
                updated_at=timezone.now(),
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
    permission_classes = [IsAuthenticated]
    lookup_field = "pk"

    def get_queryset(self):
        # Tenant scope + join user
        return _tenant_scoped_qs(self.request.user)

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
