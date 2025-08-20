
from rest_framework.permissions import IsAuthenticated
from .models import Lead
from rest_framework.generics import ListAPIView, ListCreateAPIView, RetrieveUpdateAPIView
from .models import Lead
from .serializers import LeadSerializer, LeadCreateSerializer, LeadUpdateSerializer, LeadScoreUpdateSerializer
from uuid import UUID

from datetime import timedelta
from django.utils import timezone
from django.db.models import Avg, Count, F, ExpressionWrapper, DurationField, FloatField, Value
from django.db.models.functions import TruncDate
from rest_framework.views import APIView
from rest_framework.response import Response
from django.db import transaction
from rest_framework import status, serializers
from django.db.models.functions import Cast, Coalesce

from django.shortcuts import get_object_or_404

from .serializers import ALLOWED_STATUSES



CLOSED_STATUSES  = ("Resolved", "Won", "Lost", "Can't Resolve") 
ACTIVE_OWNED     = ("WIP")
QUEUEABLE        = ("Pending", "New")




def _tenant_scoped_qs(user):
    """
    Falls back to all leads if haven't enabled multi-tenancy enforcement.
    """
    qs = Lead.objects.all()
    tenant_id = getattr(user, "tenant_id", None)
    if tenant_id:
        try:
            qs = qs.filter(tenant_id=UUID(str(tenant_id)))
        except Exception:
            qs = Lead.objects.none()
    return qs


def _user_uuid(user):
    try:
        return UUID(str(user.supabase_uid))
    except Exception:
        return None

class WIPLeadsView(ListAPIView):
    serializer_class = LeadSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return (Lead.objects
                    .filter(assigned_to=user.supabase_uid, lead_status__in=ACTIVE_OWNED)
                    .order_by('-created_at'))

class AllLeadsView(ListAPIView):
    serializer_class = LeadSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return (Lead.objects.all().order_by('-created_at'))


class AllMyLeadsView(ListCreateAPIView):
    """
    GET  -> list my leads (assigned_to = me), newest first
    POST -> create a new lead 
    """
    permission_classes = [IsAuthenticated]

    def get_serializer_class(self):
        return LeadSerializer if self.request.method == "GET" else LeadCreateSerializer

    def get_queryset(self):
        u = self.request.user
        try:
            uid = UUID(str(u.supabase_uid))
        except (ValueError, TypeError):
            return Lead.objects.none()

        qs = Lead.objects.filter(assigned_to=uid).order_by("-created_at")
        if u.tenant_id:
            try:
                qs = qs.filter(tenant_id=UUID(str(u.tenant_id)))
            except (ValueError, TypeError):
                pass
        return qs

    def perform_create(self, serializer):
        user = self.request.user
        # Force tenant/assignee on the server (ignore any client-provided values)
        tenant_uuid = None
        if user.tenant_id:
            try:
                tenant_uuid = UUID(str(user.tenant_id))
            except (ValueError, TypeError):
                pass
        try:
            assignee_uuid = UUID(str(user.supabase_uid))
        except (ValueError, TypeError):
            assignee_uuid = None

        serializer.save(
            tenant_id=tenant_uuid,
            assigned_to=assignee_uuid if serializer.validated_data.get("lead_status") == "WIP" else None,
        )

class MyLeadDetailView(RetrieveUpdateAPIView):
    """
    GET    /leads/mine/<id>/    -> fetch one of *my* leads
    PUT    /leads/mine/<id>/    -> full update
    PATCH  /leads/mine/<id>/    -> partial update
    """
    permission_classes = [IsAuthenticated]
    serializer_class = LeadUpdateSerializer
    lookup_field = "pk"

    def get_queryset(self):
        # Only allow accessing your own leads, scoped by tenant if present
        u = self.request.user
        try:
            uid = UUID(str(u.supabase_uid))
        except (ValueError, TypeError):
            return Lead.objects.none()

        qs = Lead.objects.filter(assigned_to=uid)
        if u.tenant_id:
            try:
                qs = qs.filter(tenant_id=UUID(str(u.tenant_id)))
            except (ValueError, TypeError):
                pass
        return qs

    def perform_update(self, serializer):
        """
        Keep tenant/assignee server-controlled:
        - If status becomes WIP -> assign to caller.
        - If status is not WIP -> unassign.
        """
        user = self.request.user

        # Resolve tenant UUID (optional)
        tenant_uuid = None
        if user.tenant_id:
            try:
                tenant_uuid = UUID(str(user.tenant_id))
            except (ValueError, TypeError):
                pass

        # Caller’s UUID
        try:
            assignee_uuid = UUID(str(user.supabase_uid))
        except (ValueError, TypeError):
            assignee_uuid = None

        instance = serializer.instance
        new_status = serializer.validated_data.get("lead_status", instance.lead_status)

        if new_status == "WIP":
            serializer.save(tenant_id=tenant_uuid, assigned_to=assignee_uuid)
        else:
            serializer.save(tenant_id=tenant_uuid, assigned_to=None)



class LeadStatsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        
        try:
            days = int(request.query_params.get('days', 30))
            assert days > 0
        except Exception:
            return Response({'error': "'days' must be a positive integer."}, status=400)

        since = timezone.now() - timedelta(days=days)

        qs = Lead.objects.filter(created_at__gte=since)
        total = qs.count()
        wip = qs.filter(lead_status='WIP').count()
        resolved = qs.filter(lead_status__in=CLOSED_STATUSES).count()

        avg_close = qs.filter(
            lead_status__in=CLOSED_STATUSES, updated_at__isnull=False
        ).aggregate(
            avg=Avg(ExpressionWrapper(F('updated_at') - F('created_at'), output_field=DurationField()))
        )['avg']

        per_day = list(
            qs.annotate(d=TruncDate('created_at'))
              .values('d')
              .annotate(count=Count('id'))
              .order_by('d')
        )

        return Response({
            'period_in_days': days,
            'total_leads': total,
            'wip_count': wip,
            'closed_count': resolved,
            'avg_time_to_close_seconds': (avg_close.total_seconds() if avg_close else None),
            'new_leads_per_day': [{'date': r['d'], 'count': r['count']} for r in per_day],
        })




class GetNextLead(APIView):
    """
    Atomically fetch & assign the highest-scoring unassigned lead to the caller.
    """
    permission_classes = [IsAuthenticated]

    def _order_by_score(self, qs):
        # Cast to float (handles CharField/Decimal/Integer) and push NULLs to the end.
        return (
            qs.annotate(_score=Coalesce(Cast("lead_score", FloatField()), Value(float("-inf"))))
              .order_by(F("_score").desc(nulls_last=True), "created_at", "id")
        )
    def get(self, request):
        user = request.user

        try:
            assignee_uuid = UUID(str(user.supabase_uid))
        except Exception:
            return Response(
                {"error": "Authenticated user does not have a valid supabase_uid UUID."},
                status=400,
            )

        tenant_uuid = None
        if getattr(user, "tenant_id", None):
            try:
                tenant_uuid = UUID(str(user.tenant_id))
            except Exception:
                pass

        with transaction.atomic():
            mine = (
                Lead.objects.select_for_update(skip_locked=True)
                .filter(assigned_to=assignee_uuid, lead_status__in=QUEUEABLE)
            )
            if tenant_uuid:
                mine = mine.filter(tenant_id=tenant_uuid)

            mine_candidate = self._order_by_score(mine).first()
            if mine_candidate:
                return Response({"lead": LeadSerializer(mine_candidate).data}, status=200)

            unassigned = (
                Lead.objects.select_for_update(skip_locked=True)
                .filter(assigned_to__isnull=True, lead_status__in=QUEUEABLE)
            )
            if tenant_uuid:
                unassigned = unassigned.filter(tenant_id=tenant_uuid)

            candidate = self._order_by_score(unassigned).first()
            if not candidate:
                return Response({}, status=200)

            candidate.assigned_to = assignee_uuid
            candidate.updated_at = timezone.now()
            candidate.save(update_fields=["assigned_to", "updated_at"])

            return Response({"lead": LeadSerializer(candidate).data}, status=200)

class SaveAndContinueLeadView(APIView):
    """
    Update a lead (status, assignment, fields)
    - If lead_status == 'WIP' => assign to current user.
    - Else => unassign (assigned_to = NULL).
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

        # Map Django user -> UUID for assigned_to
        try:
            assignee_uuid = UUID(str(request.user.supabase_uid))
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
            # Lock the row to avoid concurrent updates
            qs = Lead.objects.select_for_update().filter(id=lead_id)
            if tenant_uuid:
                qs = qs.filter(tenant_id=tenant_uuid)

            lead = qs.first()
            if not lead:
                return Response({"error": "Lead not found."}, status=status.HTTP_404_NOT_FOUND)

            updates = {}

            # Assignment rule 
            if lead_status is not None:
                updates["lead_status"] = lead_status
                if lead_status == "WIP":
                    updates["assigned_to"] = assignee_uuid
                else:
                    updates["assigned_to"] = None

            # Optional field updates
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

            # Apply updates
            Lead.objects.filter(id=lead.id).update(**updates)
            lead.refresh_from_db()

            return Response(
                {
                    "success": True,
                    "message": "Lead updated successfully",
                    "lead": LeadSerializer(lead).data,
                    "userId": str(assignee_uuid),
                    "userEmail": getattr(request.user, "email", None),
                },
                status=status.HTTP_200_OK,
            )
        
class TakeBreakLeadView(APIView):
    """
    Behavior:
    - If target lead is WIP (either provided or current), DO NOT unassign.
    - Otherwise, set assigned_to = NULL.
    - Enforces tenant scoping (if request.user.tenant_id is present).
    - Only allows unassigning leads currently assigned to the caller.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        data = request.data or {}
        lead_id = data.get("leadId") or data.get("lead_id")
        requested_status = data.get("lead_status") or data.get("resolution_status")

        # Resolve the caller's UUID (stored in Lead.assigned_to)
        try:
            assignee_uuid = UUID(str(request.user.supabase_uid))
        except Exception:
            return Response(
                {"error": "Authenticated user does not have a valid supabase_uid UUID."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Optional tenant scope
        tenant_uuid = None
        if getattr(request.user, "tenant_id", None):
            try:
                tenant_uuid = UUID(str(request.user.tenant_id))
            except Exception:
                # Ignore malformed tenant id; treat as no tenant filter
                pass

        with transaction.atomic():
            qs = Lead.objects.select_for_update()

            # Target selection: explicit ID or fallback to "most recent lead assigned to me"
            if lead_id is not None:
                qs = qs.filter(id=lead_id)
            else:
                qs = qs.filter(assigned_to=assignee_uuid).order_by("-updated_at", "-created_at")

            if tenant_uuid:
                qs = qs.filter(tenant_id=tenant_uuid)

            lead = qs.first()
            if not lead:
                return Response({"error": "Lead not found."}, status=status.HTTP_404_NOT_FOUND)

            # Only the current assignee can take a break on this lead
            if lead.assigned_to != assignee_uuid:
                return Response(
                    {"error": "You are not the assignee of this lead."},
                    status=status.HTTP_403_FORBIDDEN,
                )

            # Decide whether to unassign 
            should_unassign = True
            message = "Lead unassigned. Taking a break."
            current_status = (lead.lead_status or "").strip()
            requested_status = (requested_status or "").strip()

            if requested_status == "WIP" or current_status == "WIP":
                should_unassign = False
                message = "Lead is in progress. Taking a break without unassigning."

            if should_unassign:
                Lead.objects.filter(id=lead.id).update(
                    assigned_to=None,
                    updated_at=timezone.now(),
                )
                # Refresh to reflect unassignment
                lead.refresh_from_db()

            return Response(
                {
                    "success": True,
                    "message": message,
                    "leadUnassigned": should_unassign,
                    "lead": LeadSerializer(lead).data,
                    "userId": str(assignee_uuid),
                    "userEmail": getattr(request.user, "email", None),
                },
                status=status.HTTP_200_OK,
            )
        


class LeadDetailUpdateView(RetrieveUpdateAPIView):
    """
    GET   /leads/<id>/      -> read one lead
    PUT   /leads/<id>/      -> full update (only allowed fields in LeadUpdateSerializer)
    PATCH /leads/<id>/      -> partial update
    """
    permission_classes = [IsAuthenticated]
    lookup_field = "pk"

    def get_queryset(self):
        return _tenant_scoped_qs(self.request.user)

    def get_serializer_class(self):
        # Read returns full payload; write uses safe, updatable fields
        return LeadSerializer if self.request.method == "GET" else LeadUpdateSerializer



class LeadScoreUpdateView(APIView):
    """
    PATCH /leads/<id>/score/  -> update lead_score only
    PUT   /leads/<id>/score/  -> same as PATCH
    """
    permission_classes = [IsAuthenticated]

    def patch(self, request, pk):
        return self._update_score(request, pk)

    def put(self, request, pk):
        return self._update_score(request, pk)

    def _update_score(self, request, pk):
        lead = get_object_or_404(_tenant_scoped_qs(request.user), pk=pk)
        ser = LeadScoreUpdateSerializer(lead, data=request.data, partial=True)
        ser.is_valid(raise_exception=True)

        ser.save()

        return Response(LeadSerializer(lead).data, status=status.HTTP_200_OK)