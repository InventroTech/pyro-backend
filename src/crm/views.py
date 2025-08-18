
from rest_framework.permissions import IsAuthenticated
from .models import Lead
from rest_framework.generics import ListAPIView, ListCreateAPIView
from .models import Lead
from .serializers import LeadSerializer, LeadCreateSerializer
from uuid import UUID

from datetime import timedelta
from django.utils import timezone
from django.db.models import Avg, Count, F, ExpressionWrapper, DurationField
from django.db.models.functions import TruncDate
from rest_framework.views import APIView
from rest_framework.response import Response
from django.db import transaction
from rest_framework import status


class WIPLeadsView(ListAPIView):
    serializer_class = LeadSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        print(self.request)
        return (Lead.objects
                    .filter(assigned_to=user.supabase_uid, lead_status='WIP')
                    .order_by('-created_at'))



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

    

CLOSED = ['Resolved', 'Won', 'Lost', "Can't Resolve"]
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
        resolved = qs.filter(lead_status__in=CLOSED).count()

        avg_close = qs.filter(
            lead_status__in=CLOSED, updated_at__isnull=False
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


PENDING_STATUSES = ("Pending", "Follow-up", "New")
CLOSED_STATUSES  = ("Resolved", "Won", "Lost", "Can't Resolve", "WIP")

class GetNextLead(APIView):
    """
    Atomically fetch & assign the highest-scoring unassigned lead to the caller.
    """
    permission_classes = [IsAuthenticated]

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
                .filter(assigned_to=assignee_uuid, lead_status__in=PENDING_STATUSES)
            )
            if tenant_uuid:
                mine = mine.filter(tenant_id=tenant_uuid)

            mine_candidate = mine.order_by("-lead_score", "created_at", "id").first()
            if mine_candidate:
                # (Optional) nudge status to WIP and touch updated_at
                if mine_candidate.lead_status in ("New", "Pending"):
                    mine_candidate.lead_status = "WIP"
                    mine_candidate.updated_at = timezone.now()
                    mine_candidate.save(update_fields=["lead_status", "updated_at"])

                return Response({"lead": LeadSerializer(mine_candidate).data}, status=200)

            unassigned = (
                Lead.objects.select_for_update(skip_locked=True)
                .filter(assigned_to__isnull=True)
                .exclude(lead_status__in=CLOSED_STATUSES)
            )
            if tenant_uuid:
                unassigned = unassigned.filter(tenant_id=tenant_uuid)

            candidate = unassigned.order_by("-lead_score", "created_at", "id").first()
            if not candidate:
                return Response({}, status=200)

            candidate.assigned_to = assignee_uuid
            # Optional: set to WIP when claimed
            if not candidate.lead_status or candidate.lead_status in ("New", "Pending"):
                candidate.lead_status = "WIP"
            candidate.updated_at = timezone.now()
            candidate.save(update_fields=["assigned_to", "lead_status", "updated_at"])

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