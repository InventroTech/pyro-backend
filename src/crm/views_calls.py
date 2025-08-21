from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
from .models import Lead
from .policy import MAX_ATTEMPTS
from .policy_utils import next_due
from django.utils.dateparse import parse_datetime

class LeadCallOutcomeView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, lead_id: int):
        outcome = request.data.get("outcome")
        callback_at = request.data.get("callbackAt")

        if outcome not in ["Won", "Lost", "WIP"]:
            return Response({"error": "invalid outcome"}, status=400)

        lead = Lead.objects.filter(id=lead_id).first()
        if not lead:
            return Response({"error": "lead not found"}, status=404)

        now = timezone.now()
        attempt_no = (lead.attempt_count or 0) + 1
        lead.attempt_count = attempt_no
        lead.last_call_outcome = outcome

        if outcome in ("Won", "Lost"):
            lead.next_call_at = None
            lead.lead_status = outcome

        elif outcome == "WIP" and callback_at:
            dt = parse_datetime(callback_at)
            lead.next_call_at = dt
            lead.lead_status = "WIP"

        elif attempt_no >= MAX_ATTEMPTS:
            lead.next_call_at = None
            lead.lead_status = "Lost"

        else:
            lead.next_call_at = next_due(now, attempt_no - 1)

        lead.save(update_fields=["attempt_count", "last_call_outcome", "next_call_at", "lead_status"])
        
        return Response({
            "ok": True,
            "attempt_count": lead.attempt_count,
            "next_call_at": lead.next_call_at,
        }, status=200)