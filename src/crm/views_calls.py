from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
from .models import Lead
from .policy import MAX_ATTEMPTS
from .policy_utils import next_due
from django.utils.dateparse import parse_datetime
from drf_spectacular.utils import (
    extend_schema,
    OpenApiParameter,
    OpenApiExample,
    OpenApiResponse,
)
from drf_spectacular.types import OpenApiTypes
from .serializers import LeadOutcomeRequestSerializer, LeadOutcomeResponseSerializer, ErrorSerializer
from django.utils.timezone import make_aware, is_naive
import datetime




def _normalize_dt(value):
    """Accept ISO8601 string or datetime; return aware datetime or None. Raise 400-friendly error dict."""
    if value is None or value == "":
        return None

    # If DRF (or some client) already gave a datetime object
    if isinstance(value, datetime.datetime):
        dt = value
    elif isinstance(value, str):
        dt = parse_datetime(value)
        if dt is None:
            raise ValueError("callbackAt must be ISO 8601 (e.g., 2025-08-25T14:30:00Z)")
    else:
        raise ValueError("callbackAt must be an ISO 8601 string or a datetime")

    if is_naive(dt):
        dt = make_aware(dt)
    return dt


class LeadCallOutcomeView(APIView):
    permission_classes = [IsAuthenticated]
    @extend_schema(
      summary="Record outcome of a lead call",
      description=(
        "Updates a lead’s call outcome and schedules the next call as needed.\n\n"
        "- **won/lost**: clears schedule, finalizes status\n"
        "- **call_later**: requires `callbackAt`\n"
        "- Otherwise: auto-schedules based on policy"
      ),
      parameters=[
          OpenApiParameter(
              name="lead_id",
              type=OpenApiTypes.INT,
              location=OpenApiParameter.PATH,
              required=True,
              description="Lead ID (integer primary key).",
          )
      ],
      request=LeadOutcomeRequestSerializer,
      responses={
          200: LeadOutcomeResponseSerializer,
          400: OpenApiResponse(response=ErrorSerializer, description="Invalid input"),
          401: OpenApiResponse(description="Unauthorized"),
          404: OpenApiResponse(response=ErrorSerializer, description="Lead not found"),
      },
      examples=[
          OpenApiExample(
              "Mark as won",
              value={"outcome": "won"},
              request_only=True,
          ),
          OpenApiExample(
              "Mark as lost",
              value={"outcome": "lost"},
              request_only=True,
          ),
          OpenApiExample(
              "Auto Call Later Scheduling",
              value={"outcome": "call_later"},
              request_only=True,
          ),
          OpenApiExample(
              "Schedule for later",
              value={"outcome": "call_later", "callbackAt": "2025-08-25T14:30:00Z"},
              request_only=True,
          ),
          OpenApiExample(
              "Success response",
              value={"ok": True, "attempt_count": 3, "next_call_at": None},
              response_only=True,
          ),
      ],
    )

    def post(self, request, lead_id: int):
        ser = LeadOutcomeRequestSerializer(data=request.data)
        if not ser.is_valid():
            return Response(ser.errors, status=status.HTTP_400_BAD_REQUEST)
        data = ser.validated_data
        outcome = request.data.get("outcome")
        raw_callback_at = request.data.get("callbackAt")
        dt = None

        if outcome not in ["won", "lost", "call_later"]:
            return Response({"error": "invalid outcome"}, status=400)
        if raw_callback_at:
            try:
                dt = _normalize_dt(raw_callback_at)
            except ValueError as e:
                return Response({"error": str(e)}, status=400)


        lead = Lead.objects.filter(id=lead_id).first()
        if not lead:
            return Response({"error": "lead not found"}, status=404)

        now = timezone.now()
        attempt_no = (lead.attempt_count or 0) + 1
        lead.attempt_count = attempt_no
        lead.last_call_outcome = outcome

        if outcome in ("won", "lost"):
            lead.next_call_at = None
            lead.lead_status = outcome
            lead.resolved_at = now

        elif outcome == "call_later" and raw_callback_at:
            lead.next_call_at = dt
            lead.lead_status = outcome

        elif attempt_no >= MAX_ATTEMPTS:
            lead.next_call_at = None
            lead.lead_status = "closed"
            lead.resolved_at = now

        else:
            lead.next_call_at = next_due(now, attempt_no - 1)
            lead.lead_status = outcome

        lead.save(update_fields=["attempt_count", "last_call_outcome", "next_call_at", "lead_status", "resolved_at"])
        
        return Response({
            "ok": True,
            "attempt_count": lead.attempt_count,
            "next_call_at": lead.next_call_at,
        }, status=200)