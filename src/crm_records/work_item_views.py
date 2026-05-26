"""API views for unified work-item pull and events."""

from __future__ import annotations

import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .mixins import TenantScopedMixin
from authz.permissions import IsTenantAuthenticated
from crm_records.models import Record
from crm_records.serializers import RecordSerializer
from crm_records.views import _flatten_lead_response
from crm_records.work_item_pipeline import WorkItemPipeline, ui_profile_for_record
from django.db import transaction

logger = logging.getLogger(__name__)


def _work_item_payload(record: Record) -> dict:
    profile = ui_profile_for_record(record)
    if profile == "support":
        data = record.data or {}
        return {
            "work_item": {
                "record": RecordSerializer(record).data,
                "entity_type": record.entity_type,
                "ui_profile": profile,
                "ticket": {
                    "id": data.get("support_ticket_id"),
                    **{k: data.get(k) for k in data},
                },
            }
        }
    flat = _flatten_lead_response(record)
    return {
        "work_item": {
            "record": RecordSerializer(record).data,
            "entity_type": record.entity_type,
            "ui_profile": profile,
            "lead": flat,
        }
    }


class WorkItemNextView(TenantScopedMixin, APIView):
    """GET /crm-records/work-item/next/ — next self-trial lead or support ticket by bucket priority."""

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant = request.tenant
        debug = request.query_params.get("debug") in ("1", "true", "yes")
        pipeline = WorkItemPipeline()
        record = pipeline.get_next(tenant=tenant, request_user=request.user, debug=debug)
        if not record:
            return Response({}, status=status.HTTP_200_OK)
        return Response(_work_item_payload(record), status=status.HTTP_200_OK)


class WorkItemEventView(TenantScopedMixin, APIView):
    """
    POST /crm-records/work-item/event/
    Body: { record_id, event, payload }
    Records are primary; support_ticket rows are mirrored after rule execution.
    """

    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        tenant = request.tenant
        record_id = request.data.get("record_id")
        event_name = request.data.get("event")
        payload = request.data.get("payload") or {}

        if not record_id or not event_name:
            return Response(
                {"error": "record_id and event are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            record = Record.objects.get(id=record_id, tenant=tenant)
        except Record.DoesNotExist:
            return Response({"error": "Record not found"}, status=status.HTTP_404_NOT_FOUND)

        if record.entity_type == "support_ticket":
            return self._dispatch_support_record_event(request, record, event_name, payload)

        return self._dispatch_lead_record_event(request, record, event_name, payload)

    def _dispatch_lead_record_event(self, request, record: Record, event_name: str, payload: dict):
        from crm_records.events import dispatch_event
        from crm_records.models import EventLog
        from django.utils import timezone

        try:
            with transaction.atomic():
                EventLog.objects.create(
                    record=record,
                    tenant=request.tenant,
                    event=event_name,
                    payload=payload,
                    timestamp=timezone.now(),
                )
                dispatch_event(event_name, record, payload)
                record.refresh_from_db()
        except Exception as exc:
            logger.exception("[WorkItemEvent] lead event failed: %s", exc)
            return Response({"error": str(exc)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(
            {
                "ok": True,
                "logged": True,
                "record": RecordSerializer(record).data,
                "lead": _flatten_lead_response(record),
                "message": f"Event '{event_name}' processed",
            },
            status=status.HTTP_200_OK,
        )

    def _dispatch_support_record_event(self, request, record: Record, event_name: str, payload: dict):
        from support_ticket.support_dispatch import dispatch_support_record_event

        try:
            with transaction.atomic():
                dispatch_support_record_event(
                    tenant=request.tenant,
                    record=record,
                    event_name=event_name,
                    payload=payload,
                    log_event=True,
                )
                record.refresh_from_db()
        except Exception as exc:
            logger.exception("[WorkItemEvent] failed: %s", exc)
            return Response({"error": str(exc)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(
            {
                "ok": True,
                "logged": True,
                "record": RecordSerializer(record).data,
                "message": f"Event '{event_name}' processed",
            },
            status=status.HTTP_200_OK,
        )
