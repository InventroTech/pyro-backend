"""
Close selected leads so no RM is given them again.

Sets ``lead_stage`` to CLOSED (scoring and lead jobs already skip CLOSED leads)
and clears the assignee plus every scheduled follow-up, so the lead leaves the
RM's queue and no callback or unassign timer fires for it later. Call history
stays: ``call_attempts``, ``last_call_outcome`` and the first-assignment stamps
are kept for reporting.

Leads are picked by ``praja_id``, the id ops teams work with.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from django.core.cache import cache
from django.db import connection, transaction
from django.db.models.fields.json import KeyTextTransform
from django.utils import timezone
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from authz.permissions import IsTenantAuthenticated
from crm_records.models import EventLog, Record
from object_history.engine import HistoryEngine
from object_history.registry import get_config as get_history_config
from object_history.serializers import serialize_instance

logger = logging.getLogger(__name__)

CLOSED_STAGE = "CLOSED"

# Rows locked and written per transaction, so a long list does not hold every lock at once.
BATCH_SIZE = 1000

_PRAJA_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

# Keys that would put a closed lead back in front of an RM.
_FIELDS_TO_CLEAR = (
    "assigned_to",
    "next_call_at",
    "snooze_unassign_at",
    "not_connected_unassign_at",
)


class MakeClosedError(ValueError):
    """Bad request for the make-closed action."""


def parse_praja_ids(raw: Any) -> list[str]:
    """Return de-duplicated praja ids as text, or raise MakeClosedError."""
    if not isinstance(raw, list) or not raw:
        raise MakeClosedError("praja_ids must be a non-empty list of praja ids.")

    ids: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        parsed = _parse_praja_id(item)
        if parsed is None:
            raise MakeClosedError(f"praja_ids[{index}] is not a valid praja id.")
        if parsed not in seen:
            seen.add(parsed)
            ids.append(parsed)
    return ids


def make_leads_closed(*, tenant, praja_ids: list[str], actor: str | None = None) -> dict:
    """Close tenant leads, whatever their current stage, and stop any scheduled follow-up."""
    updated: list[str] = []
    skipped: list[dict] = []
    not_found: list[str] = []

    for start in range(0, len(praja_ids), BATCH_SIZE):
        _close_batch(
            tenant=tenant,
            praja_ids=praja_ids[start : start + BATCH_SIZE],
            actor=actor,
            updated=updated,
            skipped=skipped,
            not_found=not_found,
        )

    if updated:
        # Closing a fresh lead takes it out of the fresh pool count.
        cache.delete(f"fresh_leads_inventory:{tenant.id}")
        logger.info(
            "[MakeClosed] tenant=%s actor=%s updated=%s skipped=%s not_found=%s",
            tenant.id,
            actor,
            len(updated),
            len(skipped),
            len(not_found),
        )

    return {"updated": updated, "skipped": skipped, "not_found": not_found}


def _close_batch(
    *,
    tenant,
    praja_ids: list[str],
    actor: str | None,
    updated: list[str],
    skipped: list[dict],
    not_found: list[str],
) -> None:
    with transaction.atomic():
        # praja_id is unique per tenant for leads (records_lead_praja_id_tenant_unique).
        locked = {
            str(record.praja_id_text): record
            for record in (
                Record.objects.select_for_update()
                .filter(tenant=tenant, entity_type="lead")
                .annotate(praja_id_text=KeyTextTransform("praja_id", "data"))
                .filter(praja_id_text__in=praja_ids)
                .order_by("id")
            )
        }
        history_config = get_history_config(Record)
        now = timezone.now()
        changed: list[Record] = []
        history_entries: list[tuple[Record, dict]] = []
        events: list[EventLog] = []

        for praja_id in praja_ids:
            record = locked.get(praja_id)
            if record is None:
                not_found.append(praja_id)
                continue

            data = dict(record.data) if isinstance(record.data, dict) else {}
            previous = {
                "lead_stage": data.get("lead_stage"),
                "call_attempts": data.get("call_attempts"),
                "assigned_to": data.get("assigned_to"),
            }
            if not apply_closed(data):
                skipped.append({"praja_id": praja_id, "reason": "already_closed"})
                continue

            if history_config:
                history_entries.append((record, serialize_instance(record, history_config)))
            record.data = data
            record.updated_at = now
            changed.append(record)
            events.append(
                EventLog(
                    record=record,
                    tenant=tenant,
                    event="lead.made_closed",
                    payload={
                        "record_id": record.id,
                        "praja_id": praja_id,
                        "actor": actor,
                        "previous_lead_stage": previous["lead_stage"],
                        "previous_call_attempts": previous["call_attempts"],
                        "previous_assigned_to": previous["assigned_to"],
                    },
                    timestamp=now,
                )
            )
            updated.append(praja_id)

        if not changed:
            return
        # Same result as apply_closed; the rows are locked above, so nothing changes in between.
        with connection.cursor() as cursor:
            cursor.execute(
                f"UPDATE {Record._meta.db_table} "
                "SET data = (data - %s::text[]) || jsonb_build_object('lead_stage', %s::text), "
                "updated_at = %s "
                "WHERE id = ANY(%s)",
                [list(_FIELDS_TO_CLEAR), CLOSED_STAGE, now, [record.id for record in changed]],
            )
        EventLog.objects.bulk_create(events)
        HistoryEngine.bulk_capture_updates(
            history_entries,
            action="updated",
            extra_metadata={"action": "make_closed"},
        )


def apply_closed(data: dict) -> bool:
    """Mutate ``data`` into a closed lead. Return True when something changed."""
    changed = False
    if str(data.get("lead_stage") or "").strip().upper() != CLOSED_STAGE:
        data["lead_stage"] = CLOSED_STAGE
        changed = True
    for key in _FIELDS_TO_CLEAR:
        if key in data:
            data.pop(key, None)
            changed = True
    return changed


def _parse_praja_id(item: Any) -> str | None:
    if isinstance(item, bool):
        return None
    if isinstance(item, int):
        return str(item) if item > 0 else None
    if isinstance(item, str):
        text = item.strip()
        return text if _PRAJA_ID_RE.match(text) else None
    return None


class MakeLeadsClosedView(APIView):
    """
    POST /crm-records/leads/make-closed/

    Body: {"praja_ids": ["1793876", "1793877"]}
    """

    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        tenant = getattr(request, "tenant", None)
        if tenant is None:
            return Response({"error": "Tenant is required."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            praja_ids = parse_praja_ids(request.data.get("praja_ids"))
        except MakeClosedError as exc:
            return Response({"error": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        user = getattr(request, "user", None)
        actor = None
        if user is not None:
            actor = getattr(user, "email", None) or getattr(user, "supabase_uid", None)

        result = make_leads_closed(tenant=tenant, praja_ids=praja_ids, actor=actor)
        return Response(result, status=status.HTTP_200_OK)
