"""
Reset selected leads so they re-enter the fresh pool.

A lead is fresh only when ``lead_stage`` is FRESH, ``call_attempts`` is 0, and
``assigned_to`` is empty. Leftover outcome and assignment stamps make the next
pull look like a retry, so this action clears those too.

Leads are picked by ``praja_id``, the id ops teams work with.
"""

from __future__ import annotations

import json
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

# Rows locked and written per transaction, so a long list does not hold every lock at once.
BATCH_SIZE = 1000

_PRAJA_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

# Keys that keep a touched lead out of the fresh bucket or off the daily-limit count.
_FIELDS_TO_CLEAR = (
    "assigned_to",
    "last_call_outcome",
    "next_call_at",
    "snooze_unassign_at",
    "not_connected_unassign_at",
    "first_assigned_at",
    "first_assigned_to",
    "first_assigned_today_at",
    "first_assignment_today_date",
)


class MakeFreshError(ValueError):
    """Bad request for the make-fresh action."""


def parse_praja_ids(raw: Any) -> list[str]:
    """Return de-duplicated praja ids as text, or raise MakeFreshError."""
    if not isinstance(raw, list) or not raw:
        raise MakeFreshError("praja_ids must be a non-empty list of praja ids.")

    ids: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(raw):
        parsed = _parse_praja_id(item)
        if parsed is None:
            raise MakeFreshError(f"praja_ids[{index}] is not a valid praja id.")
        if parsed not in seen:
            seen.add(parsed)
            ids.append(parsed)
    return ids


def make_leads_fresh(*, tenant, praja_ids: list[str], actor: str | None = None) -> dict:
    """
    Reset tenant leads to FRESH with 0 attempts and no assignee, whatever their current stage.
    """
    updated: list[str] = []
    skipped: list[dict] = []
    not_found: list[str] = []

    for start in range(0, len(praja_ids), BATCH_SIZE):
        _reset_batch(
            tenant=tenant,
            praja_ids=praja_ids[start : start + BATCH_SIZE],
            actor=actor,
            updated=updated,
            skipped=skipped,
            not_found=not_found,
        )

    if updated:
        cache.delete(f"fresh_leads_inventory:{tenant.id}")
        logger.info(
            "[MakeFresh] tenant=%s actor=%s updated=%s skipped=%s not_found=%s",
            tenant.id,
            actor,
            len(updated),
            len(skipped),
            len(not_found),
        )

    return {"updated": updated, "skipped": skipped, "not_found": not_found}


def _reset_batch(
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
            if not _apply_fresh_reset(data):
                skipped.append({"praja_id": praja_id, "reason": "already_fresh"})
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
                    event="lead.made_fresh",
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
        # Same result as _apply_fresh_reset; the rows are locked above, so nothing changes in between.
        with connection.cursor() as cursor:
            cursor.execute(
                f"UPDATE {Record._meta.db_table} "
                "SET data = (data - %s::text[]) || %s::jsonb, updated_at = %s "
                "WHERE id = ANY(%s)",
                [
                    list(_FIELDS_TO_CLEAR),
                    json.dumps({"lead_stage": "FRESH", "call_attempts": 0}),
                    now,
                    [record.id for record in changed],
                ],
            )
        EventLog.objects.bulk_create(events)
        HistoryEngine.bulk_capture_updates(
            history_entries,
            action="updated",
            extra_metadata={"action": "make_fresh"},
        )


def _apply_fresh_reset(data: dict) -> bool:
    """Mutate ``data`` into a fresh lead. Return True when something changed."""
    changed = False
    if str(data.get("lead_stage") or "").strip().upper() != "FRESH":
        data["lead_stage"] = "FRESH"
        changed = True
    if not _attempts_are_zero(data.get("call_attempts")) or data.get("call_attempts") != 0:
        data["call_attempts"] = 0
        changed = True
    for key in _FIELDS_TO_CLEAR:
        if key in data:
            data.pop(key, None)
            changed = True
    return changed


def _attempts_are_zero(value: Any) -> bool:
    if value in (None, "", "null", "None"):
        return True
    try:
        return int(value) == 0
    except (TypeError, ValueError):
        return False


def _parse_praja_id(item: Any) -> str | None:
    if isinstance(item, bool):
        return None
    if isinstance(item, int):
        return str(item) if item > 0 else None
    if isinstance(item, str):
        text = item.strip()
        return text if _PRAJA_ID_RE.match(text) else None
    return None


class MakeLeadsFreshView(APIView):
    """
    POST /crm-records/leads/make-fresh/

    Body: {"praja_ids": ["1793876", "1793877"]}
    """

    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        tenant = getattr(request, "tenant", None)
        if tenant is None:
            return Response({"error": "Tenant is required."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            praja_ids = parse_praja_ids(request.data.get("praja_ids"))
        except MakeFreshError as exc:
            return Response({"error": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        user = getattr(request, "user", None)
        actor = None
        if user is not None:
            actor = getattr(user, "email", None) or getattr(user, "supabase_uid", None)

        result = make_leads_fresh(tenant=tenant, praja_ids=praja_ids, actor=actor)
        return Response(result, status=status.HTTP_200_OK)
