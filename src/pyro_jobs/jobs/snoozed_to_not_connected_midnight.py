"""
Snoozed → NOT_CONNECTED Midnight Job
=====================================
For SALES LEAD leads in SNOOZED stage belonging to tenant d6db1158-2212-4d94-bb01-2c28b971d9a9:
sets lead_stage to NOT_CONNECTED, clears assigned_to, removes snooze_unassign_at.

Only processes leads whose next_call_at falls on today's calendar date (UTC).
Runs once per UTC calendar day at 17:30 UTC (11 PM IST) via Brahma.
"""
from __future__ import annotations

import logging

from django.conf import settings
from django.utils import timezone

from crm_records.models import Record

logger = logging.getLogger(__name__)

TENANT_ID = "d6db1158-2212-4d94-bb01-2c28b971d9a9"
CHUNK_SIZE = 500


def run_snoozed_to_not_connected_midnight(payload: dict) -> dict:
    tz_name = settings.TIME_ZONE
    qs = Record.objects.filter(
        tenant_id=TENANT_ID,
        entity_type="lead",
        data__contains={"lead_stage": "SNOOZED", "lead_status": "SALES LEAD"},
    ).extra(
        where=[
            """
            (data->>'next_call_at') IS NOT NULL
            AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
            AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
            AND (timezone(%s, (data->>'next_call_at')::timestamptz))::date
                = (timezone(%s, NOW()))::date
            """,
        ],
        params=[tz_name, tz_name],
    )

    # Fetch all matching IDs upfront with a plain query (no server-side cursor)
    # to avoid "cursor does not exist" errors on connection resets during iteration.
    record_ids = list(qs.values_list("id", flat=True))
    updated = 0
    for offset in range(0, len(record_ids), CHUNK_SIZE):
        chunk = Record.objects.filter(id__in=record_ids[offset:offset + CHUNK_SIZE])
        for record in chunk:
            data = (record.data or {}).copy() if isinstance(record.data, dict) else {}
            if data.get("lead_stage") != "SNOOZED" or data.get("lead_status") != "SALES LEAD":
                continue
            data["lead_stage"] = "NOT_CONNECTED"
            data["assigned_to"] = None
            data.pop("snooze_unassign_at", None)
            record.data = data
            record.save(update_fields=["data", "updated_at"])
            updated += 1
            logger.debug(
                "[SnoozedToNotConnectedMidnight] record_id=%s: NOT_CONNECTED + unassigned",
                record.id,
            )

    logger.info("[SnoozedToNotConnectedMidnight] completed updated=%s rows", updated)
    return {"success": True, "updated": updated, "timestamp": timezone.now().isoformat()}
