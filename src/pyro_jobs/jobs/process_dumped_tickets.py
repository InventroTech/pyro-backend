"""
Process Dumped Support Tickets Job
====================================
Finds all tenants with unprocessed ``support_ticket_dump`` rows and processes
each tenant's dumps: deduplicate, upsert ``records``.

Runs every 5 minutes via Brahma (replaces the per-tenant background job
that was self-scheduled by the bg worker).
"""
from __future__ import annotations

import logging

from django.db.models import Q
from django.utils import timezone

logger = logging.getLogger(__name__)


def run_process_dumped_tickets(payload: dict) -> dict:
    from support_ticket.models import SupportTicketDump
    from support_ticket.views import (
        on_ticket_created_after_dump,
        process_dumped_tickets,
    )

    payload = payload or {}
    scoped_tenant_id = payload.get("tenant_id")

    if scoped_tenant_id:
        tenant_ids = [scoped_tenant_id]
    else:
        tenant_ids = list(
            SupportTicketDump.objects.filter(
                Q(is_processed__isnull=True) | Q(is_processed=False)
            )
            .values_list("tenant_id", flat=True)
            .distinct()
        )

    total_inserted = 0
    total_mirrored = 0
    total_marked = 0
    tenants_processed = 0

    for tid in tenant_ids:
        result = process_dumped_tickets(
            tenant_id=tid,
            on_ticket_created=on_ticket_created_after_dump,
        )
        total_inserted += result.inserted_tickets
        total_mirrored += result.mirrored_records
        total_marked += result.marked_processed
        tenants_processed += 1
        logger.info(
            "[ProcessDumpedTickets] tenant=%s inserted=%s mirrored=%s marked=%s",
            tid,
            result.inserted_tickets,
            result.mirrored_records,
            result.marked_processed,
        )

    logger.info(
        "[ProcessDumpedTickets] done tenants=%s total_inserted=%s total_mirrored=%s total_marked=%s",
        tenants_processed,
        total_inserted,
        total_mirrored,
        total_marked,
    )
    return {
        "success": True,
        "tenants_processed": tenants_processed,
        "inserted_tickets": total_inserted,
        "mirrored_records": total_mirrored,
        "marked_processed": total_marked,
        "timestamp": timezone.now().isoformat(),
    }
