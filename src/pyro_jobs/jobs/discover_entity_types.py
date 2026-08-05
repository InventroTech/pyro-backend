"""
Discover Entity Types Job
=========================
Incrementally discovers tenant entity types and data fields from records.

Payload (all optional):
  batch_size (int): records per batch (default 1000).
  max_runtime_seconds (int): stop after this many seconds (default unlimited).
"""
from __future__ import annotations

import logging

from django.utils import timezone

logger = logging.getLogger(__name__)


def run_discover_entity_types(payload: dict) -> dict:
    from crm_records.entity_type_discovery import discover_entity_types_from_records

    payload = payload or {}
    batch_size = int(payload.get("batch_size") or 1000)
    max_runtime_seconds = payload.get("max_runtime_seconds")
    if max_runtime_seconds is not None:
        max_runtime_seconds = int(max_runtime_seconds)

    result = discover_entity_types_from_records(
        batch_size=batch_size,
        max_runtime_seconds=max_runtime_seconds,
    )

    logger.info(
        "[DiscoverEntityTypes] processed=%s touched=%s updated=%s has_more=%s",
        result.processed,
        result.entity_types_touched,
        result.schemas_updated,
        result.has_more,
    )
    return {
        "success": True,
        "processed": result.processed,
        "entity_types_touched": result.entity_types_touched,
        "schemas_updated": result.schemas_updated,
        "last_processed_record_id": result.last_processed_record_id,
        "last_processed_updated_at": result.last_processed_updated_at,
        "has_more": result.has_more,
        "timestamp": timezone.now().isoformat(),
    }
