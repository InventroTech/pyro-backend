import time
from dataclasses import dataclass
from typing import Any

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from core.models import EntityTypeDiscoverySyncState, TenantEntityType

from .models import Record


DEFAULT_JOB_NAME = "entity_type_discovery"
DEFAULT_BATCH_SIZE = 1000


@dataclass(frozen=True)
class DiscoveryResult:
    processed: int
    entity_types_touched: int
    schemas_updated: int
    last_processed_record_id: int
    last_processed_updated_at: str | None
    has_more: bool


def infer_json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "string"


def extract_field_schema(data: Any) -> dict[str, dict[str, str]]:
    if not isinstance(data, dict):
        return {}
    return {
        str(field): {"type": infer_json_type(value)}
        for field, value in sorted(data.items())
        if field is not None
    }


def _merge_type(existing_type: str | None, incoming_type: str) -> str:
    if not existing_type:
        return incoming_type
    if existing_type == incoming_type:
        return existing_type
    if existing_type == "null":
        return incoming_type
    if incoming_type == "null":
        return existing_type
    if existing_type == "mixed":
        return existing_type
    return "mixed"


def merge_schema_fields(
    existing_schema: dict[str, Any] | None,
    incoming_fields: dict[str, dict[str, str]],
) -> tuple[dict[str, Any], bool]:
    schema = existing_schema.copy() if isinstance(existing_schema, dict) else {}
    existing_fields = schema.get("fields")
    if not isinstance(existing_fields, dict):
        existing_fields = {}

    changed = False
    merged_fields = existing_fields.copy()
    for field_name, incoming_meta in incoming_fields.items():
        incoming_type = incoming_meta.get("type", "string")
        current_meta = merged_fields.get(field_name)
        current_type = current_meta.get("type") if isinstance(current_meta, dict) else None
        merged_type = _merge_type(current_type, incoming_type)

        if current_type != merged_type:
            merged_fields[field_name] = {"type": merged_type}
            changed = True

    schema["fields"] = dict(sorted(merged_fields.items()))
    return schema, changed


def reset_discovery_bookmark(job_name: str = DEFAULT_JOB_NAME):
    EntityTypeDiscoverySyncState.objects.update_or_create(
        job_name=job_name,
        defaults={
            "last_processed_updated_at": None,
            "last_processed_record_id": 0,
            "last_success_at": None,
            "last_error": None,
        },
    )


def discover_entity_types_from_records(
    *,
    job_name: str = DEFAULT_JOB_NAME,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_runtime_seconds: int | None = None,
) -> DiscoveryResult:
    batch_size = max(1, int(batch_size or DEFAULT_BATCH_SIZE))
    started_at = time.monotonic()
    total_processed = 0
    total_touched = 0
    total_updated = 0
    last_record_id = 0
    last_updated_at = None
    has_more = False

    while True:
        result = _process_discovery_batch(job_name=job_name, batch_size=batch_size)
        total_processed += result.processed
        total_touched += result.entity_types_touched
        total_updated += result.schemas_updated
        last_record_id = result.last_processed_record_id or last_record_id
        last_updated_at = result.last_processed_updated_at or last_updated_at
        has_more = result.has_more

        if not result.has_more:
            break
        if max_runtime_seconds is None:
            break
        if time.monotonic() - started_at >= max_runtime_seconds:
            break

    return DiscoveryResult(
        processed=total_processed,
        entity_types_touched=total_touched,
        schemas_updated=total_updated,
        last_processed_record_id=last_record_id,
        last_processed_updated_at=last_updated_at,
        has_more=has_more,
    )


def _process_discovery_batch(*, job_name: str, batch_size: int) -> DiscoveryResult:
    with transaction.atomic():
        state, _created = EntityTypeDiscoverySyncState.objects.select_for_update().get_or_create(
            job_name=job_name,
            defaults={
                "last_processed_updated_at": None,
                "last_processed_record_id": 0,
            },
        )

        queryset = Record.objects.filter(tenant_id__isnull=False).order_by("updated_at", "id")
        if state.last_processed_updated_at is not None:
            queryset = queryset.filter(
                Q(updated_at__gt=state.last_processed_updated_at)
                | Q(
                    updated_at=state.last_processed_updated_at,
                    id__gt=state.last_processed_record_id,
                )
            )

        records = list(
            queryset.values("id", "tenant_id", "entity_type", "data", "updated_at")[
                : batch_size + 1
            ]
        )
        has_more = len(records) > batch_size
        records = records[:batch_size]

        if not records:
            state.last_success_at = timezone.now()
            state.last_error = None
            state.save(update_fields=["last_success_at", "last_error", "updated_at"])
            return DiscoveryResult(
                processed=0,
                entity_types_touched=0,
                schemas_updated=0,
                last_processed_record_id=state.last_processed_record_id,
                last_processed_updated_at=(
                    state.last_processed_updated_at.isoformat()
                    if state.last_processed_updated_at
                    else None
                ),
                has_more=False,
            )

        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for record in records:
            tenant_id = str(record["tenant_id"])
            entity_type = (record["entity_type"] or "").strip()
            if not entity_type:
                continue

            key = (tenant_id, entity_type)
            group = grouped.setdefault(
                key,
                {
                    "fields": {},
                    "first_seen_at": record["updated_at"],
                    "last_seen_at": record["updated_at"],
                    "last_seen_record_id": record["id"],
                },
            )
            incoming_fields = extract_field_schema(record["data"])
            merged_group_schema, _changed = merge_schema_fields(
                {"fields": group["fields"]},
                incoming_fields,
            )
            group["fields"] = merged_group_schema["fields"]
            if record["updated_at"] < group["first_seen_at"]:
                group["first_seen_at"] = record["updated_at"]
            if (
                record["updated_at"] > group["last_seen_at"]
                or record["id"] > group["last_seen_record_id"]
            ):
                group["last_seen_at"] = record["updated_at"]
                group["last_seen_record_id"] = record["id"]

        schemas_updated = 0
        for (tenant_id, entity_type), group in grouped.items():
            entity, created = TenantEntityType.objects.select_for_update().get_or_create(
                tenant_id=tenant_id,
                entity_type=entity_type,
                defaults={
                    "schema_json": {"fields": group["fields"]},
                    "fields_count": len(group["fields"]),
                    "first_seen_at": group["first_seen_at"],
                    "last_seen_at": group["last_seen_at"],
                    "last_seen_record_id": group["last_seen_record_id"],
                },
            )
            if created:
                schemas_updated += 1
                continue

            merged_schema, schema_changed = merge_schema_fields(
                entity.schema_json,
                group["fields"],
            )
            metadata_changed = False
            if entity.first_seen_at is None or group["first_seen_at"] < entity.first_seen_at:
                entity.first_seen_at = group["first_seen_at"]
                metadata_changed = True
            if (
                entity.last_seen_at is None
                or group["last_seen_at"] > entity.last_seen_at
                or group["last_seen_record_id"] > entity.last_seen_record_id
            ):
                entity.last_seen_at = group["last_seen_at"]
                entity.last_seen_record_id = group["last_seen_record_id"]
                metadata_changed = True

            if schema_changed or metadata_changed:
                entity.schema_json = merged_schema
                entity.fields_count = len(merged_schema.get("fields", {}))
                entity.save(
                    update_fields=[
                        "schema_json",
                        "fields_count",
                        "first_seen_at",
                        "last_seen_at",
                        "last_seen_record_id",
                        "updated_at",
                    ]
                )
                if schema_changed:
                    schemas_updated += 1

        last_record = records[-1]
        state.last_processed_updated_at = last_record["updated_at"]
        state.last_processed_record_id = last_record["id"]
        state.last_success_at = timezone.now()
        state.last_error = None
        state.save(
            update_fields=[
                "last_processed_updated_at",
                "last_processed_record_id",
                "last_success_at",
                "last_error",
                "updated_at",
            ]
        )

        return DiscoveryResult(
            processed=len(records),
            entity_types_touched=len(grouped),
            schemas_updated=schemas_updated,
            last_processed_record_id=state.last_processed_record_id,
            last_processed_updated_at=state.last_processed_updated_at.isoformat(),
            has_more=has_more,
        )
