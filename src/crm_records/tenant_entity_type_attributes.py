"""
Build lead-scoring attribute paths from tenant_entity_types.schema_json.

Discovery stores flat field keys under schema_json.fields, e.g.
{"fields": {"lead_score": {"type": "number"}, "name": {"type": "string"}}}
→ ["data.lead_score", "data.name", ...] plus standard Record columns.
"""

from __future__ import annotations

from typing import Any

from core.models import TenantEntityType

RECORD_BASE_ATTRIBUTES: tuple[str, ...] = (
    "id",
    "tenant_id",
    "entity_type",
    "name",
    "data",
    "created_at",
    "updated_at",
)


def attributes_from_schema_json(schema_json: dict[str, Any] | None) -> list[str]:
    fields: dict[str, Any] = {}
    if isinstance(schema_json, dict):
        raw = schema_json.get("fields")
        if isinstance(raw, dict):
            fields = raw

    data_attrs = [f"data.{name}" for name in sorted(fields.keys()) if name]
    return sorted(set(RECORD_BASE_ATTRIBUTES) | set(data_attrs))


def attributes_from_tenant_entity_type(entity: TenantEntityType) -> list[str]:
    return attributes_from_schema_json(entity.schema_json)
