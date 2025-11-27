from __future__ import annotations

from typing import Any, Dict, Iterable, Tuple

from django.db import models

from .registry import HistoryConfig


def serialize_instance(instance: models.Model, config: HistoryConfig) -> Dict[str, Any]:
    """
    Serialize instance according to config.
    """
    if config.snapshot_strategy == "custom" and config.custom_serializer:
        data = config.custom_serializer(instance)
        if not isinstance(data, dict):
            raise ValueError("custom_serializer must return a dict")
        return data

    tracked = {}
    for field_name in config.track_fields:
        tracked[field_name] = getattr(instance, field_name, None)
    if config.snapshot_strategy == "minimal":
        # minimal strategy defers to diff computation to prune unchanged fields
        return tracked
    if config.snapshot_strategy == "full":
        return tracked
    raise ValueError(f"Unsupported snapshot_strategy {config.snapshot_strategy}")


def redact_payload(
    payload: Dict[str, Any], redact_fields: Iterable[str]
) -> Dict[str, Any]:
    if not payload:
        return payload
    redacted = {}
    for key, value in payload.items():
        if key in redact_fields:
            redacted[key] = "[REDACTED]"
        else:
            redacted[key] = value
    return redacted


def compute_diff(
    before: Dict[str, Any], after: Dict[str, Any], redact_fields: Iterable[str]
) -> Dict[str, Dict[str, Any]]:
    diff = {}
    field_set = set(before.keys()) | set(after.keys())
    redact = set(redact_fields)
    for field in field_set:
        before_val = before.get(field)
        after_val = after.get(field)
        if before_val == after_val:
            continue
        diff[field] = {
            "from": "[REDACTED]" if field in redact else before_val,
            "to": "[REDACTED]" if field in redact else after_val,
        }
    return diff


__all__ = ["serialize_instance", "redact_payload", "compute_diff"]

