"""Shared Mixpanel property builders for lead events."""

from __future__ import annotations

from typing import Any, Dict, Optional

from crm_records.models import Record


def _coerce_optional_bool(value: Any) -> Optional[bool]:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in ("true", "1", "yes", "y"):
        return True
    if s in ("false", "0", "no", "n"):
        return False
    return None


def _coerce_optional_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def apply_lead_mixpanel_attributes(props: Dict[str, Any]) -> Dict[str, Any]:
    """
    Attributes every lead Mixpanel event must carry, with Mixpanel-friendly types.
    Values come from the lead payload; missing / null stays null.
    """
    props["referred_through_precheck_logic"] = _coerce_optional_bool(
        props.get("referred_through_precheck_logic")
    )
    props["contact_graph_score"] = _coerce_optional_float(props.get("contact_graph_score"))
    return props


def lead_mixpanel_properties(lead_data: Optional[Dict[str, Any]], **base: Any) -> Dict[str, Any]:
    """
    Base lead payload for Mixpanel: event-specific ``base`` fields, overlaid with all
    lead attributes (lead data wins on key clashes), plus the shared lead attributes.
    """
    props: Dict[str, Any] = dict(base)
    if isinstance(lead_data, dict):
        props.update(lead_data)
    return apply_lead_mixpanel_attributes(props)


def lead_created_mixpanel_properties(record: Record) -> Dict[str, Any]:
    """Properties for ``pyro_crm_lead_created``."""
    lead_data = dict(record.data or {})
    if record.pyro_data:
        lead_data.update(record.pyro_data)
    return lead_mixpanel_properties(
        lead_data,
        lead_id=record.id,
        tenant_id=str(record.tenant.id) if record.tenant else None,
        entity_type=record.entity_type,
        created_at=record.created_at.isoformat() if record.created_at else None,
        updated_at=record.updated_at.isoformat() if record.updated_at else None,
    )
