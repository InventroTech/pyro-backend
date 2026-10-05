"""
Combined request status for inventory_request / unmannd_request.

``data.status`` is the single lifecycle status shown in the UI:

    NEW_REQUEST → REQ_TO_VERIFY → APPROVED → IN_CART → ORDERED → DELIVERED / EXCEPTION
    (ON_HOLD / REJECTED at any pre-order step)

``data.shipment_status`` is kept for carrier detail (IN_TRANSIT, OUT_FOR_DELIVERY…)
and is kept in step with ``status`` by :func:`sync_request_status`.

Per-tenant overrides (labels, text colour / background, order, page, extra statuses, extra pages)
live in ``EntityTypeSchema.status_config`` and are merged over the defaults below.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Set

REQUEST_ENTITY_TYPES = frozenset({"inventory_request", "unmannd_request"})

NEW_REQUEST = "NEW_REQUEST"
REQ_TO_VERIFY = "REQ_TO_VERIFY"
APPROVED = "APPROVED"
IN_CART = "IN_CART"
ON_HOLD = "ON_HOLD"
REJECTED = "REJECTED"
ORDERED = "ORDERED"
DELIVERED = "DELIVERED"
EXCEPTION = "EXCEPTION"

LEGACY_STATUS_ALIASES: Dict[str, str] = {
    "VENDOR_IDENTIFIED": APPROVED,
    "IN_SHIPPING": ORDERED,
}

PRE_ORDER_STATUSES = frozenset({APPROVED, IN_CART})
POST_ORDER_STATUSES = frozenset({ORDERED, DELIVERED, EXCEPTION})
TERMINAL_STATUSES = frozenset({DELIVERED, EXCEPTION})
IN_FLIGHT_SHIPMENT_STATUSES = frozenset({"ORDERED", "IN_TRANSIT", "OUT_FOR_DELIVERY"})
SHIPPING_SHIPMENT_STATUSES = IN_FLIGHT_SHIPMENT_STATUSES | TERMINAL_STATUSES

DEFAULT_PAGES: List[Dict[str, Any]] = [
    {"id": "pending_approval", "label": "Pending Approvals", "order": 1},
    {"id": "in_cart", "label": "In Cart Items", "order": 2},
    {"id": "ordered", "label": "Ordered Items", "order": 3},
    {"id": "delivered", "label": "Delivered Items", "order": 4},
    {"id": "closed", "label": "Invoiced & Closed", "order": 5},
]

DEFAULT_STATUSES: List[Dict[str, Any]] = [
    {"value": NEW_REQUEST, "label": "New request", "color": "#78350F", "background": "#FFFBEB", "page": "pending_approval"},
    {"value": REQ_TO_VERIFY, "label": "Req to verify", "color": "#6D28D9", "background": "#F5F3FF", "page": "pending_approval"},
    {"value": APPROVED, "label": "Approved", "color": "#16A34A", "background": "#DCFCE7", "page": "pending_approval"},
    {"value": IN_CART, "label": "In cart", "color": "#1B6FE8", "background": "#E8F1FD", "page": "in_cart"},
    {"value": ON_HOLD, "label": "On hold", "color": "#F97316", "background": "#FFF7ED", "page": "pending_approval"},
    {"value": REJECTED, "label": "Rejected", "color": "#DC2626", "background": "#FEE2E2", "page": "closed"},
    {"value": ORDERED, "label": "Ordered", "color": "#1A3673", "background": "#EEF2FA", "page": "ordered"},
    {"value": DELIVERED, "label": "Delivered", "color": "#15803D", "background": "#DCFCE7", "page": "delivered"},
    {"value": EXCEPTION, "label": "Exception", "color": "#B91C1C", "background": "#FEE2E2", "page": "ordered"},
]

BUILTIN_STATUS_VALUES = frozenset(s["value"] for s in DEFAULT_STATUSES)


def normalize_status_code(raw: Any) -> str:
    if raw is None:
        return ""
    return str(raw).strip().upper().replace(" ", "_")


def normalize_request_status(raw: Any) -> str:
    """Upper-snake the value and map legacy codes (VENDOR_IDENTIFIED, IN_SHIPPING)."""
    code = normalize_status_code(raw)
    return LEGACY_STATUS_ALIASES.get(code, code)


def legacy_codes_for(status: str) -> Set[str]:
    """Old stored codes that mean ``status`` (used while un-migrated rows exist)."""
    return {old for old, new in LEGACY_STATUS_ALIASES.items() if new == status}


# ---------------------------------------------------------------------------
# Tenant config
# ---------------------------------------------------------------------------


def _clean_page(raw: Any, index: int) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, Mapping):
        return None
    page_id = str(raw.get("id") or "").strip()
    if not page_id:
        return None
    label = str(raw.get("label") or page_id).strip()
    order = raw.get("order")
    return {
        "id": page_id,
        "label": label,
        "order": order if isinstance(order, (int, float)) else index + 1,
    }


def merge_status_config(tenant_config: Any) -> Dict[str, Any]:
    """
    Merge a tenant ``status_config`` over the defaults.

    - ``pages``: a tenant list replaces the default pages.
    - ``statuses``: entries override defaults by ``value``; unknown values are appended.
      Built-in statuses can be hidden (``active: false``) but never removed, because
      workflow buttons, emails and the tracking job depend on them.
    """
    cfg = tenant_config if isinstance(tenant_config, Mapping) else {}

    pages = [dict(p) for p in DEFAULT_PAGES]
    raw_pages = cfg.get("pages")
    if isinstance(raw_pages, list):
        cleaned = [p for p in (_clean_page(p, i) for i, p in enumerate(raw_pages)) if p]
        if cleaned:
            pages = cleaned
    pages.sort(key=lambda p: p["order"])
    page_ids = {p["id"] for p in pages}

    by_value: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for index, status in enumerate(DEFAULT_STATUSES):
        by_value[status["value"]] = {**status, "order": index + 1, "active": True, "builtin": True}
        order.append(status["value"])

    raw_statuses = cfg.get("statuses")
    if isinstance(raw_statuses, list):
        for raw in raw_statuses:
            if not isinstance(raw, Mapping):
                continue
            value = normalize_request_status(raw.get("value"))
            if not value:
                continue
            existing = by_value.get(value)
            entry = dict(existing) if existing else {
                "value": value,
                "label": value.replace("_", " ").capitalize(),
                "color": "gray",
                "background": None,
                "page": None,
                "order": len(order) + 1,
                "active": True,
                "builtin": False,
            }
            for key in ("label", "color", "background", "page"):
                if key in raw and raw.get(key) not in (None, ""):
                    entry[key] = str(raw.get(key)).strip()
            if isinstance(raw.get("order"), (int, float)):
                entry["order"] = raw["order"]
            if "active" in raw:
                entry["active"] = bool(raw.get("active"))
            by_value[value] = entry
            if not existing:
                order.append(value)

    statuses = [by_value[v] for v in order]
    for status in statuses:
        if status.get("page") not in page_ids:
            status["page"] = None
    statuses.sort(key=lambda s: s["order"])

    return {"pages": pages, "statuses": statuses}


def get_tenant_status_config(tenant, entity_type: str) -> Dict[str, Any]:
    """Merged config for a tenant + entity type (defaults when no schema row)."""
    tenant_config = None
    has_tenant_config = False
    if tenant is not None and entity_type:
        from crm_records.models import EntityTypeSchema

        schema = (
            EntityTypeSchema.objects.filter(tenant=tenant, entity_type=entity_type)
            .only("status_config")
            .first()
        )
        if schema is not None and isinstance(schema.status_config, Mapping) and schema.status_config:
            tenant_config = schema.status_config
            has_tenant_config = True
    merged = merge_status_config(tenant_config)
    merged["entity_type"] = entity_type
    merged["is_custom"] = has_tenant_config
    return merged


def status_labels(config: Optional[Mapping[str, Any]] = None) -> Dict[str, str]:
    statuses = (config or merge_status_config(None)).get("statuses") or []
    return {s["value"]: s["label"] for s in statuses}


def statuses_for_page(config: Mapping[str, Any], page_id: str) -> List[str]:
    return [s["value"] for s in config.get("statuses") or [] if s.get("page") == page_id]


def stage_filter_values(config: Mapping[str, Any], page_ids: Iterable[str]) -> List[str]:
    """Stored status codes (including legacy aliases) for one or more pages."""
    values: List[str] = []
    for page_id in page_ids:
        for status in statuses_for_page(config, page_id.strip()):
            for code in [status, *sorted(legacy_codes_for(status))]:
                if code not in values:
                    values.append(code)
    return values


def allowed_status_values(config: Mapping[str, Any]) -> Set[str]:
    return {s["value"] for s in config.get("statuses") or []}


# ---------------------------------------------------------------------------
# Status / shipment sync
# ---------------------------------------------------------------------------


def _is_generated_status_text(value: Any, label_map: Mapping[str, str]) -> bool:
    """True when status_text is empty, a raw status code, or one of the standard labels."""
    text = str(value or "").strip()
    if not text:
        return True
    code = normalize_status_code(text)
    if code == text and (code in label_map or code in LEGACY_STATUS_ALIASES):
        return True
    known = {label.lower() for label in label_map.values()}
    known.update(code.replace("_", " ").lower() for code in LEGACY_STATUS_ALIASES)
    return text.lower() in known


def sync_request_status(
    data: Dict[str, Any],
    previous: Optional[Mapping[str, Any]] = None,
    *,
    labels: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """
    Keep ``status`` and ``shipment_status`` consistent; mutates and returns ``data``.

    - Legacy codes are rewritten (VENDOR_IDENTIFIED → APPROVED, IN_SHIPPING → ORDERED).
    - Status set to ORDERED / DELIVERED / EXCEPTION updates ``shipment_status`` to match.
    - Shipment reaching DELIVERED / EXCEPTION moves an ordered request there; a shipment
      moving back in flight returns DELIVERED / EXCEPTION to ORDERED.
    - A shipment starting on an APPROVED / IN_CART request means it was ordered.
    - ``status_text`` follows the label whenever the stored status code changes.
    """
    if not isinstance(data, dict):
        return data
    prev = previous if isinstance(previous, Mapping) else {}
    label_map = labels or status_labels()

    raw_status = data.get("status")
    status = normalize_request_status(raw_status)
    prev_status = normalize_request_status(prev.get("status"))
    shipment = normalize_status_code(data.get("shipment_status"))
    prev_shipment = normalize_status_code(prev.get("shipment_status"))
    shipment_present = "shipment_status" in data

    status_changed = bool(status) and status != prev_status
    shipment_changed = shipment_present and shipment != prev_shipment

    if status_changed and status in TERMINAL_STATUSES:
        data["shipment_status"] = status
    elif status_changed and status == ORDERED:
        if shipment not in IN_FLIGHT_SHIPMENT_STATUSES:
            data["shipment_status"] = "ORDERED"
    elif shipment_changed and shipment:
        if status in POST_ORDER_STATUSES and shipment in TERMINAL_STATUSES:
            status = shipment
        elif status in TERMINAL_STATUSES and shipment in IN_FLIGHT_SHIPMENT_STATUSES:
            status = ORDERED
        elif status in PRE_ORDER_STATUSES and shipment in SHIPPING_SHIPMENT_STATUSES:
            status = shipment if shipment in TERMINAL_STATUSES else ORDERED

    if not status:
        return data
    if status != raw_status:
        data["status"] = status
    auto_moved = status != normalize_request_status(raw_status)
    code_changed = status != prev_status or status != normalize_status_code(raw_status)
    if status in label_map and (
        auto_moved
        or (code_changed and _is_generated_status_text(data.get("status_text"), label_map))
    ):
        data["status_text"] = label_map[status]
    return data


def validate_request_status(status: Any, config: Mapping[str, Any]) -> Optional[str]:
    """Return an error message when ``status`` is not in the tenant's list, else None."""
    code = normalize_request_status(status)
    if not code:
        return None
    if code in allowed_status_values(config):
        return None
    allowed = ", ".join(s["value"] for s in config.get("statuses") or [])
    return f'Unknown status "{code}". Allowed: {allowed}.'
