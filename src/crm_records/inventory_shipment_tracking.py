"""
Normalize inventory_request shipment tracking fields.

Ops paste a tracking URL or AWB from the vendor site; we split into
tracking_link / tracking_number and stamp tracking_updated_at when values change.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

SHIPMENT_STATUSES = frozenset(
    {
        "NOT_SHIPPED",
        "ORDERED",
        "IN_TRANSIT",
        "OUT_FOR_DELIVERY",
        "DELIVERED",
        "EXCEPTION",
    }
)
DEFAULT_SHIPMENT_STATUS = "NOT_SHIPPED"

_TRACKING_KEYS = (
    "tracking_number",
    "tracking_link",
    "courier_name",
    "shipment_status",
    "eta",
)


def _looks_like_url(value: str) -> bool:
    v = (value or "").strip()
    if not v:
        return False
    lower = v.lower()
    return lower.startswith("http://") or lower.startswith("https://") or lower.startswith("www.")


def _ensure_http_url(value: str) -> str:
    v = (value or "").strip()
    if v.lower().startswith(("http://", "https://")):
        return v
    if v.lower().startswith("www."):
        return f"https://{v}"
    return v


def extract_tracking_number_from_url(url: str) -> Optional[str]:
    try:
        parsed = urlparse(_ensure_http_url(url))
    except Exception:
        return None
    host = (parsed.hostname or "").lower()
    params = parse_qs(parsed.query or "")
    path = parsed.path or ""

    param_keys = (
        "waybill",
        "awb",
        "tracking_number",
        "trackingNumber",
        "tracking_id",
        "trackingId",
        "trackId",
        "shipment_id",
        "cnno",
        "ref",
        "id",
        "packageId",
        "orderId",
        "shipmentId",
    )
    for key in param_keys:
        vals = params.get(key) or []
        if vals and str(vals[0]).strip() and len(str(vals[0]).strip()) >= 5:
            return str(vals[0]).strip()

    parts = [p for p in path.split("/") if p]
    last = parts[-1] if parts else ""

    if "aftership." in host and last and len(last) >= 5:
        return last

    if any(x in host for x in ("delhivery.", "shiprocket.", "bluedart.")) and last:
        if len(last) >= 6 and last.lower() not in {"track", "tracking", "shipment", "order"}:
            return last

    return None


def normalize_tracking_paste(raw: Any) -> Dict[str, Optional[str]]:
    value = str(raw or "").strip()
    if not value:
        return {"tracking_link": None, "tracking_number": None}
    if _looks_like_url(value):
        link = _ensure_http_url(value)
        return {
            "tracking_link": link,
            "tracking_number": extract_tracking_number_from_url(link),
        }
    return {"tracking_link": None, "tracking_number": value}


def _norm_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _norm_shipment_status(value: Any) -> str:
    s = str(value or "").strip().upper().replace(" ", "_")
    if s in SHIPMENT_STATUSES:
        return s
    return DEFAULT_SHIPMENT_STATUS


def apply_shipment_tracking_normalization(
    data: Dict[str, Any],
    *,
    previous: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Normalize tracking fields on inventory_request data in-place and return data.

    - If tracking_link looks like a URL and tracking_number is empty, extract AWB.
    - If a single paste-style value was stored only in tracking_link or tracking_number,
      split via normalize_tracking_paste when the other is empty.
    - Set tracking_updated_at when any tracked field changes vs previous.
    """
    if not isinstance(data, dict):
        return data

    link = _norm_str(data.get("tracking_link"))
    number = _norm_str(data.get("tracking_number"))

    # Prefer explicit fields; fill gaps from paste heuristics.
    if link and _looks_like_url(link) and not number:
        extracted = extract_tracking_number_from_url(link)
        if extracted:
            data["tracking_number"] = extracted
        data["tracking_link"] = _ensure_http_url(link)
    elif number and _looks_like_url(number) and not link:
        split = normalize_tracking_paste(number)
        data["tracking_link"] = split["tracking_link"]
        data["tracking_number"] = split["tracking_number"]
    elif link and not _looks_like_url(link) and not number:
        # Mis-pasted AWB into link field
        data["tracking_number"] = link
        data["tracking_link"] = None
    else:
        if link:
            data["tracking_link"] = _ensure_http_url(link) if _looks_like_url(link) else link
        if number is not None:
            data["tracking_number"] = number

    if "shipment_status" in data or data.get("shipment_status") is None:
        # Only normalize when key present or we are initializing
        if "shipment_status" in data:
            data["shipment_status"] = _norm_shipment_status(data.get("shipment_status"))
        elif any(data.get(k) for k in ("tracking_number", "tracking_link", "courier_name")):
            data["shipment_status"] = DEFAULT_SHIPMENT_STATUS

    courier = _norm_str(data.get("courier_name"))
    if "courier_name" in data:
        data["courier_name"] = courier

    eta = _norm_str(data.get("eta"))
    if "eta" in data:
        data["eta"] = eta

    prev = previous if isinstance(previous, dict) else {}
    changed = False
    for key in _TRACKING_KEYS:
        if key not in data:
            continue
        new_v = data.get(key)
        old_v = prev.get(key)
        if _norm_str(new_v) != _norm_str(old_v) and not (
            key == "shipment_status"
            and _norm_shipment_status(new_v) == _norm_shipment_status(old_v)
        ):
            # Compare shipment_status normalized
            if key == "shipment_status":
                if _norm_shipment_status(new_v) != _norm_shipment_status(old_v or DEFAULT_SHIPMENT_STATUS):
                    changed = True
            else:
                changed = True

    if changed:
        data["tracking_updated_at"] = datetime.now(timezone.utc).isoformat()

    return data
