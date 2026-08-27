"""
Parse shipment / courier emails for tracking fields and record match keys.
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any, Dict, List, Optional
from uuid import UUID

from crm_records.inventory_shipment_tracking import (
    extract_tracking_number_from_url,
    normalize_tracking_paste,
)

_UUID_RE = re.compile(
    r"\b([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\b"
)
_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)

_AWB_PATTERNS = (
    r"(?:AWB|waybill|tracking\s*(?:no|number|id|#)?|consignment\s*(?:no|number)?)\s*[:#]?\s*([A-Za-z0-9\-]{8,})",
    r"tracking\?awb=([A-Za-z0-9]{8,})",
    r"[?&](?:awb|waybill|tracking_number|trackingNumber)=([A-Za-z0-9]{8,})",
    r"\bAWB\s+(\d{8,})\b",
)

_COURIER_RE = re.compile(
    r"\b(Blue\s*Dart|Delhivery|DTDC|FedEx|DHL|Shiprocket|India\s*Post|Ecom\s*Express|Amazon)\b",
    re.I,
)

_ORDER_KEY_PATTERNS = (
    ("po_number", r"(?:PO|P\.O\.|purchase\s*order)\s*(?:no|number|#)?\s*[:#]?\s*([A-Za-z0-9\-/]{4,})"),
    ("order_number", r"(?:order|ord)\s*(?:no|number|#|id)?\s*[:#]?\s*([A-Za-z0-9\-/]{4,})"),
    ("sales_order_number", r"(?:sales\s*order|SO)\s*(?:no|number|#)?\s*[:#]?\s*([A-Za-z0-9\-/]{4,})"),
    ("vendor_order_id", r"(?:vendor\s*order|merchant\s*order)\s*(?:no|number|#|id)?\s*[:#]?\s*([A-Za-z0-9\-/]{4,})"),
)

_SHIPMENT_HINT_RE = re.compile(
    r"\b(shipped|shipping|dispatched|in\s*transit|out\s*for\s*delivery|"
    r"delivered|tracking|awb|waybill|courier|consignment)\b",
    re.I,
)

_ETA_RE = re.compile(
    r"(?:ETA|expected\s*(?:delivery|by)|delivery\s*(?:by|date)|arriving\s*(?:by|on))"
    r"\s*[:\-]?\s*(\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    re.I,
)


def html_to_text(html: str) -> str:
    text = unescape(html or "")
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _looks_like_awb(candidate: str) -> bool:
    c = (candidate or "").strip()
    if len(c) < 8 or len(c) > 40:
        return False
    if c.lower() in {"tracking", "shipment", "delivered", "number", "consignment"}:
        return False
    alnum = sum(ch.isalnum() for ch in c)
    return alnum >= 6


def extract_tracking_from_text(text: str) -> Dict[str, Optional[str]]:
    """Return tracking_number, tracking_link, courier_name, eta from free text."""
    blob = text or ""
    tracking_link: Optional[str] = None
    tracking_number: Optional[str] = None

    for url in _URL_RE.findall(blob):
        clean = url.rstrip(").,;]>\"'")
        lower = clean.lower()
        if any(
            h in lower
            for h in (
                "track",
                "aftership",
                "delhivery",
                "bluedart",
                "fedex",
                "dhl",
                "shiprocket",
                "dtdc",
                "indiapost",
            )
        ):
            tracking_link = clean
            extracted = extract_tracking_number_from_url(clean)
            if extracted:
                tracking_number = extracted
            break

    if not tracking_number:
        for pat in _AWB_PATTERNS:
            m = re.search(pat, blob, re.I)
            if m and _looks_like_awb(m.group(1)):
                tracking_number = m.group(1).strip()
                break

    if tracking_number and not tracking_link:
        paste = normalize_tracking_paste(tracking_number)
        if paste.get("tracking_link"):
            tracking_link = paste["tracking_link"]
            tracking_number = paste.get("tracking_number") or tracking_number

    courier = None
    m = _COURIER_RE.search(blob)
    if m:
        courier = re.sub(r"\s+", " ", m.group(1)).strip()

    eta = None
    em = _ETA_RE.search(blob)
    if em:
        eta = em.group(1).strip()

    return {
        "tracking_number": tracking_number,
        "tracking_link": tracking_link,
        "courier_name": courier,
        "eta": eta,
    }


def extract_match_keys(text: str) -> Dict[str, Any]:
    """Keys used to find an inventory_request / unmannd_request."""
    blob = text or ""
    record_ids: List[str] = []
    for m in _UUID_RE.finditer(blob):
        try:
            UUID(m.group(1))
            record_ids.append(m.group(1).lower())
        except ValueError:
            continue

    keys: Dict[str, Any] = {"record_ids": list(dict.fromkeys(record_ids))}
    for field, pat in _ORDER_KEY_PATTERNS:
        m = re.search(pat, blob, re.I)
        if m:
            keys[field] = m.group(1).strip()
    return keys


def looks_like_shipment_email(subject: str, body: str) -> bool:
    blob = f"{subject or ''}\n{body or ''}"
    if _SHIPMENT_HINT_RE.search(blob):
        return True
    parsed = extract_tracking_from_text(blob)
    return bool(parsed.get("tracking_number") or parsed.get("tracking_link"))


def parse_shipment_email(*, subject: str, html_or_text: str) -> Dict[str, Any]:
    text = html_to_text(html_or_text) if "<" in (html_or_text or "") else (html_or_text or "")
    combined = f"{subject or ''}\n{text}"
    tracking = extract_tracking_from_text(combined)
    match_keys = extract_match_keys(combined)
    return {
        "is_shipment": looks_like_shipment_email(subject, text),
        "subject": subject or "",
        "tracking_number": tracking.get("tracking_number"),
        "tracking_link": tracking.get("tracking_link"),
        "courier_name": tracking.get("courier_name"),
        "eta": tracking.get("eta"),
        "match_keys": match_keys,
    }
