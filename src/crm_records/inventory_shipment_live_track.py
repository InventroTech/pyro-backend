"""
Live shipment status lookup for inventory_request tracking.

Ops paste a tracking number and/or tracking link; we resolve the current
carrier state into the canonical pipeline statuses:

  ORDERED → IN_TRANSIT → OUT_FOR_DELIVERY → DELIVERED  (+ EXCEPTION)

Sources (tried in smart order per AWB / courier hint):
  - AfterShip when AFTERSHIP_API_KEY is set (FedEx / DHL / BlueDart / Delhivery / …)
  - BlueDart TrackDart page / Delhivery API (no key; only when courier/link is clearly them,
    or as fallback after AfterShip for bare AWBs)
  - Vendor order-track pages (e.g. genxbattery.com/track?…)
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import ipaddress
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from .inventory_shipment_tracking import (
    SHIPMENT_STATUSES,
    extract_tracking_number_from_url,
    normalize_tracking_paste,
)

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT = 12

# Host suffixes we are allowed to fetch (SSRF allowlist). Keys are courier ids.
_ALLOWED_TRACK_HOST_SUFFIXES: Dict[str, Tuple[str, ...]] = {
    "delhivery": ("delhivery.com", "dlv-api.delhivery.com", "track.delhivery.com"),
    "shiprocket": ("shiprocket.co", "shiprocket.in", "sr-track.com"),
    "bluedart": ("bluedart.com",),
    "aftership": ("aftership.com", "aftership.io"),
    "indiapost": ("indiapost.gov.in", "www.indiapost.gov.in"),
    "dtdc": ("dtdc.com", "dtdc.in"),
    "fedex": ("fedex.com",),
    "dhl": ("dhl.com", "dhl.in"),
}

# Ordered from most specific / terminal first for keyword mapping.
_STATUS_KEYWORD_RULES: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    (
        "EXCEPTION",
        (
            "exception",
            "undelivered",
            "rto",
            "returned to origin",
            "failed delivery",
            "delivery failed",
            "cancelled",
            "canceled",
            "lost",
            "damaged",
            "held",
            "on hold",
        ),
    ),
    (
        "DELIVERED",
        (
            "successfully delivered",
            "delivery completed",
            "consignee received",
            "shipment delivered",
            "delivered",
        ),
    ),
    (
        "OUT_FOR_DELIVERY",
        (
            "out for delivery",
            "ofo",
            "ofd",
            "out_for_delivery",
            "dispatched for delivery",
            "with delivery executive",
            "out for del",
        ),
    ),
    (
        "IN_TRANSIT",
        (
            "in transit",
            "in_transit",
            "shipment in transit",
            "reached at",
            "departed",
            "arrived at",
            "in scan",
            "bagged",
            "manifested",
            "pending",
            "connected",
            "received at",
            "processed at",
        ),
    ),
    (
        "ORDERED",
        (
            "order placed",
            "info received",
            "label created",
            "pickup scheduled",
            "pickup pending",
            "picked up",
            "registered",
            "booked",
            "manifest created",
            "ready for pickup",
            "soft data uploaded",
            "shipment created",
            "awb assigned",
            "not picked",
            "pickup",
        ),
    ),
)


class ShipmentTrackError(Exception):
    """Validation / client error for shipment tracking."""


def _norm_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def map_status_text(raw: Any) -> Optional[str]:
    """Map free-text carrier status into a canonical shipment_status."""
    text = _norm_str(raw)
    if not text:
        return None
    upper = text.upper().replace(" ", "_").replace("-", "_")
    if upper in SHIPMENT_STATUSES:
        return upper
    lower = text.lower()
    for status, keywords in _STATUS_KEYWORD_RULES:
        for kw in keywords:
            if kw in lower:
                return status
    return None


def _detect_courier_from_host(host: str) -> Optional[str]:
    host = (host or "").lower().strip(".")
    for courier, suffixes in _ALLOWED_TRACK_HOST_SUFFIXES.items():
        for suffix in suffixes:
            if host == suffix or host.endswith("." + suffix):
                return courier
    return None


_AMAZON_ORDER_ID_RE = re.compile(r"^\d{3}-\d{7}-\d{7}$")
_AMAZON_TBA_RE = re.compile(r"^TBA[0-9A-Z]{8,}$", re.I)


def looks_like_amazon_tracking_id(value: Optional[str]) -> bool:
    """True for Amazon order ids (402-…) or Amazon Logistics TBA package ids."""
    v = _norm_str(value)
    if not v:
        return False
    if _AMAZON_ORDER_ID_RE.match(v):
        return True
    if _AMAZON_TBA_RE.match(v):
        return True
    if v.upper().startswith(("TBA", "AMZL")):
        return True
    return False


def detect_courier(
    *,
    tracking_link: Optional[str] = None,
    courier_name: Optional[str] = None,
    tracking_number: Optional[str] = None,
) -> Optional[str]:
    name = (_norm_str(courier_name) or "").lower().replace(" ", "").replace("-", "").replace("_", "")
    if name:
        if "fedex" in name or name == "fx":
            return "fedex"
        if name == "dhl" or name.startswith("dhl"):
            return "dhl"
        if "blue" in name and "dart" in name:
            return "bluedart"
        if "delhivery" in name:
            return "delhivery"
        if "amazon" in name or name in {"amzn", "amzl"}:
            return "amazon"
        if "post" in name:
            return "indiapost"
        for courier in _ALLOWED_TRACK_HOST_SUFFIXES:
            if courier in name or name in courier:
                return courier

    if looks_like_amazon_tracking_id(tracking_number):
        return "amazon"

    link = _norm_str(tracking_link)
    if link:
        try:
            host = (urllib.parse.urlparse(_ensure_https(link)).hostname or "").lower()
        except Exception:
            host = ""
        if "amazon." in host or host.endswith("amzn.in"):
            return "amazon"
        detected = _detect_courier_from_host(host)
        if detected:
            return detected
    return None



def _normalize_courier_slug(courier: Optional[str]) -> Optional[str]:
    raw = _norm_str(courier)
    if not raw:
        return None
    c = raw.lower().replace(" ", "-").replace("_", "-")
    if "amazon" in c or c in {"amzn", "amzl"}:
        return "amazon"
    if "fedex" in c:
        return "fedex"
    if c == "dhl" or c.startswith("dhl-") or c.startswith("dhl"):
        if "germany" in c:
            return "dhl-germany"
        if "global" in c or "ecommerce" in c or "packet" in c:
            return "dhl-global-mail"
        if "express" in c:
            return "dhl-express"
        return "dhl"
    if "bluedart" in c or "blue-dart" in c:
        return "bluedart"
    if "delhivery" in c:
        return "delhivery"
    if "dtdc" in c:
        return "dtdc"
    if "shiprocket" in c:
        return "shiprocket"
    if "india-post" in c or "indiapost" in c or c in {"india-post", "post"}:
        return "india-post"
    if c in {"aftership", "vendor", "fallback", "link-scrape", "link_scrape"}:
        return None
    return c


def _courier_display_name(courier: Optional[str]) -> Optional[str]:
    """Map AfterShip slugs to UI courier labels (Amazon Order → Amazon)."""
    slug = _normalize_courier_slug(courier) or _norm_str(courier)
    if not slug:
        return None
    c = slug.lower()
    if c.startswith("amazon"):
        return "Amazon"
    if c.startswith("fedex"):
        return "FedEx"
    if c == "dhl" or c.startswith("dhl"):
        return "DHL"
    if "bluedart" in c:
        return "BlueDart"
    if "delhivery" in c:
        return "Delhivery"
    if c == "dtdc":
        return "DTDC"
    if "shiprocket" in c:
        return "Shiprocket"
    if "india-post" in c or "indiapost" in c:
        return "India Post"
    return slug.replace("-", " ").title()


def _public_tracking_link(awb: Optional[str], courier: Optional[str] = None) -> Optional[str]:
    """
    Build a public track URL from AWB + courier when the user did not paste a link.

    Prefers official carrier pages; falls back to AfterShip (with slug when known,
    or bare number for auto-detect).
    """
    number = _norm_str(awb)
    if not number:
        return None
    safe = urllib.parse.quote(number, safe="")
    slug = _normalize_courier_slug(courier)
    if slug == "amazon":
        return f"https://www.aftership.com/track/amazon/{safe}"
    if slug == "fedex":
        return f"https://www.fedex.com/fedextrack/?trknbr={safe}"
    if slug and slug.startswith("dhl"):
        return f"https://www.dhl.com/en/express/tracking.html?AWB={safe}&brand=DHL"
    if slug == "delhivery":
        return f"https://www.delhivery.com/track/package/?waybill={safe}"
    if slug == "bluedart":
        return f"https://www.aftership.com/track/bluedart/{safe}"
    if slug == "dtdc":
        return f"https://www.aftership.com/track/dtdc/{safe}"
    if slug == "shiprocket":
        return f"https://www.aftership.com/track/shiprocket/{safe}"
    if slug == "india-post":
        return f"https://www.aftership.com/track/india-post/{safe}"
    if slug:
        return f"https://www.aftership.com/track/{slug}/{safe}"
    # No courier yet — AfterShip landing page auto-detects for many carriers.
    return f"https://www.aftership.com/track/{safe}"


def _resolve_tracking_link(
    *,
    awb: Optional[str],
    courier: Optional[str] = None,
    user_link: Optional[str] = None,
    carrier_link: Optional[str] = None,
) -> Optional[str]:
    """
    Choose the best tracking URL.

    Priority:
      1) User-pasted link
      2) Carrier official link from AfterShip (courier_tracking_link)
      3) Synthesized public URL from AWB + courier
    """
    pasted = _norm_str(user_link)
    if pasted:
        return pasted
    official = _norm_str(carrier_link)
    if official:
        # Prefer real carrier hosts over a generic AfterShip page when both exist.
        host = ""
        path = ""
        try:
            parsed = urllib.parse.urlparse(official)
            host = (parsed.hostname or "").lower()
            path = (parsed.path or "").lower()
        except Exception:
            host = ""
            path = ""
        # AfterShip sometimes returns a bare Amazon progress-tracker URL with no id — useless.
        if "amazon." in host and "progress-tracker" in path and "packageid" not in official.lower() and "trackingid" not in official.lower():
            official = None
        elif host and "aftership." not in host and official:
            return official
    synthesized = _public_tracking_link(awb, courier)
    if synthesized:
        return synthesized
    return official


def _ensure_https(url: str) -> str:
    v = (url or "").strip()
    if v.lower().startswith("www."):
        return "https://" + v
    if v.lower().startswith("http://"):
        return "https://" + v[len("http://") :]
    return v


def _looks_like_tracking_page(parsed: urllib.parse.ParseResult) -> bool:
    """Vendor order-track pages (e.g. genxbattery.com/track?order=...)."""
    path = (parsed.path or "").lower()
    query = (parsed.query or "").lower()
    if any(token in path for token in ("/track", "tracking", "/shipment", "/awb", "/consignment")):
        return True
    if any(
        key in query
        for key in (
            "order=",
            "awb=",
            "waybill=",
            "tracking",
            "token=",
            "cnno=",
            "shipment",
        )
    ):
        return True
    return False


def _host_resolves_to_public_ip(host: str) -> bool:
    """Reject hosts that resolve to private / loopback / link-local addresses (SSRF)."""
    try:
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except socket.gaierror:
        # DNS unavailable in this environment — hostname already passed non-IP checks.
        return True
    if not infos:
        return True
    for info in infos:
        ip_str = info[4][0]
        try:
            addr = ipaddress.ip_address(ip_str)
        except ValueError:
            return False
        if (
            addr.is_private
            or addr.is_loopback
            or addr.is_link_local
            or addr.is_reserved
            or addr.is_multicast
            or addr.is_unspecified
        ):
            return False
    return True


def _assert_safe_track_url(url: str, *, allow_vendor_pages: bool = True) -> Tuple[str, str]:
    """
    Allow https tracking URLs on known carriers, or vendor order-track pages.

    Returns (safe_url, courier_or_vendor_id). Rebuilds URL with validated host.
    """
    raw = _norm_str(url)
    if not raw:
        raise ShipmentTrackError("Tracking link is required.")
    raw = _ensure_https(raw)
    try:
        parsed = urllib.parse.urlparse(raw)
    except Exception as exc:
        raise ShipmentTrackError("Invalid tracking link.") from exc
    if parsed.scheme.lower() != "https":
        raise ShipmentTrackError("Only https tracking links are allowed.")
    if parsed.username or parsed.password:
        raise ShipmentTrackError("URL credentials are not allowed.")
    host = (parsed.hostname or "").lower().strip(".")
    if not host:
        raise ShipmentTrackError("Tracking link host is required.")
    if host in {"localhost", "127.0.0.1", "::1"} or host.endswith(".localhost"):
        raise ShipmentTrackError("Local hosts are not allowed.")
    if re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}", host) or ":" in host:
        raise ShipmentTrackError("IP address hosts are not allowed.")
    if not _host_resolves_to_public_ip(host):
        raise ShipmentTrackError("Tracking link host is not allowed.")

    courier = _detect_courier_from_host(host)
    if not courier:
        if not allow_vendor_pages or not _looks_like_tracking_page(parsed):
            raise ShipmentTrackError("Tracking link host is not a supported carrier.")
        courier = "vendor"

    path = parsed.path or "/"
    if path.startswith("//") or "\\" in path or "@" in path:
        raise ShipmentTrackError("Invalid tracking link path.")
    path = urllib.parse.quote(urllib.parse.unquote(path), safe="/-._~")
    query = urllib.parse.quote(urllib.parse.unquote(parsed.query or ""), safe="=&%+-._~")
    safe = urllib.parse.urlunparse(("https", host, path, "", query, ""))
    return safe, courier


def _extract_awb_from_text(text: str) -> Optional[str]:
    patterns = (
        r"(?:AWB|waybill|tracking\s*(?:no|number|#)?)\s*[:#]?\s*([A-Za-z0-9]{8,})",
        r"tracking\?awb=([A-Za-z0-9]{8,})",
        r"awb=([A-Za-z0-9]{8,})",
        r"\bAWB\s+(\d{8,})\b",
    )
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            return m.group(1).strip()
    return None


def _extract_courier_from_text(text: str) -> Optional[str]:
    m = re.search(
        r"\b(Blue\s*Dart|Delhivery|DTDC|FedEx|DHL|Shiprocket|India\s*Post|Ecom\s*Express)\b",
        text,
        re.I,
    )
    if not m:
        return None
    name = re.sub(r"\s+", " ", m.group(1)).strip()
    return name


def _extract_order_id_from_url(url: str) -> Optional[str]:
    try:
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query or "")
        for key in ("order", "order_id", "orderId", "orderID", "orderno", "packageId"):
            vals = qs.get(key) or []
            if vals and str(vals[0]).strip():
                return str(vals[0]).strip()
    except Exception:
        return None
    return None


def _parse_vendor_tracking_html(html: str) -> Dict[str, Optional[str]]:
    """
    Pull current pipeline status / AWB / courier from vendor track pages
    (e.g. GenX Mission Tracking) without treating the whole progress legend as current.
    """
    cleaned = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html or "")
    cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)

    status: Optional[str] = None
    status_detail: Optional[str] = None

    # Prefer explicit "Current Status" value.
    m = re.search(
        r"Current Status\s*[:\-]?\s*(Delivered|Out for Delivery|In Transit|Picked Up|Registered|Exception|Undelivered|Shipped)",
        cleaned,
        re.I,
    )
    if m:
        status_detail = m.group(1).strip()
        status = map_status_text(status_detail)

    if not status:
        m = re.search(
            r"SHIPMENT\s+(DELIVERED|OUT FOR DELIVERY|ARRIVED|BOOKED|IN TRANSIT|UNDELIVERED)",
            html or "",
            re.I,
        )
        if m:
            status_detail = f"SHIPMENT {m.group(1).upper()}"
            status = map_status_text(status_detail)

    if not status:
        # Hero / title style single status near "Mission Tracking" / order header.
        m = re.search(
            r"(?:Mission Tracking|Track Your Shipment).{0,120}?\b(Delivered|Out for Delivery|In Transit|Picked Up)\b",
            cleaned,
            re.I,
        )
        if m:
            status_detail = m.group(1).strip()
            status = map_status_text(status_detail)

    if not status:
        # Fallback keyword windows — still prefer terminal states.
        windows: List[str] = []
        lower = cleaned.lower()
        for _, kws in _STATUS_KEYWORD_RULES:
            for kw in kws:
                idx = lower.find(kw)
                if idx >= 0:
                    windows.append(cleaned[max(0, idx - 20) : idx + len(kw) + 40])
        status = _pick_best_status(windows)
        if status:
            status_detail = next((w for w in windows if map_status_text(w) == status), None)

    awb = _extract_awb_from_text(cleaned) or _extract_awb_from_text(html or "")
    courier_name = _extract_courier_from_text(cleaned)

    eta = None
    m = re.search(
        r"(?:Delivered On|Last Updated|ETA|Expected)\s*[:\-]?\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})",
        cleaned,
        re.I,
    )
    if m:
        eta = m.group(1).strip()

    return {
        "shipment_status": status,
        "status_detail": status_detail,
        "tracking_number": awb,
        "courier_name": courier_name,
        "eta": eta,
    }


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/html, */*;q=0.8",
            "Accept-Language": "en-IN,en;q=0.9",
            "Accept-Encoding": "identity",
        }
    )
    return s


def _walk_strings(obj: Any, *, limit: int = 80) -> List[str]:
    out: List[str] = []

    def walk(node: Any) -> None:
        if len(out) >= limit:
            return
        if isinstance(node, dict):
            for k, v in node.items():
                lk = str(k).lower()
                if isinstance(v, (str, int, float)) and any(
                    x in lk
                    for x in (
                        "status",
                        "scan",
                        "remark",
                        "instruction",
                        "message",
                        "state",
                        "tag",
                        "checkpoint",
                        "description",
                    )
                ):
                    s = str(v).strip()
                    if s:
                        out.append(s)
                walk(v)
        elif isinstance(node, list):
            for item in node[:40]:
                walk(item)
        elif isinstance(node, str) and node.strip():
            # skip huge blobs
            if len(node) < 240:
                out.append(node.strip())

    walk(obj)
    return out


def _pick_best_status(candidates: List[str]) -> Optional[str]:
    mapped = [m for m in (map_status_text(c) for c in candidates) if m]
    if not mapped:
        return None
    # Prefer terminal / later pipeline states when multiple appear.
    priority = ["EXCEPTION", "DELIVERED", "OUT_FOR_DELIVERY", "IN_TRANSIT", "ORDERED", "NOT_SHIPPED"]
    for p in priority:
        if p in mapped:
            return p
    return mapped[0]


def _extract_eta(candidates: List[str], payload: Any = None) -> Optional[str]:
    # ISO / date-like fields in JSON
    if isinstance(payload, dict):
        for key in (
            "promised_delivery_date",
            "expected_delivery_date",
            "edd",
            "eta",
            "estimated_delivery",
            "estimated_date",
        ):
            # shallow + one-level nest
            if key in payload and _norm_str(payload.get(key)):
                return str(payload.get(key))[:32]
            for v in payload.values():
                if isinstance(v, dict) and key in v and _norm_str(v.get(key)):
                    return str(v.get(key))[:32]
                if isinstance(v, list):
                    for item in v[:5]:
                        if isinstance(item, dict) and key in item and _norm_str(item.get(key)):
                            return str(item.get(key))[:32]

    date_re = re.compile(
        r"\b(?:\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|"
        r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2}(?:,\s*\d{4})?)\b",
        re.I,
    )
    for c in candidates:
        if "deliver" in c.lower() or "eta" in c.lower() or "edd" in c.lower():
            m = date_re.search(c)
            if m:
                return m.group(0)
    return None


def _track_delhivery(awb: str) -> Dict[str, Any]:
    """Call Delhivery public unified-tracking with browser-like Origin."""
    # Server-constructed URL — AWB is query-encoded, host is fixed.
    safe_awb = urllib.parse.quote(str(awb).strip(), safe="")
    url = f"https://dlv-api.delhivery.com/v3/unified-tracking?wbn={safe_awb}"
    sess = _session()
    sess.headers.update(
        {
            "Origin": "https://www.delhivery.com",
            "Referer": "https://www.delhivery.com/",
            "Accept": "application/json, text/plain, */*",
        }
    )
    try:
        resp = sess.get(url, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as exc:
        logger.warning("delhivery track failed awb=%s err=%s", awb, exc)
        return {
            "ok": False,
            "courier": "delhivery",
            "error": "Could not reach Delhivery tracking.",
        }

    if resp.status_code >= 400:
        return {
            "ok": False,
            "courier": "delhivery",
            "error": "Delhivery tracking returned an error.",
        }

    try:
        payload = resp.json()
    except Exception:
        return {
            "ok": False,
            "courier": "delhivery",
            "error": "Delhivery returned a non-JSON response.",
        }

    data = payload.get("data") if isinstance(payload, dict) else None
    if not data:
        msg = ""
        if isinstance(payload, dict):
            msg = str(payload.get("message") or "")
        return {
            "ok": False,
            "courier": "delhivery",
            "error": msg or "No tracking data found for this AWB.",
            "raw_message": msg or None,
        }

    candidates = _walk_strings(payload)
    status = _pick_best_status(candidates)
    eta = _extract_eta(candidates, payload if isinstance(payload, dict) else None)
    status_detail = next((c for c in candidates if map_status_text(c) == status), None)

    return {
        "ok": bool(status),
        "courier": "delhivery",
        "courier_name": "Delhivery",
        "shipment_status": status or "ORDERED",
        "status_detail": status_detail,
        "eta": eta,
        "tracking_number": awb,
        "tracking_link": f"https://www.delhivery.com/track/package/?waybill={safe_awb}",
        "method": "delhivery_api",
        "tracked_at": datetime.now(timezone.utc).isoformat(),
        "error": None if status else "Could not determine delivery status from Delhivery.",
    }


def _bluedart_page_looks_like_error_chrome(cleaned: str) -> bool:
    """
    BlueDart error / empty-result pages still contain words like UNDELIVERED
    (e.g. "available only for UNDELIVERED Waybills") which must not be treated
    as a real shipment status — that false positive blocked AfterShip/FedEx/DHL.
    """
    lower = (cleaned or "").lower()
    return any(
        phrase in lower
        for phrase in (
            "available only for undelivered waybills",
            "no records found",
            "invalid waybill",
            "please enter a valid",
            "waybill number does not exist",
        )
    )


def _track_bluedart(awb: str) -> Dict[str, Any]:
    """Fetch BlueDart public TrackDart result page (server-built URL)."""
    safe_awb = urllib.parse.quote(str(awb).strip(), safe="")
    url = (
        "https://www.bluedart.com/web/guest/trackdartresultthirdparty"
        f"?trackFor=0&trackNo={safe_awb}"
    )
    sess = _session()
    sess.headers.update(
        {
            "Referer": "https://www.bluedart.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    try:
        resp = sess.get(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
    except requests.RequestException as exc:
        logger.warning("bluedart track failed awb=%s err=%s", awb, exc)
        return {
            "ok": False,
            "courier": "bluedart",
            "error": "Could not reach BlueDart tracking.",
            "method": "bluedart_page",
        }

    if resp.status_code >= 400:
        return {
            "ok": False,
            "courier": "bluedart",
            "error": "BlueDart tracking returned an error.",
            "method": "bluedart_page",
        }

    text = resp.text or ""
    cleaned = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
    cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)

    # Prefer the explicit status row on TrackDart pages.
    status = None
    status_detail = None
    explicit_status = False
    m = re.search(
        r"Status\s+(Shipment\s+Delivered|Shipment\s+Out\s+for\s+Delivery|"
        r"In\s+Transit|Shipment\s+Undelivered|Delivered|Out\s+for\s+Delivery)",
        cleaned,
        re.I,
    )
    if m:
        status_detail = m.group(1).strip()
        status = map_status_text(status_detail)
        explicit_status = bool(status)

    # Error chrome (wrong AWB / not BlueDart) — never treat as a successful track.
    if not explicit_status and _bluedart_page_looks_like_error_chrome(cleaned):
        return {
            "ok": False,
            "courier": "bluedart",
            "error": "No BlueDart tracking data found for this AWB.",
            "method": "bluedart_page",
        }

    if not status:
        parsed = _parse_vendor_tracking_html(text)
        # Only accept strong SHIPMENT / Current Status hits — not keyword chrome.
        detail = (parsed.get("status_detail") or "")
        if detail and re.search(
            r"(?i)(shipment\s+(delivered|out for delivery|in transit|undelivered)|"
            r"current status)",
            detail,
        ):
            status = parsed.get("shipment_status")
            status_detail = detail

    if not status:
        # No usable BlueDart result for this AWB (wrong courier / not found).
        return {
            "ok": False,
            "courier": "bluedart",
            "error": "No BlueDart tracking data found for this AWB.",
            "method": "bluedart_page",
        }

    eta = None
    m = re.search(r"Date of Delivery\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})", cleaned, re.I)
    if m:
        eta = m.group(1).strip()

    return {
        "ok": True,
        "courier": "bluedart",
        "courier_name": "BlueDart",
        "shipment_status": status,
        "status_detail": status_detail,
        "eta": eta,
        "tracking_number": awb,
        "tracking_link": url,
        "method": "bluedart_page",
        "tracked_at": datetime.now(timezone.utc).isoformat(),
        "error": None,
    }


def _carrier_trackers_for_awb(
    awb: str, courier: Optional[str]
) -> List[Tuple[str, Any]]:
    """
    Ordered free scrapers for a bare AWB (BlueDart / Delhivery only).

    FedEx / DHL / most couriers are resolved via AfterShip — not native carrier APIs.
    """
    trackers = {
        "bluedart": _track_bluedart,
        "delhivery": _track_delhivery,
    }

    digits = awb.isdigit()
    length = len(awb)
    guessed: List[str] = []
    if courier in trackers:
        guessed.append(courier)
    if digits and length == 11:
        guessed.extend(["bluedart", "delhivery"])
    else:
        guessed.extend(["delhivery", "bluedart"])

    order: List[str] = []
    for name in guessed + ["bluedart", "delhivery"]:
        if name not in order and name in trackers:
            order.append(name)
    return [(name, trackers[name]) for name in order]


def _map_aftership_tag(tag: Any, detail: Any = None) -> Optional[str]:
    """Map AfterShip canonical tags; fall back to free-text mapping."""
    raw = _norm_str(tag) or ""
    # AfterShip tags are PascalCase; accept snake / spaced forms too.
    key = raw.replace(" ", "").replace("_", "").lower()
    mapping = {
        "pending": "ORDERED",
        "inforeceived": "ORDERED",
        "intransit": "IN_TRANSIT",
        "outfordelivery": "OUT_FOR_DELIVERY",
        "delivered": "DELIVERED",
        "availableforpickup": "OUT_FOR_DELIVERY",
        "attemptfail": "EXCEPTION",
        "exception": "EXCEPTION",
        "expired": "EXCEPTION",
    }
    if key in mapping:
        return mapping[key]
    return map_status_text(tag) or map_status_text(detail)


def _aftership_extract_tracking(payload: Any) -> Optional[Dict[str, Any]]:
    """Normalize AfterShip list/create/get-by-id envelopes to one tracking dict."""
    if not isinstance(payload, dict):
        return None
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    if not isinstance(data, dict):
        return None
    trackings = data.get("trackings")
    if isinstance(trackings, list) and trackings and isinstance(trackings[0], dict):
        return trackings[0]
    one = data.get("tracking")
    if isinstance(one, dict):
        return one
    # GET by id returns the tracking fields directly under data.
    if data.get("tracking_number") or data.get("tag") or data.get("id"):
        return data
    return None


def _aftership_status_from_tracking(t0: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Prefer real carrier checkpoints over a bare Pending tag.

    AfterShip often creates trackings as Pending with 0 checkpoints until the
    carrier sync runs (minutes to hours). Pending alone must not look like a
    final delivery status.
    """
    checkpoints = t0.get("checkpoints") if isinstance(t0.get("checkpoints"), list) else []
    tag = t0.get("tag") or ""
    detail = t0.get("subtag_message") or t0.get("subtag") or tag

    # Newest checkpoint first in AfterShip responses.
    if checkpoints:
        last = checkpoints[0] if isinstance(checkpoints[0], dict) else {}
        if not isinstance(last, dict) and len(checkpoints) > 0:
            last = checkpoints[-1] if isinstance(checkpoints[-1], dict) else {}
        # Prefer the latest non-empty checkpoint tag.
        for cp in checkpoints:
            if isinstance(cp, dict) and (cp.get("tag") or cp.get("message")):
                last = cp
                break
        # AfterShip lists oldest→newest; use the last element as latest.
        if checkpoints and isinstance(checkpoints[-1], dict):
            last = checkpoints[-1]
        cp_tag = last.get("tag") or ""
        cp_msg = last.get("message") or last.get("subtag_message") or detail
        status = _map_aftership_tag(cp_tag, cp_msg) or _map_aftership_tag(tag, detail)
        return status, _norm_str(cp_msg) or _norm_str(detail) or _norm_str(tag)

    status = _map_aftership_tag(tag, detail)
    return status, _norm_str(detail) or _norm_str(tag)


def _aftership_is_weak_pending(t0: Dict[str, Any]) -> bool:
    tag = (_norm_str(t0.get("tag")) or "").lower()
    checkpoints = t0.get("checkpoints") if isinstance(t0.get("checkpoints"), list) else []
    subtag = (_norm_str(t0.get("subtag")) or "").lower()
    # Pending with no scans — carrier has not reported yet (or stale Pending_005).
    if tag == "pending" and not checkpoints:
        return True
    if subtag.startswith("pending_") and not checkpoints:
        return True
    return False



def _join_place_parts(*parts: Any) -> Optional[str]:
    """Comma-join unique non-empty place fragments."""
    cleaned: List[str] = []
    for part in parts:
        s = _norm_str(part)
        if not s:
            continue
        if s not in cleaned:
            cleaned.append(s)
    return ", ".join(cleaned) if cleaned else None


def _empty_shipment_details() -> Dict[str, Any]:
    return {
        "origin": None,
        "destination": None,
        "current_location": None,
        "events": [],
    }


def _aftership_shipment_details(t0: Dict[str, Any]) -> Dict[str, Any]:
    """Origin / destination / scan timeline from an AfterShip tracking object."""
    origin = _norm_str(t0.get("origin_raw_location")) or _join_place_parts(
        t0.get("origin_city"),
        t0.get("origin_state"),
        t0.get("origin_postal_code"),
        t0.get("origin_country_region"),
    )
    destination = _norm_str(t0.get("destination_raw_location")) or _join_place_parts(
        t0.get("destination_city"),
        t0.get("destination_state"),
        t0.get("destination_postal_code"),
        t0.get("destination_country_region") or t0.get("courier_destination_country_region"),
    )
    events: List[Dict[str, Any]] = []
    checkpoints = t0.get("checkpoints") if isinstance(t0.get("checkpoints"), list) else []
    # AfterShip is oldest → newest; reverse for newest-first UI.
    for cp in reversed(checkpoints):
        if not isinstance(cp, dict):
            continue
        loc = _norm_str(cp.get("location")) or _join_place_parts(
            cp.get("city"),
            cp.get("state"),
            cp.get("zip"),
            cp.get("country_region_name") or cp.get("country_region"),
        )
        msg = _norm_str(cp.get("message") or cp.get("subtag_message") or cp.get("tag"))
        if not msg and not loc:
            continue
        events.append(
            {
                "time": _norm_str(cp.get("checkpoint_time") or cp.get("created_at")),
                "message": msg,
                "location": loc,
                "status": _map_aftership_tag(cp.get("tag"), msg),
            }
        )
    current_location = events[0].get("location") if events else None
    return {
        "origin": origin,
        "destination": destination,
        "current_location": current_location,
        "events": events,
    }



def _aftership_detect_slugs(
    awb: str,
    *,
    headers: Dict[str, str],
) -> List[str]:
    """
    Ask AfterShip which couriers match this tracking number.
    Returns slug list (best first). Empty on failure.
    """
    url = "https://api.aftership.com/tracking/2024-04/couriers/detect"
    try:
        resp = requests.post(
            url,
            json={"tracking_number": awb},
            headers=headers,
            timeout=DEFAULT_TIMEOUT,
        )
    except requests.RequestException as exc:
        logger.warning("aftership detect failed awb=%s err=%s", awb, exc)
        return []
    if resp.status_code >= 400:
        logger.info("aftership detect status=%s awb=%s", resp.status_code, awb)
        return []
    try:
        payload = resp.json()
    except Exception:
        return []
    data = payload.get("data") if isinstance(payload, dict) else None
    couriers = (data or {}).get("couriers") if isinstance(data, dict) else None
    if not isinstance(couriers, list):
        return []
    slugs: List[str] = []
    for c in couriers:
        if not isinstance(c, dict):
            continue
        slug = _norm_str(c.get("slug"))
        if slug and slug not in slugs:
            slugs.append(slug)
    logger.info("aftership detect awb=%s slugs=%s", awb, slugs[:8])
    return slugs


def _track_aftership_api(
    awb: str,
    courier: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    api_key = _norm_str(os.environ.get("AFTERSHIP_API_KEY"))
    if not api_key:
        return None

    headers = {
        "as-api-key": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    # Fixed AfterShip API host — not user controlled.
    # 2024-04+ create body is flat: {"tracking_number": "...", "slug": "..."}
    # (nested {"tracking": {...}} returns 4007 tracking_number is required).
    base = "https://api.aftership.com/tracking/2024-04/trackings"

    slug_hint = None
    if courier and courier not in {"aftership", "vendor"}:
        slug_hint = courier.replace("_", "-")

    # Prefer likely carriers for digit-length heuristics when courier unknown.
    length = len(awb)
    digits = awb.isdigit()
    amazonish = (courier == "amazon") or looks_like_amazon_tracking_id(awb)
    slug_candidates: List[Optional[str]] = []
    if slug_hint:
        slug_candidates.append(slug_hint)

    # Amazon ids must NOT fall through FedEx/DHL length heuristics (common mislabel).
    if amazonish or slug_hint in {"amazon", "amazon-order", "amazon-mcf"}:
        slug_candidates.extend(["amazon", "amazon-order", "amazon-mcf", None])
    elif slug_hint in {"fedex", "dhl", "dhl-express", "dhl-germany", "dhl-global-mail"}:
        related = [slug_hint, "fedex", "dhl", "dhl-express", "dhl-germany", None]
        slug_candidates.extend(related)
    elif slug_hint in {"bluedart", "delhivery", "dtdc", "shiprocket", "india-post"}:
        slug_candidates.extend([slug_hint, None])
    else:
        # Auto-detect FIRST. Length heuristics alone mislabel Amazon package ids
        # like 371022899423 as FedEx (both are often 12 digits).
        detected = _aftership_detect_slugs(awb, headers=headers)
        if detected:
            slug_candidates.extend(detected)
        slug_candidates.append(None)
        if digits and length == 12:
            slug_candidates.extend(["fedex", "dhl", "dhl-express"])
        elif digits and length in {10, 9}:
            slug_candidates.extend(["dhl", "dhl-express", "fedex"])
        elif digits and length == 11:
            slug_candidates.extend(["bluedart", "dhl", "fedex"])
        slug_candidates.extend(
            [
                "amazon",
                "amazon-order",
                "fedex",
                "dhl",
                "dhl-express",
                "dhl-germany",
                "dhl-global-mail",
                "bluedart",
                "delhivery",
                "dtdc",
                "india-post",
                "shiprocket",
            ]
        )

    # de-dupe
    seen = set()
    slugs: List[Optional[str]] = []
    for s in slug_candidates:
        key = s or ""
        if key in seen:
            continue
        seen.add(key)
        slugs.append(s)

    preferred_couriers = {
        c
        for c in (
            slug_hint,
            "amazon",
            "amazon-order",
            "amazon-mcf",
            "fedex",
            "dhl",
            "dhl-express",
            courier,
        )
        if c
    }

    last_err: Optional[Dict[str, Any]] = None

    def _err_rank(err: Dict[str, Any]) -> int:
        """Higher is better soft-failure to keep."""
        c = str(err.get("courier") or "").lower()
        detail = f"{err.get('status_detail') or ''} {err.get('error') or ''}".lower()
        score = 0
        if c.startswith("amazon"):
            score += 50
        elif c.startswith("fedex") or c.startswith("dhl"):
            score += 30
        elif c in preferred_couriers:
            score += 20
        if "wrong carrier" in detail:
            score -= 40
        if "pending_001" in detail or "no carrier scans" in detail:
            score += 5
        return score

    def _remember_err(err: Optional[Dict[str, Any]]) -> None:
        """Keep the best soft-failure — prefer Amazon Pending over Wrong-carrier FedEx/DHL."""
        nonlocal last_err
        if not err:
            return
        if not last_err:
            last_err = err
            return
        if _err_rank(err) > _err_rank(last_err):
            last_err = err

    def _result_from_tracking(t0: Dict[str, Any], *, allow_weak_pending: bool) -> Optional[Dict[str, Any]]:
        status, detail = _aftership_status_from_tracking(t0)
        slug = _norm_str(t0.get("slug")) or courier or "aftership"
        link = _resolve_tracking_link(
            awb=awb,
            courier=slug if slug != "aftership" else courier,
            carrier_link=_norm_str(t0.get("courier_tracking_link")),
        )
        eta = _norm_str(t0.get("expected_delivery") or t0.get("shipment_delivery_date"))

        if _aftership_is_weak_pending(t0) and not allow_weak_pending:
            subtag = _norm_str(t0.get("subtag")) or ""
            msg = (
                "AfterShip has this number but no carrier scans yet "
                f"({detail or 'Pending'}). Carrier updates usually arrive within a few hours — "
                "set Courier correctly (DHL / FedEx / BlueDart) and click Refresh later."
            )
            if subtag.upper().startswith("PENDING_005"):
                msg = (
                    "AfterShip reports no recent carrier updates for this number "
                    "(Pending_005). Check the tracking id and Courier (DHL / FedEx / BlueDart), "
                    "then try Refresh later."
                )
            logger.info(
                "aftership weak pending awb=%s slug=%s tag=%s subtag=%s detail=%s checkpoints=%s",
                awb,
                slug,
                t0.get("tag"),
                subtag,
                detail,
                len(t0.get("checkpoints") or []) if isinstance(t0.get("checkpoints"), list) else 0,
            )
            details = _aftership_shipment_details(t0)
            out_slug = slug
            out_name = _courier_display_name(slug) or slug.replace("-", " ").title()
            if amazonish and not str(slug).startswith("amazon"):
                out_slug = "amazon"
                out_name = "Amazon"
                link = _resolve_tracking_link(awb=awb, courier="amazon", carrier_link=link)
            if str(out_slug).startswith("amazon"):
                msg = (
                    "AfterShip recognized this as Amazon but has no delivery scans yet. "
                    "Courier set to Amazon — refresh later when Amazon reports tracking."
                )
            return {
                "ok": False,
                "courier": out_slug,
                "courier_name": out_name,
                # Do not claim ORDERED — Pending is unknown, not a real pipeline step.
                "shipment_status": None,
                "status_detail": detail,
                "eta": eta,
                "tracking_number": awb,
                "tracking_link": link,
                "method": "aftership_api",
                "tracked_at": datetime.now(timezone.utc).isoformat(),
                "error": msg,
                "_aftership_id": _norm_str(t0.get("id")),
                **details,
            }

        if not status and _norm_str(t0.get("tag")):
            status = "ORDERED"
        details = _aftership_shipment_details(t0)
        out_slug = slug
        out_name = _courier_display_name(slug) or slug.replace("-", " ").title()
        # Amazon-looking ids must not be labeled FedEx/DHL from length heuristics.
        if amazonish and not str(slug).startswith("amazon"):
            out_slug = "amazon"
            out_name = "Amazon"
            link = _resolve_tracking_link(awb=awb, courier="amazon", carrier_link=link)
        return {
            "ok": bool(status),
            "courier": out_slug,
            "courier_name": out_name,
            "shipment_status": status or "ORDERED",
            "status_detail": detail,
            "eta": eta,
            "tracking_number": awb,
            "tracking_link": link,
            "method": "aftership_api",
            "tracked_at": datetime.now(timezone.utc).isoformat(),
            "error": None if status else "Could not map AfterShip status.",
            "_aftership_id": _norm_str(t0.get("id")),
            **details,
        }

    def _poll_until_ready(tracking_id: str) -> Optional[Dict[str, Any]]:
        """AfterShip often needs a short wait before carrier checkpoints appear."""
        last_local: Optional[Dict[str, Any]] = None
        for attempt in range(4):
            try:
                resp = requests.get(
                    f"{base}/{urllib.parse.quote(tracking_id, safe='')}",
                    headers=headers,
                    timeout=DEFAULT_TIMEOUT,
                )
            except requests.RequestException:
                break
            if resp.status_code >= 400:
                break
            try:
                t0 = _aftership_extract_tracking(resp.json())
            except Exception:
                t0 = None
            if not t0:
                break
            parsed = _result_from_tracking(t0, allow_weak_pending=False)
            last_local = parsed
            if parsed and parsed.get("ok") and not _aftership_is_weak_pending(t0):
                return parsed
            # First attempts wait; last attempt returns the weak pending message.
            if attempt < 3:
                time.sleep(2.0)
        return last_local

    def _aftership_meta_error(payload: Any) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        code = meta.get("code")
        msg = _norm_str(meta.get("message"))
        if code in {401, 403, 40101, 40102, 40300, 40301, 40302, 40303}:
            return "AfterShip API key was rejected — check AFTERSHIP_API_KEY."
        if code == 4007:
            return "AfterShip create failed (tracking_number required) — API body mismatch."
        return msg

    def _handle_tracking_obj(t0: Dict[str, Any], *, poll: bool = False) -> Optional[Dict[str, Any]]:
        if not _aftership_is_weak_pending(t0):
            return _result_from_tracking(t0, allow_weak_pending=False)
        # Weak Pending: only poll when the user picked this courier (slug_hint),
        # otherwise keep trying other carrier slugs quickly.
        if poll:
            tid = _norm_str(t0.get("id"))
            if tid:
                polled = _poll_until_ready(tid)
                if polled and polled.get("ok"):
                    return polled
                if polled:
                    return polled
        return _result_from_tracking(t0, allow_weak_pending=False)

    for slug in slugs:
        params: Dict[str, str] = {"tracking_numbers": awb}
        if slug:
            params["slug"] = slug
        # Poll only when this slug matches an explicit courier hint.
        should_poll = bool(slug_hint and slug == slug_hint)
        try:
            resp = requests.get(base, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
        except requests.RequestException as exc:
            logger.warning("aftership get failed awb=%s err=%s", awb, exc)
            _remember_err(
                {
                    "ok": False,
                    "courier": slug or courier or "aftership",
                    "error": "Could not reach AfterShip.",
                    "method": "aftership_api",
                }
            )
            continue

        if resp.status_code < 400:
            try:
                t0 = _aftership_extract_tracking(resp.json())
            except Exception:
                t0 = None
            if t0:
                parsed = _handle_tracking_obj(t0, poll=should_poll)
                if parsed and parsed.get("ok"):
                    parsed.pop("_aftership_id", None)
                    logger.info(
                        "aftership hit awb=%s slug=%s status=%s",
                        awb,
                        parsed.get("courier"),
                        parsed.get("shipment_status"),
                    )
                    return parsed
                if parsed:
                    _remember_err(parsed)
                    # Keep trying other slugs — wrong carrier often stays Pending forever.
        else:
            err_msg = _aftership_meta_error(resp.json() if resp.content else None)
            if err_msg:
                err_payload = {
                    "ok": False,
                    "courier": slug or courier or "aftership",
                    "error": err_msg,
                    "method": "aftership_api",
                }
                _remember_err(err_payload)
                if "API key" in err_msg:
                    return err_payload

        # Create tracking then re-fetch (AfterShip needs registration first).
        create_body: Dict[str, Any] = {"tracking_number": awb}
        if slug:
            create_body["slug"] = slug
        try:
            created = requests.post(
                base,
                json=create_body,
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
            )
        except requests.RequestException:
            continue

        created_payload = None
        try:
            created_payload = created.json()
        except Exception:
            created_payload = None

        meta_code = None
        if isinstance(created_payload, dict):
            meta_code = ((created_payload.get("meta") or {}) or {}).get("code")

        if created.status_code < 400 or meta_code in {4003, 400}:
            t0 = _aftership_extract_tracking(created_payload) if created_payload else None
            if t0:
                parsed = _handle_tracking_obj(t0, poll=should_poll)
                if parsed and parsed.get("ok"):
                    parsed.pop("_aftership_id", None)
                    return parsed
                if parsed:
                    _remember_err(parsed)
            try:
                resp2 = requests.get(base, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
                if resp2.status_code < 400:
                    t0 = _aftership_extract_tracking(resp2.json())
                    if t0:
                        parsed = _handle_tracking_obj(t0, poll=should_poll)
                        if parsed and parsed.get("ok"):
                            parsed.pop("_aftership_id", None)
                            return parsed
                        if parsed:
                            _remember_err(parsed)
            except requests.RequestException:
                continue
        else:
            err_msg = _aftership_meta_error(created_payload)
            if err_msg:
                err_payload = {
                    "ok": False,
                    "courier": slug or courier or "aftership",
                    "error": err_msg,
                    "method": "aftership_api",
                }
                _remember_err(err_payload)
                if "API key" in err_msg:
                    return err_payload

    if last_err:
        last_err.pop("_aftership_id", None)
        logger.info(
            "aftership exhausted slugs awb=%s last_courier=%s ok=%s error=%s",
            awb,
            last_err.get("courier"),
            last_err.get("ok"),
            last_err.get("error"),
        )
    return last_err or {
        "ok": False,
        "courier": courier or "aftership",
        "error": "No AfterShip tracking found for this number.",
        "method": "aftership_api",
    }


def _track_allowlisted_link(url: str) -> Dict[str, Any]:
    safe_url, courier = _assert_safe_track_url(url, allow_vendor_pages=True)
    sess = _session()
    if courier == "delhivery":
        sess.headers["Origin"] = "https://www.delhivery.com"
        sess.headers["Referer"] = "https://www.delhivery.com/"
    try:
        resp = sess.get(safe_url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
    except requests.RequestException as exc:
        logger.warning("track link fetch failed courier=%s err=%s", courier, exc)
        return {
            "ok": False,
            "courier": courier,
            "error": "Could not reach the tracking page.",
            "method": "link_scrape",
        }
    if resp.status_code >= 400:
        return {
            "ok": False,
            "courier": courier,
            "error": "Tracking page returned an error.",
            "method": "link_scrape",
        }

    text = resp.text or ""
    content_type = (resp.headers.get("content-type") or "").lower()
    candidates: List[str] = []
    eta = None
    parsed_vendor: Dict[str, Optional[str]] = {}

    if "json" in content_type:
        try:
            payload = resp.json()
            candidates = _walk_strings(payload)
            eta = _extract_eta(candidates, payload if isinstance(payload, dict) else None)
        except Exception:
            candidates = []
    else:
        parsed_vendor = _parse_vendor_tracking_html(text)
        if parsed_vendor.get("shipment_status"):
            status = parsed_vendor["shipment_status"]
            awb = (
                parsed_vendor.get("tracking_number")
                or extract_tracking_number_from_url(safe_url)
                or _extract_order_id_from_url(safe_url)
            )
            courier_name = parsed_vendor.get("courier_name")
            if courier == "vendor" and courier_name:
                detected = detect_courier(courier_name=courier_name)
                courier = detected or "vendor"
            return {
                "ok": True,
                "courier": courier,
                "courier_name": courier_name
                or (None if courier == "vendor" else courier.replace("_", " ").title()),
                "shipment_status": status,
                "status_detail": parsed_vendor.get("status_detail"),
                "eta": parsed_vendor.get("eta"),
                "tracking_number": awb,
                "tracking_link": safe_url,
                "method": "vendor_page" if courier == "vendor" or parsed_vendor.get("courier_name") else "link_scrape",
                "tracked_at": datetime.now(timezone.utc).isoformat(),
                "error": None,
            }

        cleaned = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
        cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        windows: List[str] = []
        lower = cleaned.lower()
        for _, kws in _STATUS_KEYWORD_RULES:
            for kw in kws:
                idx = lower.find(kw)
                if idx >= 0:
                    start = max(0, idx - 40)
                    end = min(len(cleaned), idx + len(kw) + 60)
                    windows.append(cleaned[start:end])
        candidates = windows or [cleaned[:1200]]
        eta = _extract_eta(candidates)

    for m in re.finditer(r"(\{[^{}]{0,40}\"(?:status|tag|shipment_status)\"[^{}]{0,200}\})", text):
        try:
            blob = json.loads(m.group(1))
            candidates.extend(_walk_strings(blob, limit=20))
        except Exception:
            pass

    status = _pick_best_status(candidates)
    awb = (
        (parsed_vendor or {}).get("tracking_number")
        or extract_tracking_number_from_url(safe_url)
        or _extract_awb_from_text(text)
        or _extract_order_id_from_url(safe_url)
    )
    courier_name = (parsed_vendor or {}).get("courier_name") or (
        None if courier == "vendor" else courier.replace("_", " ").title()
    )
    return {
        "ok": bool(status),
        "courier": courier,
        "courier_name": courier_name,
        "shipment_status": status or ("ORDERED" if awb else None),
        "status_detail": next((c for c in candidates if map_status_text(c) == status), None),
        "eta": eta or (parsed_vendor or {}).get("eta"),
        "tracking_number": awb,
        "tracking_link": safe_url,
        "method": "vendor_page" if courier == "vendor" else "link_scrape",
        "tracked_at": datetime.now(timezone.utc).isoformat(),
        "error": None if status else "Could not determine status from tracking page.",
    }


def track_shipment(
    *,
    tracking_number: Optional[str] = None,
    tracking_link: Optional[str] = None,
    courier_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Resolve live shipment status from number and/or link.

    Returns a dict with shipment_status, courier_name, eta, tracking_*, method, ok, error.
    """
    number = _norm_str(tracking_number)
    link = _norm_str(tracking_link)

    # If one field holds a paste of the other shape, split it.
    if number and not link and (number.lower().startswith("http") or number.lower().startswith("www.")):
        split = normalize_tracking_paste(number)
        link = split.get("tracking_link")
        number = split.get("tracking_number") or number
    if link and not number:
        number = extract_tracking_number_from_url(link)

    if not number and not link:
        raise ShipmentTrackError("Provide a tracking number or tracking link.")

    courier = detect_courier(
        tracking_link=link,
        courier_name=courier_name,
        tracking_number=number,
    )
    last: Optional[Dict[str, Any]] = None
    aftership_last: Optional[Dict[str, Any]] = None
    scraper_names = {"bluedart", "delhivery"}
    aftership_ready = bool(_norm_str(os.environ.get("AFTERSHIP_API_KEY")))
    logger.info(
        "track_shipment start awb=%s link=%s courier_hint=%s aftership_ready=%s",
        number,
        (link[:80] + "…") if link and len(link) > 80 else link,
        courier or courier_name,
        aftership_ready,
    )

    def _finalize(result: Dict[str, Any]) -> Dict[str, Any]:
        """Always attach a usable tracking_link when the user only pasted an AWB."""
        courier_for_link = (
            courier
            or _normalize_courier_slug(_norm_str(result.get("courier")))
            or _normalize_courier_slug(_norm_str(result.get("courier_name")))
            or _normalize_courier_slug(courier_name)
        )
        result["tracking_link"] = _resolve_tracking_link(
            awb=number or result.get("tracking_number"),
            courier=courier_for_link,
            user_link=link,
            carrier_link=result.get("tracking_link"),
        )
        if number and not result.get("tracking_number"):
            result["tracking_number"] = number
        return result

    def _try_scraper(name: str, tracker: Any) -> Optional[Dict[str, Any]]:
        nonlocal last
        if link:
            host_l = link.lower()
            if "bluedart" in host_l and name == "delhivery":
                return None
            if "delhivery" in host_l and name == "bluedart":
                return None
            # Don't let scrapers steal FedEx/DHL numbers — those go through AfterShip.
            if "fedex" in host_l or "dhl." in host_l or "dhl/" in host_l:
                return None
        result = tracker(number)
        if result.get("ok"):
            return _finalize(result)
        if result.get("error") and "not configured" not in str(result.get("error")).lower():
            last = result
        elif last is None:
            last = result
        return None

    # AfterShip is the single multi-carrier source (FedEx / DHL / BlueDart / …).
    # BlueDart / Delhivery scrapers only when the courier/link is clearly them,
    # or as a last fallback after AfterShip for bare AWBs.
    if number:
        scrape_trackers = list(_carrier_trackers_for_awb(number, courier))

        prefer_scraper_first = courier in scraper_names or (
            bool(link) and any(k in link.lower() for k in ("bluedart", "delhivery"))
        )

        def _run_aftership() -> Optional[Dict[str, Any]]:
            nonlocal aftership_last, last
            if not aftership_ready:
                return None
            after = _track_aftership_api(number, courier=courier)
            if after and after.get("ok"):
                after.pop("_aftership_id", None)
                return _finalize(after)
            if after:
                after.pop("_aftership_id", None)
                aftership_last = after
                if after.get("error") and "not configured" not in str(after.get("error")).lower():
                    last = after
                after_courier = str(after.get("courier") or "").lower()
                if courier and after_courier in {courier, courier.replace("_", "-")} and after.get("error"):
                    return _finalize(after)
            return None

        if prefer_scraper_first:
            for name, tracker in scrape_trackers:
                hit = _try_scraper(name, tracker)
                if hit:
                    return hit
            hit = _run_aftership()
            if hit:
                return hit
        else:
            hit = _run_aftership()
            if hit:
                return hit
            for name, tracker in scrape_trackers:
                hit = _try_scraper(name, tracker)
                if hit:
                    return hit

    # Allowlisted / vendor tracking link scrape.
    if link:
        try:
            scraped = _track_allowlisted_link(link)
            if scraped.get("ok"):
                return _finalize(scraped)
            last = scraped
        except ShipmentTrackError as exc:
            last = {
                "ok": False,
                "error": str(exc),
                "courier": courier,
                "method": "link_scrape",
            }

    if last and last.get("ok"):
        return last

    # Soft fallback: we have tracking identifiers but couldn't read live status.
    # Prefer AfterShip's error over a wrong-courier scraper message.
    err = None
    if aftership_last and aftership_last.get("error"):
        err = aftership_last.get("error")
    elif last:
        err = last.get("error")
    if err and any(
        phrase in str(err).lower()
        for phrase in (
            "invalid awb",
            "very old package",
            "no tracking data",
            "not found",
            "not configured",
        )
    ):
        # Keep AfterShip-specific failures readable.
        if not (aftership_last and aftership_last.get("error")):
            err = "Could not resolve live carrier status for this tracking number yet."

    if not err:
        err = "Live carrier status unavailable. Click Refresh later when the carrier has scans."

    # Only hint about missing AfterShip when it really isn't configured.
    if not _norm_str(os.environ.get("AFTERSHIP_API_KEY")):
        err = (
            f"{err} Set AFTERSHIP_API_KEY in pyro-backend .env "
            "(covers FedEx / DHL / BlueDart / …) and restart the backend."
        )

    # Prefer AfterShip soft-fail payload (null status) — never invent ORDERED/Delivered.
    src = aftership_last or last or {}
    courier_out = src.get("courier") or courier
    name_out = _norm_str(courier_name) or _courier_display_name(src.get("courier_name")) or src.get("courier_name")
    if not name_out and courier_out:
        name_out = _courier_display_name(str(courier_out)) or str(courier_out).replace("-", " ").title()
    # Normalize AfterShip amazon-* slugs to the UI label "Amazon".
    if name_out and str(name_out).lower().startswith("amazon"):
        name_out = "Amazon"
    status_out = src.get("shipment_status") if src else None
    # Only default to ORDERED when we have no AfterShip answer at all.
    if status_out is None and not aftership_last:
        status_out = "ORDERED"

    return _finalize(
        {
            "ok": False,
            "courier": courier_out,
            "courier_name": name_out,
            "shipment_status": status_out,
            "status_detail": src.get("status_detail"),
            "eta": src.get("eta"),
            "tracking_number": number,
            "tracking_link": src.get("tracking_link"),
            "method": src.get("method") or "fallback",
            "tracked_at": datetime.now(timezone.utc).isoformat(),
            "error": err,
            "origin": src.get("origin"),
            "destination": src.get("destination"),
            "current_location": src.get("current_location"),
            "events": src.get("events") or [],
        }
    )
