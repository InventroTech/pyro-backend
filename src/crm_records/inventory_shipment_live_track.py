"""
Live shipment status lookup for inventory_request tracking.

Ops paste a tracking number and/or tracking link; we resolve the current
carrier state into the canonical pipeline statuses:

  ORDERED → IN_TRANSIT → OUT_FOR_DELIVERY → DELIVERED  (+ EXCEPTION)

Sources (tried in smart order per AWB / courier hint):
  - BlueDart TrackDart page (no key)
  - Delhivery unified-tracking API (no key)
  - Vendor order-track pages (e.g. genxbattery.com/track?…)
  - FedEx Track API when FEDEX_API_KEY + FEDEX_SECRET_KEY are set
  - DHL Unified Tracking when DHL_API_KEY is set
  - AfterShip when AFTERSHIP_API_KEY is set (covers FedEx / DHL / BlueDart / …)
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import ipaddress
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
    "amazon": ("amazon.in", "amazon.com"),
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


def detect_courier(
    *,
    tracking_link: Optional[str] = None,
    courier_name: Optional[str] = None,
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
        if "post" in name:
            return "indiapost"
        for courier in _ALLOWED_TRACK_HOST_SUFFIXES:
            if courier in name or name in courier:
                return courier

    link = _norm_str(tracking_link)
    if link:
        try:
            host = (urllib.parse.urlparse(_ensure_https(link)).hostname or "").lower()
        except Exception:
            host = ""
        detected = _detect_courier_from_host(host)
        if detected:
            return detected
    return None


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
        for key in ("order", "order_id", "orderId", "orderno"):
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
    m = re.search(
        r"Status\s+(Shipment\s+Delivered|Shipment\s+Out\s+for\s+Delivery|"
        r"In\s+Transit|Shipment\s+Undelivered|Delivered|Out\s+for\s+Delivery)",
        cleaned,
        re.I,
    )
    if m:
        status_detail = m.group(1).strip()
        status = map_status_text(status_detail)
    if not status:
        parsed = _parse_vendor_tracking_html(text)
        status = parsed.get("shipment_status")
        status_detail = parsed.get("status_detail")

    if not status:
        # No usable BlueDart result for this AWB (wrong courier / not found).
        if "waybill no" not in cleaned.lower() and awb not in cleaned:
            return {
                "ok": False,
                "courier": "bluedart",
                "error": "No BlueDart tracking data found for this AWB.",
                "method": "bluedart_page",
            }
        # Page mentions AWB but status unclear — keyword fallback.
        status = _pick_best_status([cleaned[i : i + 80] for i in range(0, min(len(cleaned), 4000), 200)])

    eta = None
    m = re.search(r"Date of Delivery\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})", cleaned, re.I)
    if m:
        eta = m.group(1).strip()

    return {
        "ok": bool(status),
        "courier": "bluedart",
        "courier_name": "BlueDart",
        "shipment_status": status or "ORDERED",
        "status_detail": status_detail,
        "eta": eta,
        "tracking_number": awb,
        "tracking_link": url,
        "method": "bluedart_page",
        "tracked_at": datetime.now(timezone.utc).isoformat(),
        "error": None if status else "Could not determine delivery status from BlueDart.",
    }


def _carrier_trackers_for_awb(
    awb: str, courier: Optional[str]
) -> List[Tuple[str, Any]]:
    """
    Ordered list of (name, tracker_fn) to try for a bare AWB.

    Includes free scrapers always; FedEx/DHL/AfterShip run when configured
    (trackers no-op / return not-ok when env keys are missing).
    """
    trackers = {
        "bluedart": _track_bluedart,
        "delhivery": _track_delhivery,
        "fedex": _track_fedex,
        "dhl": _track_dhl,
    }

    # AWB shape heuristics (best-effort — still fall through to others).
    digits = awb.isdigit()
    length = len(awb)
    guessed: List[str] = []
    if courier in trackers:
        guessed.append(courier)
    if digits and length == 11:
        guessed.extend(["bluedart", "delhivery", "fedex", "dhl"])
    elif digits and length == 12:
        guessed.extend(["fedex", "dhl", "delhivery", "bluedart"])
    elif digits and length in {10, 9}:
        guessed.extend(["dhl", "fedex", "bluedart", "delhivery"])
    else:
        guessed.extend(["bluedart", "delhivery", "fedex", "dhl"])

    # De-dupe, preserve order, always eventually try every native tracker.
    order: List[str] = []
    for name in guessed + ["bluedart", "delhivery", "fedex", "dhl"]:
        if name not in order and name in trackers:
            order.append(name)
    return [(name, trackers[name]) for name in order]


def _fedex_access_token() -> Optional[str]:
    client_id = _norm_str(os.environ.get("FEDEX_API_KEY") or os.environ.get("FEDEX_CLIENT_ID"))
    client_secret = _norm_str(os.environ.get("FEDEX_SECRET_KEY") or os.environ.get("FEDEX_CLIENT_SECRET"))
    if not client_id or not client_secret:
        return None
    # Fixed FedEx API host.
    url = "https://apis.fedex.com/oauth/token"
    try:
        resp = requests.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": USER_AGENT},
            timeout=DEFAULT_TIMEOUT,
        )
    except requests.RequestException as exc:
        logger.warning("fedex oauth failed err=%s", exc)
        return None
    if resp.status_code >= 400:
        logger.warning("fedex oauth status=%s", resp.status_code)
        return None
    try:
        return _norm_str((resp.json() or {}).get("access_token"))
    except Exception:
        return None


def _track_fedex(awb: str) -> Dict[str, Any]:
    """FedEx Track API (requires FEDEX_API_KEY + FEDEX_SECRET_KEY)."""
    token = _fedex_access_token()
    if not token:
        return {
            "ok": False,
            "courier": "fedex",
            "error": "FedEx API not configured (set FEDEX_API_KEY and FEDEX_SECRET_KEY).",
            "method": "fedex_api",
        }
    url = "https://apis.fedex.com/track/v1/trackingnumbers"
    body = {
        "includeDetailedScans": True,
        "trackingInfo": [{"trackingNumberInfo": {"trackingNumber": awb}}],
    }
    try:
        resp = requests.post(
            url,
            json=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "X-locale": "en_US",
                "User-Agent": USER_AGENT,
            },
            timeout=DEFAULT_TIMEOUT,
        )
    except requests.RequestException as exc:
        logger.warning("fedex track failed awb=%s err=%s", awb, exc)
        return {
            "ok": False,
            "courier": "fedex",
            "error": "Could not reach FedEx tracking.",
            "method": "fedex_api",
        }
    if resp.status_code >= 400:
        return {
            "ok": False,
            "courier": "fedex",
            "error": "FedEx tracking returned an error.",
            "method": "fedex_api",
        }
    try:
        payload = resp.json()
    except Exception:
        return {
            "ok": False,
            "courier": "fedex",
            "error": "FedEx returned invalid JSON.",
            "method": "fedex_api",
        }

    candidates = _walk_strings(payload)
    # Prefer latestStatusDetail fields when present.
    try:
        results = (
            (((payload or {}).get("output") or {}).get("completeTrackResults") or [])[0].get("trackResults")
            or []
        )
        if results and isinstance(results[0], dict):
            detail = results[0].get("latestStatusDetail") or {}
            for key in ("description", "statusByLocale", "code", "derivedCode"):
                if detail.get(key):
                    candidates.insert(0, str(detail.get(key)))
    except Exception:
        pass

    status = _pick_best_status(candidates)
    eta = _extract_eta(candidates, payload if isinstance(payload, dict) else None)
    safe_awb = urllib.parse.quote(awb, safe="")
    return {
        "ok": bool(status),
        "courier": "fedex",
        "courier_name": "FedEx",
        "shipment_status": status or "ORDERED",
        "status_detail": next((c for c in candidates if map_status_text(c) == status), None),
        "eta": eta,
        "tracking_number": awb,
        "tracking_link": f"https://www.fedex.com/fedextrack/?trknbr={safe_awb}",
        "method": "fedex_api",
        "tracked_at": datetime.now(timezone.utc).isoformat(),
        "error": None if status else "Could not determine delivery status from FedEx.",
    }


def _track_dhl(awb: str) -> Dict[str, Any]:
    """DHL Unified Tracking API (requires DHL_API_KEY)."""
    api_key = _norm_str(os.environ.get("DHL_API_KEY"))
    if not api_key:
        return {
            "ok": False,
            "courier": "dhl",
            "error": "DHL API not configured (set DHL_API_KEY).",
            "method": "dhl_api",
        }
    # Fixed DHL API host; AWB only as query param.
    url = "https://api-eu.dhl.com/track/shipments"
    try:
        resp = requests.get(
            url,
            params={"trackingNumber": awb},
            headers={
                "DHL-API-Key": api_key,
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
            timeout=DEFAULT_TIMEOUT,
        )
    except requests.RequestException as exc:
        logger.warning("dhl track failed awb=%s err=%s", awb, exc)
        return {
            "ok": False,
            "courier": "dhl",
            "error": "Could not reach DHL tracking.",
            "method": "dhl_api",
        }
    if resp.status_code >= 400:
        return {
            "ok": False,
            "courier": "dhl",
            "error": "DHL tracking returned an error.",
            "method": "dhl_api",
        }
    try:
        payload = resp.json()
    except Exception:
        return {
            "ok": False,
            "courier": "dhl",
            "error": "DHL returned invalid JSON.",
            "method": "dhl_api",
        }

    candidates = _walk_strings(payload)
    try:
        shipments = (payload or {}).get("shipments") or []
        if shipments and isinstance(shipments[0], dict):
            st = shipments[0].get("status") or {}
            for key in ("statusCode", "status", "description", "remark"):
                if st.get(key):
                    candidates.insert(0, str(st.get(key)))
    except Exception:
        pass

    # DHL statusCode values: pre-transit, transit, delivered, failure, unknown
    status = _pick_best_status(candidates)
    if not status:
        raw = " ".join(candidates[:8]).lower()
        if "delivered" in raw:
            status = "DELIVERED"
        elif "failure" in raw or "exception" in raw:
            status = "EXCEPTION"
        elif "transit" in raw:
            status = "IN_TRANSIT"
        elif "pre-transit" in raw or "pretransit" in raw:
            status = "ORDERED"

    eta = _extract_eta(candidates, payload if isinstance(payload, dict) else None)
    safe_awb = urllib.parse.quote(awb, safe="")
    return {
        "ok": bool(status),
        "courier": "dhl",
        "courier_name": "DHL",
        "shipment_status": status or "ORDERED",
        "status_detail": next((c for c in candidates if map_status_text(c) == status), None),
        "eta": eta,
        "tracking_number": awb,
        "tracking_link": f"https://www.dhl.com/en/express/tracking.html?AWB={safe_awb}&brand=DHL",
        "method": "dhl_api",
        "tracked_at": datetime.now(timezone.utc).isoformat(),
        "error": None if status else "Could not determine delivery status from DHL.",
    }


def _track_aftership_api(awb: str, courier: Optional[str] = None) -> Optional[Dict[str, Any]]:
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
    base = "https://api.aftership.com/tracking/2024-04/trackings"

    slug_hint = None
    if courier and courier not in {"aftership", "amazon", "vendor"}:
        slug_hint = courier.replace("_", "-")

    # Try GET (existing), then POST create + GET, across likely carrier slugs.
    slug_candidates: List[Optional[str]] = []
    if slug_hint:
        slug_candidates.append(slug_hint)
    slug_candidates.extend(
        [
            None,  # auto / any
            "fedex",
            "dhl",
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

    last_err: Optional[Dict[str, Any]] = None

    def _parse_trackings(payload: Any) -> Optional[Dict[str, Any]]:
        trackings = (((payload or {}).get("data") or {}).get("trackings")) if isinstance(payload, dict) else None
        if not isinstance(trackings, list) or not trackings:
            # create response nests under data.tracking
            one = ((payload or {}).get("data") or {}).get("tracking") if isinstance(payload, dict) else None
            if isinstance(one, dict):
                trackings = [one]
            else:
                return None
        t0 = trackings[0] if isinstance(trackings[0], dict) else {}
        tag = t0.get("tag") or t0.get("subtag") or ""
        status = map_status_text(str(tag)) or map_status_text(str(t0.get("subtag_message") or ""))
        eta = _norm_str(t0.get("expected_delivery"))
        slug = _norm_str(t0.get("slug")) or courier or "aftership"
        return {
            "ok": bool(status),
            "courier": slug,
            "courier_name": slug.replace("-", " ").title(),
            "shipment_status": status or "ORDERED",
            "status_detail": str(tag or "") or None,
            "eta": eta,
            "tracking_number": awb,
            "tracking_link": f"https://www.aftership.com/track/{slug}/{urllib.parse.quote(awb, safe='')}",
            "method": "aftership_api",
            "tracked_at": datetime.now(timezone.utc).isoformat(),
            "error": None if status else "Could not map AfterShip status.",
        }

    for slug in slugs:
        params: Dict[str, str] = {"tracking_numbers": awb}
        if slug:
            params["slug"] = slug
        try:
            resp = requests.get(base, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
        except requests.RequestException as exc:
            logger.warning("aftership get failed awb=%s err=%s", awb, exc)
            last_err = {
                "ok": False,
                "courier": slug or courier or "aftership",
                "error": "Could not reach AfterShip.",
                "method": "aftership_api",
            }
            continue

        if resp.status_code < 400:
            try:
                parsed = _parse_trackings(resp.json())
            except Exception:
                parsed = None
            if parsed and parsed.get("ok"):
                return parsed
            if parsed:
                last_err = parsed

        # Create tracking then re-fetch (AfterShip often needs registration first).
        create_body: Dict[str, Any] = {"tracking_number": awb}
        if slug:
            create_body["slug"] = slug
        try:
            created = requests.post(
                base,
                json={"tracking": create_body},
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
            )
        except requests.RequestException:
            continue
        if created.status_code < 400 or created.status_code in {400, 409}:
            # 409 = already exists; either way try parse create body or GET again.
            try:
                parsed = _parse_trackings(created.json())
                if parsed and parsed.get("ok"):
                    return parsed
            except Exception:
                pass
            try:
                resp2 = requests.get(base, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
                if resp2.status_code < 400:
                    parsed = _parse_trackings(resp2.json())
                    if parsed and parsed.get("ok"):
                        return parsed
                    if parsed:
                        last_err = parsed
            except requests.RequestException:
                continue

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

    courier = detect_courier(tracking_link=link, courier_name=courier_name)
    last: Optional[Dict[str, Any]] = None

    # 1) Try carrier-native trackers for AWB (BlueDart / Delhivery / FedEx / DHL).
    #    Wrong-courier failures are ignored — keep trying the rest.
    if number:
        for name, tracker in _carrier_trackers_for_awb(number, courier):
            if link:
                host_l = link.lower()
                if "bluedart" in host_l and name == "delhivery":
                    continue
                if "delhivery" in host_l and name == "bluedart":
                    continue
                if "fedex" in host_l and name in {"bluedart", "delhivery", "dhl"}:
                    # Still allow fedex tracker; skip unrelated scrapers first pass
                    if name != "fedex":
                        continue
                if ("dhl." in host_l or "dhl/" in host_l) and name in {"bluedart", "delhivery", "fedex"}:
                    if name != "dhl":
                        continue
            result = tracker(number)
            if result.get("ok"):
                if link:
                    result["tracking_link"] = link
                return result
            # Ignore "not configured" soft skips when deciding last error priority.
            if result.get("error") and "not configured" not in str(result.get("error")).lower():
                last = result
            elif last is None:
                last = result

    # 2) Optional AfterShip API (covers FedEx / DHL / BlueDart / … with one key).
    if number:
        after = _track_aftership_api(number, courier=courier)
        if after and after.get("ok"):
            if link:
                after["tracking_link"] = link
            return after
        if after and after.get("error") and "not configured" not in str(after.get("error")).lower():
            last = after


    # 3) Allowlisted / vendor tracking link scrape.
    if link:
        try:
            scraped = _track_allowlisted_link(link)
            if scraped.get("ok"):
                if number and not scraped.get("tracking_number"):
                    scraped["tracking_number"] = number
                return scraped
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
    # Avoid leaking a wrong-courier message like Delhivery "invalid AWB".
    err = (last or {}).get("error") if last else None
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
        err = "Could not resolve live carrier status for this tracking number yet."

    if not err:
        err = "Live carrier status unavailable — pipeline set to Ordered until tracking resolves."

    # Hint when FedEx/DHL need API keys (their public sites block scrapers).
    missing = []
    if not _norm_str(os.environ.get("FEDEX_API_KEY") or os.environ.get("FEDEX_CLIENT_ID")):
        missing.append("FEDEX_API_KEY/FEDEX_SECRET_KEY")
    if not _norm_str(os.environ.get("DHL_API_KEY")):
        missing.append("DHL_API_KEY")
    if not _norm_str(os.environ.get("AFTERSHIP_API_KEY")):
        missing.append("AFTERSHIP_API_KEY")
    if missing and (not courier or courier in {"fedex", "dhl"}):
        err = (
            f"{err} For FedEx/DHL set AFTERSHIP_API_KEY "
            f"(or {' / '.join(missing[:2])})."
        )

    return {
        "ok": False,
        "courier": courier or (last or {}).get("courier"),
        "courier_name": (_norm_str(courier_name) or (courier.title() if courier else None)),
        "shipment_status": "ORDERED",
        "status_detail": None,
        "eta": None,
        "tracking_number": number,
        "tracking_link": link,
        "method": "fallback",
        "tracked_at": datetime.now(timezone.utc).isoformat(),
        "error": err,
    }
