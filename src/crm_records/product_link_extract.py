"""
Extract product name and price from a pasted storefront URL.

Uses ScrapingBee (same pattern as AfterShip for shipment tracking):
  SCRAPINGBEE_API_KEY in pyro-backend .env / Render env.

We never fetch the vendor page from Render. ScrapingBee renders it behind
residential/premium proxies so Cloudflare/WAF blocks do not apply.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os
import re
import socket
import urllib.parse
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

SCRAPINGBEE_ENDPOINT = "https://app.scrapingbee.com/api/v1/"
DEFAULT_TIMEOUT = 40

_STRIP_QUERY_KEYS = {
    "srsltid",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
}

_VENDOR_BY_HOST = {
    "amazon.in": "AMAZON",
    "amazon.com": "AMAZON",
    "robu.in": "ROBU",
    "robocraze.com": "ROBOCRAZE",
    "zbotic.in": "ZBOTIC",
    "flyrobo.in": "FLYROBO",
    "robokits.co.in": "ROBOKITS",
    "mouser.in": "MOUSER",
    "mouser.com": "MOUSER",
    "digikey.in": "DIGIKEY",
    "digikey.com": "DIGIKEY",
    "tannatechbiz.com": "TANNATECHBIZ",
    "anubisrc.com": "ANUBISRC",
    "uavstore.in": "UAVSTORE",
    "fpvstore.in": "FPVSTORE",
    "fpvguru.in": "FPVGURU",
    "fpvguru.com": "FPVGURU",
    "evelta.com": "EVELTA",
    "tujorc.com": "TUJORC",
    "quadkart.in": "QUADKART",
    "ktron.in": "KTRON",
    "drkstore.in": "DRKSTORE",
    "uavgarage.com": "UAVGARAGE",
    "fabtolab.com": "FABTOLAB",
    "flipkart.com": "FLIPKART",
}

_AI_EXTRACT_RULES = {
    "title": {
        "description": "the product name or title shown on the page",
        "type": "string",
    },
    "price": {
        "description": "the current selling price as a number only, no currency symbol",
        "type": "number",
    },
    "currency": {
        "description": "ISO currency code such as INR or USD",
        "type": "string",
    },
    "image": {
        "description": "absolute URL of the main product photo (og:image or first gallery image), not a logo or icon",
        "type": "string",
    },
    "available": {
        "description": "whether the product is in stock",
        "type": "boolean",
    },
}

_PRICE_RE = re.compile(r"(\d[\d,]*(?:\.\d{1,4})?)")


class ProductLinkExtractError(ValueError):
    """Invalid product URL."""


def _norm_str(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def scrapingbee_configured() -> bool:
    return bool(_scrapingbee_api_key())


def _scrapingbee_api_key() -> Optional[str]:
    raw = ""
    try:
        from django.conf import settings as dj_settings

        raw = str(getattr(dj_settings, "SCRAPINGBEE_API_KEY", "") or "")
    except Exception:
        pass
    return _norm_str(raw or os.getenv("SCRAPINGBEE_API_KEY"))


def _strip_tracking_query(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return url
    if not parsed.query:
        return url
    pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    kept = [(k, v) for (k, v) in pairs if k.lower() not in _STRIP_QUERY_KEYS]
    query = urllib.parse.urlencode(kept, doseq=True)
    return urllib.parse.urlunparse(parsed._replace(query=query))


def _host_resolves_to_public_ip(host: str) -> bool:
    try:
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except socket.gaierror:
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


def assert_safe_product_url(url: str) -> str:
    """Return a reconstructed https URL, or raise ProductLinkExtractError."""
    raw = _norm_str(url)
    if not raw:
        raise ProductLinkExtractError("Product URL is required.")
    if "://" not in raw:
        raw = "https://" + raw
    raw = _strip_tracking_query(raw)
    try:
        parsed = urllib.parse.urlparse(raw)
    except Exception as exc:
        raise ProductLinkExtractError("Invalid product URL.") from exc
    if parsed.scheme.lower() != "https":
        raise ProductLinkExtractError("Only https product URLs are supported.")
    host = (parsed.hostname or "").lower().strip(".")
    if not host or parsed.username or parsed.password:
        raise ProductLinkExtractError("Invalid product URL.")
    if parsed.port not in (None, 443):
        raise ProductLinkExtractError("Invalid product URL port.")
    try:
        ipaddress.ip_address(host)
        raise ProductLinkExtractError("IP product URLs are not allowed.")
    except ValueError:
        pass
    if not _host_resolves_to_public_ip(host):
        raise ProductLinkExtractError("Product URL host is not public.")
    path = parsed.path or "/"
    if path.startswith("//") or "\\" in path or "@" in path:
        raise ProductLinkExtractError("Invalid product URL path.")
    path = urllib.parse.quote(urllib.parse.unquote(path), safe="/-._~")
    query = urllib.parse.quote(urllib.parse.unquote(parsed.query or ""), safe="=&%+-._~")
    return urllib.parse.urlunparse(("https", host, path, "", query, ""))


def vendor_from_host(host: str) -> str:
    host = (host or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    if host in _VENDOR_BY_HOST:
        return _VENDOR_BY_HOST[host]
    for suffix, name in _VENDOR_BY_HOST.items():
        if host == suffix or host.endswith("." + suffix):
            return name
    label = host.split(".")[0] if host else ""
    return label.replace("-", " ").upper() if label else ""


def parse_price_number(value: Any) -> Optional[float]:
    if value is None or value is False:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        n = float(value)
        return n if n > 0 else None
    text = str(value).strip()
    if not text:
        return None
    m = _PRICE_RE.search(text.replace(" ", ""))
    if not m:
        return None
    try:
        n = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    return n if n > 0 else None


def _parse_currency(value: Any, *, default: str = "INR") -> str:
    raw = str(value or "").strip().upper()
    if raw in {"INR", "RS", "₹", "RUPEE", "RUPEES"}:
        return "INR"
    if raw in {"USD", "US$", "$"}:
        return "USD"
    if "INR" in raw or "₹" in raw or "RS" in raw:
        return "INR"
    if "USD" in raw or "$" in raw:
        return "USD"
    return default


def normalize_image_url(value: Any) -> Optional[str]:
    """Return an https product image URL, or None."""
    raw = _norm_str(value)
    if not raw:
        return None
    if raw.startswith("//"):
        raw = "https:" + raw
    elif raw.startswith("http://"):
        raw = "https://" + raw[len("http://") :]
    if not raw.lower().startswith("https://"):
        return None
    if raw.lower().startswith("https://invalid") or "placeholder" in raw.lower():
        return None
    try:
        parsed = urllib.parse.urlparse(raw)
    except Exception:
        return None
    if not parsed.hostname:
        return None
    return raw[:2000]


def extract_product_from_url(url: str, *, pincode: Optional[str] = None) -> Dict[str, Any]:
    """Fetch structured product fields for a storefront URL via ScrapingBee."""
    del pincode  # unused; kept for API compatibility
    safe_url = assert_safe_product_url(url)
    host = (urllib.parse.urlparse(safe_url).hostname or "").lower()
    vendor = vendor_from_host(host)
    api_key = _scrapingbee_api_key()
    logger.info(
        "product_link_extract start host=%s vendor=%s key_set=%s",
        host,
        vendor,
        bool(api_key),
    )
    if not api_key:
        return {
            "ok": False,
            "configured": False,
            "title": None,
            "price": None,
            "currency": "INR",
            "image": None,
            "available": None,
            "vendor": vendor,
            "link": safe_url,
            "error": (
                "Product link extract is not configured. "
                "Set SCRAPINGBEE_API_KEY (same pattern as AFTERSHIP_API_KEY)."
            ),
            "method": "scrapingbee",
            "debug": {"key_set": False, "host": host},
        }

    params = {
        "api_key": api_key,
        "url": safe_url,
        "render_js": "true",
        "premium_proxy": "true",
        "country_code": "in",
        "ai_extract_rules": json.dumps(_AI_EXTRACT_RULES, separators=(",", ":")),
    }
    try:
        resp = requests.get(SCRAPINGBEE_ENDPOINT, params=params, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as exc:
        logger.warning("scrapingbee request failed host=%s err=%s", host, exc)
        return _fail(
            safe_url,
            vendor,
            "Could not reach the product extract service.",
            debug={"host": host, "exception": type(exc).__name__, "detail": str(exc)[:300]},
        )

    body_preview = (resp.text or "")[:500]
    logger.info(
        "scrapingbee response host=%s status=%s body_preview=%s",
        host,
        resp.status_code,
        body_preview.replace("\n", " ")[:300],
    )
    if resp.status_code in (401, 403):
        logger.warning("scrapingbee key rejected status=%s body=%s", resp.status_code, body_preview[:200])
        return _fail(
            safe_url,
            vendor,
            "ScrapingBee API key was rejected — check SCRAPINGBEE_API_KEY.",
            debug={"host": host, "provider_status": resp.status_code, "provider_body": body_preview},
        )
    if resp.status_code == 429:
        return _fail(
            safe_url,
            vendor,
            "Product extract quota exceeded. Try again later.",
            debug={"host": host, "provider_status": resp.status_code, "provider_body": body_preview},
        )
    if resp.status_code >= 400:
        logger.warning("scrapingbee extract failed status=%s host=%s body=%s", resp.status_code, host, body_preview[:300])
        return _fail(
            safe_url,
            vendor,
            "Vendor page could not be read. Try another product link.",
            debug={"host": host, "provider_status": resp.status_code, "provider_body": body_preview},
        )

    payload: Any
    try:
        payload = resp.json()
    except ValueError:
        logger.warning("scrapingbee non-json body host=%s", host)
        return _fail(
            safe_url,
            vendor,
            "Could not parse product details from the page.",
            debug={"host": host, "provider_status": resp.status_code, "provider_body": body_preview},
        )

    if not isinstance(payload, dict):
        return _fail(
            safe_url,
            vendor,
            "Could not parse product details from the page.",
            debug={"host": host, "provider_status": resp.status_code, "payload_type": type(payload).__name__},
        )

    title = _norm_str(payload.get("title"))
    if title and len(title) > 240:
        title = title[:237] + "…"
    price = parse_price_number(payload.get("price"))
    currency = _parse_currency(payload.get("currency"))
    image = normalize_image_url(payload.get("image"))
    available = payload.get("available")
    if not isinstance(available, bool):
        available = None

    if not title and price is None:
        return _fail(
            safe_url,
            vendor,
            "Could not extract product name or price from this page.",
            debug={"host": host, "provider_status": resp.status_code, "payload_keys": list(payload.keys())},
        )

    logger.info(
        "product_link_extract ok host=%s title=%s price=%s image=%s",
        host,
        (title or "")[:80],
        price,
        bool(image),
    )
    return {
        "ok": True,
        "configured": True,
        "title": title,
        "price": price,
        "currency": currency,
        "image": image,
        "available": available,
        "vendor": vendor,
        "link": safe_url,
        "error": None,
        "method": "scrapingbee",
        "debug": {"host": host, "provider_status": resp.status_code, "key_set": True},
    }


def _fail(link: str, vendor: str, error: str, *, debug: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "ok": False,
        "configured": True,
        "title": None,
        "price": None,
        "currency": "INR",
        "image": None,
        "available": None,
        "vendor": vendor,
        "link": link,
        "error": error,
        "method": "scrapingbee",
        "debug": debug or {},
    }
