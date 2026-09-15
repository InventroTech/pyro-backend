"""
Extract product name and price from a pasted storefront URL.

Uses ScrapingBee (same pattern as AfterShip for shipment tracking):
  SCRAPINGBEE_API_KEY in pyro-backend .env / Render env.

We never fetch the vendor page from Render. ScrapingBee renders it behind
residential/premium proxies so Cloudflare/WAF blocks do not apply.

Robu.in is behind Cloudflare. ScrapingBee returns the real product HTML, but
ai_extract_rules hallucinates a generic item (e.g. "Men's Performance T-Shirt").
For Robu we skip AI and parse title / price / image from the HTML ourselves.
"""

from __future__ import annotations

import html
import ipaddress
import json
import logging
import os
import re
import socket
import urllib.parse
from typing import Any, Dict, Iterable, Optional

import requests

logger = logging.getLogger(__name__)

SCRAPINGBEE_ENDPOINT = "https://app.scrapingbee.com/api/v1/"
DEFAULT_TIMEOUT = 40
ROBU_TIMEOUT = 55

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
    "gbraid",
    "wbraid",
    "gclsrc",
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
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_TAG_RE = re.compile(r"<[^>]+>")
_JSON_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.I | re.S,
)
_H1_PRODUCT_RE = re.compile(
    r'<h1\b[^>]*class=["\'][^"\']*product_title[^"\']*["\'][^>]*>(.*?)</h1>',
    re.I | re.S,
)
_OG_TITLE_SUFFIX_RE = re.compile(
    r"\s*[-|–—]\s*(?:Robu\.in|Robu)\b.*$",
    re.I,
)
_STOPWORDS = {"the", "and", "for", "with", "from", "you", "our", "new"}
# Only interstitial copy — not "challenge-platform", which Cloudflare leaves
# in scripts on successful product pages.
_CHALLENGE_MARKERS = (
    "just a moment",
    "attention required",
    "performing security verification",
    "enable javascript and cookies to continue",
)


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


def _is_tracking_query_key(key: str) -> bool:
    lk = (key or "").lower()
    if lk in _STRIP_QUERY_KEYS:
        return True
    return lk.startswith("utm_") or lk.startswith("gad_")


def _strip_tracking_query(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return url
    if not parsed.query:
        return url
    pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    kept = [(k, v) for (k, v) in pairs if not _is_tracking_query_key(k)]
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


def _canonical_host(host: str) -> str:
    host = (host or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def is_robu_host(host: str) -> bool:
    h = _canonical_host(host)
    return h == "robu.in" or h.endswith(".robu.in")


def vendor_from_host(host: str) -> str:
    host = _canonical_host(host)
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


def _strip_tags(value: str) -> str:
    return html.unescape(_TAG_RE.sub(" ", value)).replace("\xa0", " ")


def _clean_title(value: Any) -> Optional[str]:
    raw = _norm_str(value)
    if not raw:
        return None
    text = re.sub(r"\s+", " ", _strip_tags(raw)).strip()
    text = _OG_TITLE_SUFFIX_RE.sub("", text).strip(" -|–—")
    if not text:
        return None
    if len(text) > 240:
        text = text[:237] + "…"
    return text


def _tokens(text: str) -> set[str]:
    return {t for t in _TOKEN_RE.findall(text.lower()) if len(t) >= 3 and t not in _STOPWORDS}


def title_matches_product_slug(title: str, url: str) -> bool:
    """True when the title shares meaningful tokens with a /product/<slug> path."""
    path = urllib.parse.urlparse(url).path.rstrip("/")
    slug = path.rsplit("/", 1)[-1] if path else ""
    slug_tokens = _tokens(slug.replace("-", " "))
    if len(slug_tokens) < 2:
        return True
    overlap = slug_tokens & _tokens(title or "")
    return len(overlap) >= 2 or (len(overlap) / len(slug_tokens) >= 0.4)


def _looks_like_challenge(html_text: str) -> bool:
    """True only for an actual Cloudflare interstitial, not a loaded product page."""
    title_m = re.search(r"<title[^>]*>(.*?)</title>", html_text or "", re.I | re.S)
    title = re.sub(r"\s+", " ", _strip_tags(title_m.group(1))).lower() if title_m else ""
    if any(marker in title for marker in _CHALLENGE_MARKERS):
        return True
    head = (html_text or "")[:4000].lower()
    return any(marker in head for marker in _CHALLENGE_MARKERS)


def _meta_content(html_text: str, prop: str) -> Optional[str]:
    attr = re.escape(prop)
    pat1 = re.compile(
        rf'<meta\b[^>]*\b(?:property|name)=["\']{attr}["\'][^>]*\bcontent=["\']([^"\']+)["\']',
        re.I,
    )
    m = pat1.search(html_text)
    if m:
        return html.unescape(m.group(1)).strip() or None
    pat2 = re.compile(
        rf'<meta\b[^>]*\bcontent=["\']([^"\']+)["\'][^>]*\b(?:property|name)=["\']{attr}["\']',
        re.I,
    )
    m = pat2.search(html_text)
    if m:
        return html.unescape(m.group(1)).strip() or None
    return None


def _walk_json(obj: Any) -> Iterable[Any]:
    if isinstance(obj, list):
        for item in obj:
            yield from _walk_json(item)
        return
    if isinstance(obj, dict):
        yield obj
        graph = obj.get("@graph")
        if graph is not None:
            yield from _walk_json(graph)
        for key, value in obj.items():
            if key == "@graph":
                continue
            if isinstance(value, (dict, list)):
                yield from _walk_json(value)


def _is_product_type(value: Any) -> bool:
    types = value if isinstance(value, list) else [value]
    for item in types:
        text = str(item or "").lower()
        if text == "product" or text.endswith("/product"):
            return True
    return False


def _image_from_ld(value: Any) -> Optional[str]:
    if isinstance(value, list) and value:
        return _image_from_ld(value[0])
    if isinstance(value, dict):
        return _norm_str(value.get("url") or value.get("contentUrl"))
    return _norm_str(value)


def _offers_from_ld(product: Dict[str, Any]) -> Dict[str, Any]:
    offers = product.get("offers")
    if isinstance(offers, list) and offers:
        offers = offers[0]
    return offers if isinstance(offers, dict) else {}


def _product_from_json_ld(html_text: str) -> Dict[str, Any]:
    for match in _JSON_LD_RE.finditer(html_text):
        raw = match.group(1)
        raw = re.sub(r"<!--.*?-->", "", raw, flags=re.S).strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except ValueError:
            continue
        for node in _walk_json(payload):
            if isinstance(node, dict) and _is_product_type(node.get("@type")):
                return node
    return {}


def _parse_availability(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    text = str(value or "").lower().replace("_", "").replace("-", "").replace(" ", "")
    if not text:
        return None
    if "outofstock" in text or "soldout" in text or "unavailable" in text:
        return False
    if "instock" in text or text in {"available", "true"}:
        return True
    return None


def parse_storefront_html(html_text: str) -> Dict[str, Any]:
    """Pull title / price / image from WooCommerce JSON-LD, Open Graph, or H1."""
    product = _product_from_json_ld(html_text)
    offers = _offers_from_ld(product) if product else {}

    title = _clean_title(product.get("name")) if product else None
    if not title:
        h1 = _H1_PRODUCT_RE.search(html_text)
        if h1:
            title = _clean_title(h1.group(1))
    if not title:
        title = _clean_title(_meta_content(html_text, "og:title"))

    price = parse_price_number(offers.get("price") or offers.get("lowPrice"))
    if price is None:
        price = parse_price_number(_meta_content(html_text, "product:price:amount"))
    if price is None:
        price = parse_price_number(_meta_content(html_text, "og:price:amount"))

    currency = _parse_currency(
        offers.get("priceCurrency")
        or _meta_content(html_text, "product:price:currency")
        or _meta_content(html_text, "og:price:currency"),
        default="INR",
    )

    image = normalize_image_url(_image_from_ld(product.get("image")) if product else None)
    if not image:
        image = normalize_image_url(_meta_content(html_text, "og:image"))

    available = _parse_availability(offers.get("availability"))

    return {
        "title": title,
        "price": price,
        "currency": currency,
        "image": image,
        "available": available,
    }


def _scrapingbee_params(api_key: str, url: str, host: str) -> Dict[str, str]:
    params = {
        "api_key": api_key,
        "url": url,
        "render_js": "true",
        "premium_proxy": "true",
        "country_code": "in",
    }
    if is_robu_host(host):
        # Settings confirmed by ScrapingBee support. Do not send ai_extract_rules:
        # the HTML is correct; AI extraction invents a generic product.
        params["wait"] = "4000"
        params["wait_browser"] = "domcontentloaded"
        return params
    params["ai_extract_rules"] = json.dumps(_AI_EXTRACT_RULES, separators=(",", ":"))
    return params


def extract_product_from_url(url: str, *, pincode: Optional[str] = None) -> Dict[str, Any]:
    """Fetch structured product fields for a storefront URL via ScrapingBee."""
    del pincode  # unused; kept for API compatibility
    safe_url = assert_safe_product_url(url)
    host = (urllib.parse.urlparse(safe_url).hostname or "").lower()
    vendor = vendor_from_host(host)
    api_key = _scrapingbee_api_key()
    robu = is_robu_host(host)
    logger.info(
        "product_link_extract start host=%s vendor=%s key_set=%s extract=%s",
        host,
        vendor,
        bool(api_key),
        "html" if robu else "ai",
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
            "debug": {"key_set": False, "host": host, "extract": "html" if robu else "ai"},
        }

    params = _scrapingbee_params(api_key, safe_url, host)
    timeout = ROBU_TIMEOUT if robu else DEFAULT_TIMEOUT
    try:
        resp = requests.get(SCRAPINGBEE_ENDPOINT, params=params, timeout=timeout)
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

    if robu:
        return _result_from_robu_html(resp.text or "", safe_url, vendor, host, resp.status_code)

    return _result_from_ai_payload(resp, safe_url, vendor, host, body_preview)


def _result_from_robu_html(
    html_text: str,
    safe_url: str,
    vendor: str,
    host: str,
    status_code: int,
) -> Dict[str, Any]:
    parsed = parse_storefront_html(html_text)
    title = parsed.get("title")
    price = parsed.get("price")
    if not title and price is None and _looks_like_challenge(html_text):
        return _fail(
            safe_url,
            vendor,
            "Vendor page could not be read. Try another product link.",
            debug={"host": host, "provider_status": status_code, "extract": "html", "challenge": True},
        )
    if title and not title_matches_product_slug(title, safe_url):
        logger.warning(
            "product_link_extract slug mismatch host=%s title=%s url=%s",
            host,
            title[:80],
            safe_url,
        )
        return _fail(
            safe_url,
            vendor,
            "Could not extract product name or price from this page.",
            debug={
                "host": host,
                "provider_status": status_code,
                "extract": "html",
                "slug_mismatch": True,
                "title": title[:80],
            },
        )
    if not title and price is None:
        return _fail(
            safe_url,
            vendor,
            "Could not extract product name or price from this page.",
            debug={"host": host, "provider_status": status_code, "extract": "html"},
        )
    logger.info(
        "product_link_extract ok host=%s title=%s price=%s image=%s extract=html",
        host,
        (title or "")[:80],
        price,
        bool(parsed.get("image")),
    )
    return {
        "ok": True,
        "configured": True,
        "title": title,
        "price": price,
        "currency": "INR",
        "image": parsed.get("image"),
        "available": parsed.get("available"),
        "vendor": vendor,
        "link": safe_url,
        "error": None,
        "method": "scrapingbee",
        "debug": {"host": host, "provider_status": status_code, "key_set": True, "extract": "html"},
    }


def _result_from_ai_payload(
    resp: requests.Response,
    safe_url: str,
    vendor: str,
    host: str,
    body_preview: str,
) -> Dict[str, Any]:
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

    title = _clean_title(payload.get("title"))
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
        "product_link_extract ok host=%s title=%s price=%s image=%s extract=ai",
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
        "debug": {"host": host, "provider_status": resp.status_code, "key_set": True, "extract": "ai"},
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
