"""
Live e-commerce price comparison for inventory requestors.

Fetches current prices from Amazon.in, Robu.in, and Flipkart for a product query
and/or extracts prices from product page URLs.

Optional official Amazon Product Advertising API (PA-API 5) credentials:
  AMAZON_PAAPI_ACCESS_KEY
  AMAZON_PAAPI_SECRET_KEY
  AMAZON_PAAPI_PARTNER_TAG
  AMAZON_PAAPI_HOST          (default: webservices.amazon.in)
  AMAZON_PAAPI_REGION        (default: eu-west-1)
"""

from __future__ import annotations

import html as html_lib
import json
import logging
import os
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional

import requests

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT = 18
MAX_RESULTS_PER_SOURCE = 5

# Cap HTML fed to regex extractors to bound worst-case ReDoS cost (CodeQL).
_MAX_HTML_SCAN_CHARS = 1_500_000

# Bounded regexes — avoid unbounded [^>]* / .*? on attacker-controlled HTML.
_LD_JSON_SCRIPT_RE = re.compile(
    r'<script\b[^>]{0,400}?\btype\s*=\s*["\']application/ld\+json["\'][^>]{0,400}?>'
    r'([\s\S]{0,500000}?)</script\s*>',
    re.IGNORECASE,
)
_OG_CURRENCY_RE = re.compile(
    r'property\s*=\s*["\'](?:product:price:currency|og:price:currency)["\']'
    r'[^>]{0,300}?content\s*=\s*["\']([^"\']{1,32})["\']',
    re.IGNORECASE,
)
_OG_CURRENCY_RE_ALT = re.compile(
    r'content\s*=\s*["\']([^"\']{1,32})["\']'
    r'[^>]{0,300}?property\s*=\s*["\'](?:product:price:currency|og:price:currency)["\']',
    re.IGNORECASE,
)
# Titles rarely contain tags; reject '<' inside to keep matching linear.
_TITLE_RE = re.compile(
    r'<title\b[^>]{0,200}?>([^<]{0,2000})</title\s*>',
    re.IGNORECASE,
)


def _html_for_regex(html: str) -> str:
    """Return a length-capped HTML slice for regex scanning."""
    if not html:
        return ""
    if len(html) <= _MAX_HTML_SCAN_CHARS:
        return html
    return html[:_MAX_HTML_SCAN_CHARS]


def _extract_html_title(html: str) -> str:
    m = _TITLE_RE.search(_html_for_regex(html))
    if not m:
        return ""
    return html_lib.unescape(m.group(1).strip())


SUPPORTED_SOURCES = ("amazon", "robu", "flipkart")  # legacy; prefer vendor registry

try:
    from .price_compare_vendors import (  # type: ignore
        SUPPORTED_VENDOR_IDS,
        VENDOR_SPECS,
        detect_vendor_id,
        list_vendor_catalog,
        resolve_vendors,
        search_vendors,
    )
except Exception:  # pragma: no cover - allow partial imports in isolation
    SUPPORTED_VENDOR_IDS = SUPPORTED_SOURCES
    VENDOR_SPECS = []
    detect_vendor_id = None  # type: ignore
    list_vendor_catalog = None  # type: ignore
    resolve_vendors = None  # type: ignore
    search_vendors = None  # type: ignore


class PriceCompareError(Exception):
    """Raised for invalid price-compare requests."""


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-IN,en;q=0.9",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        }
    )
    return s


def normalize_indian_pincode(value: Any) -> Optional[str]:
    """Return a 6-digit Indian PIN code, or None if invalid."""
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 6 and digits[0] != "0":
        return digits
    return None


def _apply_amazon_pincode(sess: requests.Session, pincode: str) -> None:
    """Best-effort: set Amazon.in delivery location so search HTML includes local ETAs."""
    try:
        sess.get("https://www.amazon.in/", timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        sess.post(
            "https://www.amazon.in/gp/delivery/ajax/address-change.html",
            data={
                "locationType": "LOCATION_INPUT",
                "zipCode": pincode,
                "storeContext": "generic",
                "deviceType": "web",
                "pageType": "Gateway",
                "actionSource": "glow",
            },
            headers={
                "Referer": "https://www.amazon.in/",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            },
            timeout=DEFAULT_TIMEOUT,
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        logger.warning("Failed to set Amazon pincode=%s: %s", pincode, exc)


def _apply_flipkart_pincode(sess: requests.Session, pincode: str) -> None:
    """Best-effort: set Flipkart delivery PIN so search/product pages reflect local ETAs."""
    try:
        sess.get("https://www.flipkart.com/", timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        sess.post(
            "https://www.flipkart.com/api/6/user/location",
            json={"pincode": pincode},
            headers={
                "Referer": "https://www.flipkart.com/",
                "Origin": "https://www.flipkart.com",
                "Content-Type": "application/json",
                "Accept": "*/*",
                "X-User-Agent": f"{USER_AGENT} FKUA/website/42/website/Desktop",
            },
            timeout=DEFAULT_TIMEOUT,
            allow_redirects=True,
        )
        # Common cookies Flipkart reads for location-aware delivery text.
        sess.cookies.set("pincode", pincode, domain=".flipkart.com")
    except requests.RequestException as exc:
        logger.warning("Failed to set Flipkart pincode=%s: %s", pincode, exc)


def _parse_price_number(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw) if float(raw) >= 0 else None
    s = str(raw).strip()
    if not s:
        return None
    s = s.replace(",", "").replace("₹", "").replace("Rs.", "").replace("INR", "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    return val if val >= 0 else None


def _result(
    *,
    source: str,
    title: str,
    price: Optional[float],
    currency: str,
    link: str,
    available: bool = True,
    error: Optional[str] = None,
    method: str = "live",
    delivery_date: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "source": source,
        "title": (title or "").strip() or None,
        "price": price,
        "currency": currency or "INR",
        "link": link,
        "delivery_date": (delivery_date or "").strip() or None,
        "available": bool(available and price is not None and error is None),
        "error": error,
        "method": method,
    }


def _clean_delivery_text(raw: str) -> Optional[str]:
    """Normalize marketplace delivery/ETA strings, preferring the calendar date."""
    if not raw:
        return None

    text = re.sub(r"<[^>]+>", " ", str(raw))
    text = html_lib.unescape(text)
    text = re.sub(r"\s+", " ", text).strip(" \t\n\r-|,")
    if not text:
        return None

    preferred = _prefer_calendar_date(text)
    if preferred:
        return preferred

    # Fallback phrases (only when no date was found).
    for pat in (
        r"(Get it by[^.]{0,40})",
        r"(Delivery by[^.]{0,40})",
        r"(Arrives[^.]{0,40})",
        r"(Ships? (?:within|in)[^.]{0,40})",
        r"(Usually (?:dispatched|ships)[^.]{0,50})",
        r"(Dispatch(?:es|ed)? (?:in|within)[^.]{0,40})",
    ):
        m = re.search(pat, text, re.I)
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip(" \t\n\r-|,")

    if len(text) <= 60 and re.search(
        r"(delivery|deliver|ship|dispatch|tomorrow|today|\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b|\b\d{1,2}\b)",
        text,
        re.I,
    ):
        text = re.sub(r"^(?:FREE\s+)?delivery\s+", "", text, flags=re.I).strip()
        return text or None
    return None


_DATE_RE = re.compile(
    r"(?:"
    r"Tomorrow(?:\s*,?\s*\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*)?"
    r"|Today(?:\s*,?\s*\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*)?"
    r"|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*\.?,?\s+\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?"
    r"|\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?(?:\s+\d{4})?"
    r")",
    re.I,
)


def _prefer_calendar_date(text: str) -> Optional[str]:
    """Extract the delivery date itself (not 'FREE delivery ...')."""
    if not text:
        return None
    # Fastest option often more useful; keep primary date first if both exist.
    dates = [re.sub(r"\s+", " ", m.group(0)).strip(" ,") for m in _DATE_RE.finditer(text)]
    if not dates:
        return None
    # De-dupe while preserving order.
    uniq: List[str] = []
    for d in dates:
        if d not in uniq:
            uniq.append(d)
    if len(uniq) == 1:
        return uniq[0]
    # e.g. "Fri, 24 Jul (fastest: Tomorrow, 23 Jul)"
    return f"{uniq[0]} (fastest: {uniq[1]})"


def _format_delivery_day_range(min_days: int, max_days: int) -> str:
    """Convert handling+transit day counts into a human calendar range."""
    from datetime import date, timedelta

    today = date.today()
    start = today + timedelta(days=max(0, int(min_days)))
    end = today + timedelta(days=max(0, int(max_days)))

    def fmt(d: date) -> str:
        return d.strftime("%a, ") + str(d.day) + d.strftime(" %b")

    if start == end:
        return fmt(start)
    return f"{fmt(start)} – {fmt(end)}"


def _delivery_from_schema_delivery_time(delivery_time: Dict[str, Any]) -> Optional[str]:
    if not isinstance(delivery_time, dict):
        return None
    handling = delivery_time.get("handlingTime") if isinstance(delivery_time.get("handlingTime"), dict) else {}
    transit = delivery_time.get("transitTime") if isinstance(delivery_time.get("transitTime"), dict) else {}

    def _num(obj: Dict[str, Any], key: str, default: int = 0) -> int:
        try:
            return int(float(obj.get(key, default) or default))
        except (TypeError, ValueError):
            return default

    total_min = _num(handling, "minValue") + _num(transit, "minValue")
    total_max = _num(handling, "maxValue") + _num(transit, "maxValue")
    if total_max <= 0 and total_min <= 0:
        return None
    if total_max < total_min:
        total_max = total_min
    return _format_delivery_day_range(total_min, total_max)


def _extract_amazon_delivery(chunk: str) -> Optional[str]:
    # Prefer the dedicated delivery block (has bold date spans).
    block = re.search(
        r'data-cy="delivery-block"[^>]*>(.*?)</div>\s*</div>\s*</div>',
        chunk,
        re.I | re.S,
    )
    if block:
        cleaned = _clean_delivery_text(block.group(0))
        if cleaned:
            return cleaned

    recipe = re.search(r'data-cy="delivery-recipe"[^>]*>(.*?)</div>', chunk, re.I | re.S)
    if recipe:
        cleaned = _clean_delivery_text(recipe.group(0))
        if cleaned:
            return cleaned

    # Fallback: any date-looking text in the card.
    cleaned = _clean_delivery_text(chunk)
    if cleaned and _prefer_calendar_date(cleaned):
        return cleaned
    return cleaned


def _extract_flipkart_delivery(node: Dict[str, Any]) -> Optional[str]:
    """Pull delivery promise text from Flipkart product card JSON."""
    candidates: List[str] = []

    def collect(obj: Any, depth: int = 0) -> None:
        if depth > 4 or len(candidates) > 12:
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                key = str(k).lower()
                if any(tok in key for tok in ("deliver", "promise", "ship", "eta")):
                    if isinstance(v, str) and v.strip():
                        candidates.append(v.strip())
                    elif isinstance(v, dict):
                        for sk in ("text", "message", "label", "title", "value", "date"):
                            if isinstance(v.get(sk), str) and v.get(sk):
                                candidates.append(str(v.get(sk)))
                collect(v, depth + 1)
        elif isinstance(obj, list):
            for item in obj[:20]:
                collect(item, depth + 1)
        elif isinstance(obj, str) and re.search(r"deliver|tomorrow|get by", obj, re.I):
            candidates.append(obj)

    collect(node)
    for raw in candidates:
        cleaned = _clean_delivery_text(raw)
        if cleaned:
            return cleaned
    return None


def _robu_default_delivery_estimate() -> str:
    """
    Typical Robu.in OfferShippingDetails window when the product page is blocked.
    Matches common JSON-LD on Robu: handling 0–1 day + transit 2–7 days.
    """
    return _format_delivery_day_range(2, 8)


def _extract_robu_delivery(product: Dict[str, Any], page_html: str = "") -> Optional[str]:
    for key, value in product.items():
        if any(tok in str(key).lower() for tok in ("deliver", "ship", "dispatch", "eta")):
            if isinstance(value, (str, int, float)) and str(value).strip():
                cleaned = _clean_delivery_text(str(value))
                if cleaned:
                    return cleaned

    if page_html:
        # Schema.org OfferShippingDetails.deliveryTime on Robu product pages.
        for block in _extract_json_ld_blocks(page_html):
            for node in _walk_json(block):
                if not isinstance(node, dict):
                    continue
                delivery_time = node.get("deliveryTime")
                if isinstance(delivery_time, dict):
                    estimated = _delivery_from_schema_delivery_time(delivery_time)
                    if estimated:
                        return estimated
                shipping = node.get("shippingDetails")
                if isinstance(shipping, dict) and isinstance(shipping.get("deliveryTime"), dict):
                    estimated = _delivery_from_schema_delivery_time(shipping["deliveryTime"])
                    if estimated:
                        return estimated

        for pat in (
            r"(Usually (?:dispatched|ships)[^<.]{0,50})",
            r"(Ships? (?:within|in)[^<.]{0,40})",
            r"(Dispatch(?:es)? (?:in|within)[^<.]{0,40})",
            r"(Delivery (?:in|within|by)[^<.]{0,40})",
            r"(Estimated delivery[^<.]{0,50})",
        ):
            m = re.search(pat, page_html, re.I)
            if m:
                cleaned = _clean_delivery_text(m.group(1))
                if cleaned:
                    return cleaned
    return None


def _extract_json_ld_blocks(html: str) -> List[Any]:
    blocks: List[Any] = []
    scanned = _html_for_regex(html)
    for m in _LD_JSON_SCRIPT_RE.finditer(scanned):
        raw = m.group(1).strip()
        if not raw:
            continue
        try:
            blocks.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return blocks


def _walk_json(obj: Any) -> Iterable[Any]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_json(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_json(v)


def _product_from_json_ld(blocks: List[Any]) -> Optional[Dict[str, Any]]:
    for block in blocks:
        for node in _walk_json(block):
            if not isinstance(node, dict):
                continue
            types = node.get("@type")
            type_list = types if isinstance(types, list) else [types]
            type_list = [str(t) for t in type_list if t]
            if not any(t.endswith("Product") for t in type_list):
                continue
            title = str(node.get("name") or "").strip()
            offers = node.get("offers")
            offer_list = offers if isinstance(offers, list) else [offers] if offers else []
            for offer in offer_list:
                if not isinstance(offer, dict):
                    continue
                price = _parse_price_number(offer.get("price") or offer.get("lowPrice"))
                currency = str(offer.get("priceCurrency") or "INR").upper()
                link = str(offer.get("url") or node.get("url") or "").strip()
                availability = str(offer.get("availability") or "")
                available = "OutOfStock" not in availability
                if price is not None:
                    return {
                        "title": title,
                        "price": price,
                        "currency": currency,
                        "link": link,
                        "available": available,
                    }
    return None


def extract_price_from_html(html: str, fallback_url: str = "") -> Optional[Dict[str, Any]]:
    """Extract a product price from HTML (JSON-LD first, then common meta tags)."""
    product = _product_from_json_ld(_extract_json_ld_blocks(html))
    if product:
        if not product.get("link") and fallback_url:
            product["link"] = fallback_url
        return product

    scanned = _html_for_regex(html)
    meta_patterns = [
        r'<meta[^>]{1,400}?property=["\']product:price:amount["\'][^>]{1,400}?content=["\']([^"\']{1,64})["\']',
        r'<meta[^>]{1,400}?content=["\']([^"\']{1,64})["\'][^>]{1,400}?property=["\']product:price:amount["\']',
        r'<meta[^>]{1,400}?property=["\']og:price:amount["\'][^>]{1,400}?content=["\']([^"\']{1,64})["\']',
        r'<meta[^>]{1,400}?itemprop=["\']price["\'][^>]{1,400}?content=["\']([^"\']{1,64})["\']',
        r'"price"\s*:\s*"?(?P<p>[\d.]{1,20})"?',
    ]
    for pat in meta_patterns:
        m = re.search(pat, scanned, re.I)
        if not m:
            continue
        price = _parse_price_number(m.groupdict().get("p") or m.group(1))
        if price is None:
            continue
        currency = "INR"
        cur_m = _OG_CURRENCY_RE.search(scanned) or _OG_CURRENCY_RE_ALT.search(scanned)
        if cur_m:
            currency = cur_m.group(1).upper()
        title = _extract_html_title(scanned)
        return {
            "title": title,
            "price": price,
            "currency": currency,
            "link": fallback_url,
            "available": True,
        }
    return None


def _fetch_html(url: str, headers: Optional[Dict[str, str]] = None) -> str:
    """
    Fetch HTML with requests, falling back to urllib on 403.
    Some storefronts (notably Robu) intermittently block the requests client.
    """
    import urllib.request
    import ssl
    import time

    sess = _session()
    req_headers = dict(headers or {})
    last_exc: Optional[Exception] = None

    # Warm cookies for Robu — bare product hits are often 403 without a prior home visit.
    if "robu.in" in (url or "").lower():
        try:
            sess.get("https://robu.in/", timeout=min(8, DEFAULT_TIMEOUT), allow_redirects=True)
        except requests.RequestException:
            pass

    for attempt in range(2):
        try:
            resp = sess.get(url, headers=req_headers, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
            if resp.status_code != 403:
                resp.raise_for_status()
                return resp.text
            last_exc = requests.HTTPError(f"403 for {url}", response=resp)
        except requests.RequestException as exc:
            last_exc = exc
        if attempt == 0:
            time.sleep(0.35)

    ua_headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Accept-Encoding": "identity",
        **req_headers,
    }
    # Prefer cookie-aware urllib opener for Robu.
    try:
        import http.cookiejar

        cj = http.cookiejar.CookieJar()
        ctx = ssl.create_default_context()
        opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(cj),
            urllib.request.HTTPSHandler(context=ctx),
        )
        if "robu.in" in (url or "").lower():
            home_req = urllib.request.Request("https://robu.in/", headers=ua_headers)
            try:
                opener.open(home_req, timeout=DEFAULT_TIMEOUT).read()
            except Exception:
                pass
        req = urllib.request.Request(url, headers=ua_headers)
        with opener.open(req, timeout=DEFAULT_TIMEOUT) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except Exception as urllib_exc:
        if last_exc:
            raise last_exc from urllib_exc
        raise


def fetch_url_price(url: str, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """Fetch a single product URL and return a normalized price result."""
    source = detect_source(url)
    headers: Dict[str, str] = {}
    if source == "robu":
        headers["Referer"] = "https://robu.in/"
        if not url.rstrip("/").endswith(".html") and "?" not in url:
            url = url.rstrip("/") + "/"
    elif source == "flipkart":
        headers["Referer"] = "https://www.flipkart.com/"
    elif source == "amazon":
        headers["Referer"] = "https://www.amazon.in/"

    try:
        html = _fetch_html(url, headers=headers)
        final_url = url
    except Exception as exc:
        logger.warning("price_compare fetch failed url=%s err=%s", url, exc)
        return _result(
            source=source or "other",
            title="",
            price=None,
            currency="INR",
            link=url,
            available=False,
            error=f"Failed to fetch URL: {exc}",
            method="url",
        )

    extracted = extract_price_from_html(html, fallback_url=final_url)
    if not extracted:
        # Amazon product pages sometimes omit JSON-LD; try a-price markers.
        whole = re.search(r"a-price-whole[^>]*>([\d,]+)", html)
        frac = re.search(r"a-price-fraction[^>]*>(\d+)", html)
        if whole:
            price_s = whole.group(1).replace(",", "")
            if frac:
                price_s = f"{price_s}.{frac.group(1)}"
            title = _extract_html_title(html)
            extracted = {
                "title": title,
                "price": _parse_price_number(price_s),
                "currency": "INR",
                "link": final_url,
                "available": True,
            }

    if not extracted or extracted.get("price") is None:
        return _result(
            source=source or "other",
            title="",
            price=None,
            currency="INR",
            link=url,
            available=False,
            error="Could not extract price from page",
            method="url",
        )

    delivery = None
    if source == "amazon":
        delivery = _extract_amazon_delivery(html)
    elif source == "robu":
        delivery = _extract_robu_delivery({}, html)
    elif source == "flipkart":
        delivery = _clean_delivery_text(
            next(iter(re.findall(r"(Delivery by[^<.]{0,40})", html, re.I)), "")
        )

    return _result(
        source=source or "other",
        title=str(extracted.get("title") or ""),
        price=extracted.get("price"),
        currency=str(extracted.get("currency") or "INR"),
        link=str(extracted.get("link") or url),
        available=bool(extracted.get("available", True)),
        delivery_date=delivery,
        method="url",
    )


def detect_source(url: str) -> str:
    if detect_vendor_id is not None:
        return detect_vendor_id(url)
    try:
        host = urllib.parse.urlparse(url).hostname or ""
    except Exception:
        return "other"
    host = host.lower()
    if "amazon." in host:
        return "amazon"
    if "robu.in" in host:
        return "robu"
    if "flipkart.com" in host:
        return "flipkart"
    return "other"


def _amazon_paapi_configured() -> bool:
    return bool(
        os.getenv("AMAZON_PAAPI_ACCESS_KEY")
        and os.getenv("AMAZON_PAAPI_SECRET_KEY")
        and os.getenv("AMAZON_PAAPI_PARTNER_TAG")
    )


def search_amazon(
    query: str,
    session: Optional[requests.Session] = None,
    pincode: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Search Amazon.in live HTML (PA-API used when credentials are configured)."""
    if _amazon_paapi_configured():
        try:
            paapi_results = _search_amazon_paapi(query)
            if paapi_results:
                return paapi_results
        except Exception as exc:
            logger.warning("Amazon PA-API search failed, falling back to HTML: %s", exc)

    sess = session or _session()
    pin = normalize_indian_pincode(pincode)
    if pin:
        _apply_amazon_pincode(sess, pin)
    url = f"https://www.amazon.in/s?k={urllib.parse.quote_plus(query)}"
    try:
        resp = sess.get(
            url,
            headers={"Referer": "https://www.amazon.in/"},
            timeout=DEFAULT_TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        return [
            _result(
                source="amazon",
                title="",
                price=None,
                currency="INR",
                link=url,
                available=False,
                error=f"Amazon search failed: {exc}",
            )
        ]

    html = resp.text
    if "enter the characters you see" in html.lower() or "/errors/validateCaptcha" in html:
        return [
            _result(
                source="amazon",
                title="",
                price=None,
                currency="INR",
                link=url,
                available=False,
                error="Amazon blocked the request (captcha). Configure AMAZON_PAAPI_* keys for reliable results.",
            )
        ]

    results: List[Dict[str, Any]] = []
    pattern = re.compile(
        r'<div[^>]*data-asin="([A-Z0-9]{10})"[^>]*data-component-type="s-search-result"[^>]*>(.*?)'
        r'(?=<div[^>]*data-asin="[A-Z0-9]{10}"[^>]*data-component-type="s-search-result"|$)',
        re.S,
    )
    for m in pattern.finditer(html):
        asin, chunk = m.group(1), m.group(2)
        if re.search(r">\s*Sponsored\s*<", chunk, re.I):
            continue
        price_m = re.search(r"a-price-whole[^>]*>([\d,]+)", chunk)
        if not price_m:
            continue
        frac_m = re.search(r"a-price-fraction[^>]*>(\d+)", chunk)
        price_s = price_m.group(1).replace(",", "")
        if frac_m:
            price_s = f"{price_s}.{frac_m.group(1)}"
        price = _parse_price_number(price_s)
        if price is None:
            continue

        title = ""
        for title_pat in (
            r'<h2[^>]*aria-label=["\']([^"\']+)["\']',
            r'<h2[^>]*>.*?<span[^>]*class="[^"]*a-text-normal[^"]*"[^>]*>(.*?)</span>',
            r"<h2[^>]*>.*?<span[^>]*>(.*?)</span>",
        ):
            tm = re.search(title_pat, chunk, re.S | re.I)
            if tm:
                title = re.sub(r"<[^>]+>", "", tm.group(1)).strip()
                title = html_lib.unescape(title)
                if title:
                    break

        link_m = re.search(rf'href="(/[^"]*/dp/{asin}[^"]*)"', chunk)
        if link_m:
            path = html_lib.unescape(link_m.group(1)).split("?")[0]
            link = "https://www.amazon.in" + path
        else:
            link = f"https://www.amazon.in/dp/{asin}"

        results.append(
            _result(
                source="amazon",
                title=title or f"Amazon product {asin}",
                price=price,
                currency="INR",
                link=link,
                delivery_date=_extract_amazon_delivery(chunk),
                method="live_html",
            )
        )
        if len(results) >= MAX_RESULTS_PER_SOURCE:
            break

    if not results:
        return [
            _result(
                source="amazon",
                title="",
                price=None,
                currency="INR",
                link=url,
                available=False,
                error="No priced Amazon results found",
            )
        ]
    return results


def _search_amazon_paapi(query: str) -> List[Dict[str, Any]]:
    """
    Minimal Amazon PA-API 5 SearchItems call.
    Requires AMAZON_PAAPI_ACCESS_KEY / SECRET_KEY / PARTNER_TAG.
    """
    # Lazy import-free implementation via signed REST is complex; use official SDK if installed.
    try:
        from amazon_paapi import AmazonApi  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "amazon-paapi package not installed; set HTML fallback or install amazon-paapi"
        ) from exc

    access_key = os.environ["AMAZON_PAAPI_ACCESS_KEY"]
    secret_key = os.environ["AMAZON_PAAPI_SECRET_KEY"]
    partner_tag = os.environ["AMAZON_PAAPI_PARTNER_TAG"]
    country = os.getenv("AMAZON_PAAPI_COUNTRY", "IN")
    amazon = AmazonApi(access_key, secret_key, partner_tag, country)
    search = amazon.search_items(keywords=query, item_count=MAX_RESULTS_PER_SOURCE)
    items = getattr(search, "items", None) or []
    out: List[Dict[str, Any]] = []
    for item in items:
        title = getattr(getattr(item, "item_info", None), "title", None)
        title_s = getattr(title, "display_value", None) or ""
        link = getattr(item, "detail_page_url", None) or ""
        listings = getattr(getattr(item, "offers", None), "listings", None) or []
        price = None
        currency = "INR"
        if listings:
            price_obj = getattr(listings[0], "price", None)
            if price_obj is not None:
                price = _parse_price_number(getattr(price_obj, "amount", None))
                currency = str(getattr(price_obj, "currency", None) or "INR")
        if price is None:
            continue
        out.append(
            _result(
                source="amazon",
                title=str(title_s),
                price=price,
                currency=currency,
                link=str(link),
                method="paapi",
            )
        )
    return out


def search_robu(query: str, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    """
    Search Robu via their public JSON search API:
      POST https://robu.in/api/search/  {"q": "<query>"}
    """
    sess = session or _session()
    api_url = "https://robu.in/api/search/"
    search_link = f"https://robu.in/shop?search={urllib.parse.quote_plus(query)}"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://robu.in",
        "Referer": search_link,
    }

    payload: Any = None
    try:
        resp = sess.post(
            api_url,
            headers=headers,
            json={"q": query},
            timeout=DEFAULT_TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()
        payload = resp.json()
    except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
        # Fallback: stdlib POST (some environments block the requests client).
        try:
            import ssl
            import urllib.request as urllib_request

            body = json.dumps({"q": query}).encode("utf-8")
            req = urllib_request.Request(
                api_url,
                data=body,
                headers={
                    "User-Agent": USER_AGENT,
                    **headers,
                    "Accept-Encoding": "identity",
                },
                method="POST",
            )
            ctx = ssl.create_default_context()
            with urllib_request.urlopen(req, timeout=DEFAULT_TIMEOUT, context=ctx) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        except Exception as fallback_exc:
            logger.warning("Robu search API failed: %s / %s", exc, fallback_exc)
            return [
                _result(
                    source="robu",
                    title="",
                    price=None,
                    currency="INR",
                    link=search_link,
                    available=False,
                    error=f"Robu search failed: {fallback_exc}",
                )
            ]

    products = []
    if isinstance(payload, dict):
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        products = data.get("products") if isinstance(data, dict) else []
    if not isinstance(products, list):
        products = []

    results: List[Dict[str, Any]] = []
    for product in products:
        if not isinstance(product, dict):
            continue
        title = str(product.get("name") or "").strip()
        slug = str(product.get("slug") or "").strip().strip("/")
        link = f"https://robu.in/product/{slug}/" if slug else search_link
        price = (
            _parse_price_number(product.get("sale_price"))
            or _parse_price_number(product.get("special_price"))
            or _parse_price_number(product.get("price"))
            or _parse_price_number(product.get("regular_price"))
        )
        if price is None:
            continue
        delivery = _extract_robu_delivery(product)
        results.append(
            {
                "_product": product,
                "_link": link,
                "result": _result(
                    source="robu",
                    title=title or "Robu product",
                    price=price,
                    currency="INR",
                    link=link,
                    delivery_date=delivery,
                    method="robu_api",
                ),
            }
        )
        if len(results) >= MAX_RESULTS_PER_SOURCE:
            break

    # Enrich missing Robu delivery estimates from product pages (best-effort).
    missing = [row for row in results if not row["result"].get("delivery_date")]
    if missing:
        with ThreadPoolExecutor(max_workers=min(3, len(missing))) as pool:
            fut_map = {
                pool.submit(_fetch_html, row["_link"], {"Referer": "https://robu.in/"}): row
                for row in missing
            }
            for fut in as_completed(fut_map):
                row = fut_map[fut]
                try:
                    page_html = fut.result()
                except Exception:
                    continue
                delivery = _extract_robu_delivery(row["_product"], page_html)
                if delivery:
                    row["result"]["delivery_date"] = delivery

    # Robu product pages are often blocked; reuse any scraped store ETA, else default window.
    scraped_eta = next(
        (row["result"].get("delivery_date") for row in results if row["result"].get("delivery_date")),
        None,
    )
    fallback_eta = scraped_eta or _robu_default_delivery_estimate()
    for row in results:
        if not row["result"].get("delivery_date"):
            row["result"]["delivery_date"] = fallback_eta

    final_results = [row["result"] for row in results]
    if not final_results:
        return [
            _result(
                source="robu",
                title="",
                price=None,
                currency="INR",
                link=search_link,
                available=False,
                error="No Robu products found for this query",
            )
        ]
    final_results.sort(key=lambda r: float(r.get("price") or 0))
    return final_results


def search_flipkart(
    query: str,
    session: Optional[requests.Session] = None,
    pincode: Optional[str] = None,
) -> List[Dict[str, Any]]:
    sess = session or _session()
    pin = normalize_indian_pincode(pincode)
    if pin:
        _apply_flipkart_pincode(sess, pin)
    url = f"https://www.flipkart.com/search?q={urllib.parse.quote_plus(query)}"
    if pin:
        url = f"{url}&pincode={pin}"
    try:
        resp = sess.get(
            url,
            headers={"Referer": "https://www.flipkart.com/"},
            timeout=DEFAULT_TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        return [
            _result(
                source="flipkart",
                title="",
                price=None,
                currency="INR",
                link=url,
                available=False,
                error=f"Flipkart search failed: {exc}",
            )
        ]

    html = resp.text
    results: List[Dict[str, Any]] = []
    m = re.search(r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});?\s*</script>", html, re.S)
    if m:
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            data = None
        if data is not None:
            seen = set()
            for node in _walk_json(data):
                if not isinstance(node, dict):
                    continue
                if "pricing" not in node or "titles" not in node or "baseUrl" not in node:
                    continue
                pricing = node.get("pricing") or {}
                prices = pricing.get("prices") if isinstance(pricing, dict) else None
                price = None
                if isinstance(prices, list):
                    for p in prices:
                        if isinstance(p, dict) and not p.get("strikeOff"):
                            price = _parse_price_number(p.get("value"))
                            if price is not None:
                                break
                titles = node.get("titles") or {}
                title = ""
                if isinstance(titles, dict):
                    title = str(titles.get("title") or titles.get("newTitle") or "").strip()
                base = str(node.get("baseUrl") or "").strip()
                if not base or price is None:
                    continue
                link = "https://www.flipkart.com" + base if base.startswith("/") else base
                if link in seen:
                    continue
                seen.add(link)
                results.append(
                    _result(
                        source="flipkart",
                        title=title or "Flipkart product",
                        price=price,
                        currency="INR",
                        link=link.split("&")[0],
                        delivery_date=_extract_flipkart_delivery(node),
                        method="live_html",
                    )
                )
                if len(results) >= MAX_RESULTS_PER_SOURCE:
                    break

    if not results:
        return [
            _result(
                source="flipkart",
                title="",
                price=None,
                currency="INR",
                link=url,
                available=False,
                error="No priced Flipkart results found",
            )
        ]
    return results


def compare_prices(
    *,
    query: str = "",
    sources: Optional[List[str]] = None,
    urls: Optional[List[str]] = None,
    pincode: Optional[str] = None,
    profile: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compare live prices across configured vendor sites.

    Provide `query` to search marketplaces and/or `urls` to extract prices from product pages.
    Optional `pincode` (Indian 6-digit PIN) improves Amazon delivery dates.
    Optional `profile` (`core`|`extended`) selects the default vendor set when `sources` is omitted.
    """
    query = (query or "").strip()
    urls = [u.strip() for u in (urls or []) if u and str(u).strip()]
    if not query and not urls:
        raise PriceCompareError("Provide a product query and/or at least one product URL.")

    pin = normalize_indian_pincode(pincode)

    resolved_specs = []
    if resolve_vendors is not None:
        resolved_specs = resolve_vendors(sources=sources, profile=profile)
    normalized_sources = [s.id for s in resolved_specs]

    # Allow legacy Flipkart when explicitly requested.
    leftover: List[str] = []
    if sources:
        for raw in sources:
            key = str(raw or "").strip().lower()
            if key == "flipkart" and key not in normalized_sources:
                leftover.append("flipkart")

    results: List[Dict[str, Any]] = []
    errors: List[str] = []

    url_sess = _session()
    if pin:
        if any("amazon." in (u or "").lower() for u in urls):
            _apply_amazon_pincode(url_sess, pin)
        if any("flipkart." in (u or "").lower() for u in urls):
            _apply_flipkart_pincode(url_sess, pin)
    for u in urls[:10]:
        results.append(fetch_url_price(u, url_sess))

    if query and (resolved_specs or leftover):
        if resolved_specs and search_vendors is not None:
            vendor_rows, vendor_errors = search_vendors(
                query,
                [s.id for s in resolved_specs],
                profile=profile,
                pincode=pin,
                amazon_search=search_amazon,
                robu_search=search_robu,
            )
            results.extend(vendor_rows)
            errors.extend(vendor_errors)

        if leftover:
            with ThreadPoolExecutor(max_workers=len(leftover)) as pool:
                fut_map = {}
                if "flipkart" in leftover:
                    fut_map[pool.submit(search_flipkart, query, None, pin)] = "flipkart"
                for fut in as_completed(fut_map):
                    source = fut_map[fut]
                    try:
                        results.extend(fut.result())
                    except Exception as exc:
                        logger.exception("price_compare source failed source=%s", source)
                        errors.append(f"{source}: {exc}")
                        results.append(
                            _result(
                                source=source,
                                title="",
                                price=None,
                                currency="INR",
                                link="",
                                available=False,
                                error=str(exc),
                            )
                        )

    priced = [r for r in results if r.get("price") is not None]
    cheapest = None
    if priced:
        cheapest = min(priced, key=lambda r: float(r["price"]))

    catalog = list_vendor_catalog() if list_vendor_catalog is not None else []
    return {
        "query": query or None,
        "sources": normalized_sources + leftover,
        "profile": (profile or "").strip().lower() or None,
        "pincode": pin,
        "results": results,
        "cheapest": cheapest,
        "errors": errors,
        "amazon_paapi_configured": _amazon_paapi_configured(),
        "vendors": catalog,
    }
