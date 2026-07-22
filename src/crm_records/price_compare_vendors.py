"""
Config-driven multi-vendor live price search.

Architecture:
  - Catalog: price_compare_vendors.json (add/remove sites without code changes)
  - Strategies: amazon | robu | shopify | wc_store | html
  - Profiles:
      core     -> small reliable default set
      extended -> all enabled catalog vendors
  - Env overrides: PRICE_COMPARE_PROFILE, PRICE_COMPARE_VENDORS
  - Frontend loads vendor labels from GET /crm-records/price-compare/vendors/
"""

from __future__ import annotations

import html as html_lib
import json
import logging
import os
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

import requests

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).with_name("price_compare_vendors.json")

PRICE_NEAR_RE = re.compile(
    r"(?:&#8377;|₹|&rupee;|Rs\.\s*|INR\s*|रo\s*)\s*([\d,]+(?:\.\d+)?)",
    re.I,
)

BAD_TITLES = {
    "out of stock",
    "read more",
    "add to cart",
    "sale",
    "select options",
    "quick view",
    "my account",
    "contact us",
    "best sellers",
    "client portal",
    "cart",
    "checkout",
    "home",
    "shop",
}


@dataclass(frozen=True)
class VendorSpec:
    id: str
    label: str
    hosts: Sequence[str]
    strategy: str
    base_url: str = ""
    search_url: str = ""
    vendor_name: str = ""
    enabled: bool = True
    profile: str = "extended"

    @property
    def method(self) -> str:
        return self.strategy


def _as_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@lru_cache(maxsize=1)
def _load_config() -> Dict[str, Any]:
    try:
        with _CONFIG_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            raise ValueError("vendor config root must be an object")
        return data
    except Exception as exc:
        logger.exception("Failed to load price compare vendor config: %s", exc)
        return {
            "defaults": {
                "profile": "core",
                "max_results_per_vendor": 3,
                "timeout_seconds": 10,
                "max_workers": 8,
            },
            "vendors": [
                {
                    "id": "amazon",
                    "label": "Amazon",
                    "vendor_name": "AMAZON",
                    "hosts": ["amazon."],
                    "strategy": "amazon",
                    "base_url": "https://www.amazon.in",
                    "enabled": True,
                    "profile": "core",
                },
                {
                    "id": "robu",
                    "label": "Robu",
                    "vendor_name": "ROBU",
                    "hosts": ["robu.in"],
                    "strategy": "robu",
                    "base_url": "https://robu.in",
                    "enabled": True,
                    "profile": "core",
                },
            ],
        }


def get_runtime_defaults() -> Dict[str, Any]:
    cfg = _load_config().get("defaults") or {}
    profile = (cfg.get("profile") or "core")
    try:
        from django.conf import settings as dj_settings

        profile = getattr(dj_settings, "PRICE_COMPARE_PROFILE", None) or profile
    except Exception:
        pass
    profile = (os.getenv("PRICE_COMPARE_PROFILE") or profile or "core").strip().lower()
    if profile not in {"core", "extended"}:
        profile = "core"
    return {
        "profile": profile,
        "max_results_per_vendor": int(cfg.get("max_results_per_vendor") or 3),
        "timeout_seconds": int(cfg.get("timeout_seconds") or 10),
        "max_workers": int(cfg.get("max_workers") or 8),
    }


def _parse_vendor(row: Dict[str, Any]) -> Optional[VendorSpec]:
    vid = str(row.get("id") or "").strip().lower()
    if not vid:
        return None
    strategy = str(row.get("strategy") or row.get("method") or "html").strip().lower()
    hosts = tuple(str(h).strip().lower() for h in (row.get("hosts") or []) if str(h).strip())
    profile = str(row.get("profile") or "extended").strip().lower()
    if profile not in {"core", "extended"}:
        profile = "extended"
    return VendorSpec(
        id=vid,
        label=str(row.get("label") or vid).strip(),
        hosts=hosts,
        strategy=strategy,
        base_url=str(row.get("base_url") or "").strip(),
        search_url=str(row.get("search_url") or "").strip(),
        vendor_name=str(row.get("vendor_name") or "").strip(),
        enabled=_as_bool(row.get("enabled"), True),
        profile=profile,
    )


@lru_cache(maxsize=1)
def get_all_vendors() -> tuple:
    rows = _load_config().get("vendors") or []
    out: List[VendorSpec] = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        spec = _parse_vendor(row)
        if not spec or spec.id in seen:
            continue
        seen.add(spec.id)
        out.append(spec)
    return tuple(out)


def get_vendor_map() -> Dict[str, VendorSpec]:
    return {v.id: v for v in get_all_vendors()}


def list_vendor_catalog(*, include_disabled: bool = False) -> List[Dict[str, Any]]:
    rows = []
    for v in get_all_vendors():
        if not include_disabled and not v.enabled:
            continue
        rows.append(
            {
                "id": v.id,
                "label": v.label,
                "vendor_name": v.vendor_name or v.label.upper().replace(" ", ""),
                "hosts": list(v.hosts),
                "strategy": v.strategy,
                "base_url": v.base_url,
                "enabled": v.enabled,
                "profile": v.profile,
            }
        )
    return rows


def resolve_vendors(
    *,
    sources: Optional[Sequence[str]] = None,
    profile: Optional[str] = None,
) -> List[VendorSpec]:
    vendor_map = get_vendor_map()
    enabled = [v for v in get_all_vendors() if v.enabled]

    if sources:
        resolved: List[VendorSpec] = []
        for raw in sources:
            key = str(raw or "").strip().lower()
            spec = vendor_map.get(key)
            if spec and spec.enabled and spec not in resolved:
                resolved.append(spec)
        return resolved

    env_ids = os.getenv("PRICE_COMPARE_VENDORS", "").strip()
    if env_ids:
        resolved = []
        for raw in env_ids.split(","):
            key = raw.strip().lower()
            spec = vendor_map.get(key)
            if spec and spec.enabled and spec not in resolved:
                resolved.append(spec)
        if resolved:
            return resolved

    selected_profile = (profile or get_runtime_defaults()["profile"]).strip().lower()
    if selected_profile not in {"core", "extended"}:
        selected_profile = "core"
    if selected_profile == "extended":
        return enabled
    return [v for v in enabled if v.profile == "core"]


_defaults = get_runtime_defaults()
MAX_RESULTS_PER_VENDOR = _defaults["max_results_per_vendor"]
VENDOR_TIMEOUT = _defaults["timeout_seconds"]

VENDOR_SPECS: List[VendorSpec] = list(get_all_vendors())
SUPPORTED_VENDOR_IDS = tuple(v.id for v in VENDOR_SPECS)
_VENDOR_BY_ID = {v.id: v for v in VENDOR_SPECS}


def detect_vendor_id(url: str) -> str:
    try:
        host = (urllib.parse.urlparse(url).hostname or "").lower()
    except Exception:
        return "other"
    for spec in get_all_vendors():
        if any(h in host for h in spec.hosts):
            return spec.id
    if "flipkart.com" in host:
        return "flipkart"
    return "other"


def _parse_price_number(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        val = float(raw)
        return val if val >= 0 else None
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


def _mk_result(
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


def _empty(spec: VendorSpec, search_link: str, error: str) -> List[Dict[str, Any]]:
    return [
        _mk_result(
            source=spec.id,
            title="",
            price=None,
            currency="INR",
            link=search_link,
            available=False,
            error=error,
            method=spec.strategy,
        )
    ]


def _get_text(
    url: str,
    *,
    session: Optional[requests.Session] = None,
    headers: Optional[Dict[str, str]] = None,
    prefer_json: bool = False,
) -> str:
    """Fetch URL text via requests, falling back to urllib on 403 only."""
    from .price_compare import USER_AGENT, _fetch_html, _session

    sess = session or _session()
    req_headers = {
        "Accept": "application/json, text/plain, */*" if prefer_json else "text/html,application/xhtml+xml,*/*;q=0.8",
        **(headers or {}),
    }
    try:
        resp = sess.get(url, headers=req_headers, timeout=VENDOR_TIMEOUT, allow_redirects=True)
        if resp.status_code == 403:
            raise requests.HTTPError("403", response=resp)
        resp.raise_for_status()
        return resp.text
    except requests.HTTPError as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 403:
            return _fetch_html(url, headers={**req_headers, "User-Agent": USER_AGENT})
        raise
    except requests.RequestException:
        return _fetch_html(url, headers={**req_headers, "User-Agent": USER_AGENT})


def _search_shopify(spec: VendorSpec, query: str, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    q = urllib.parse.quote(query)
    url = (
        f"{spec.base_url.rstrip('/')}/search/suggest.json"
        f"?q={q}&resources[type]=product&resources[limit]={MAX_RESULTS_PER_VENDOR}"
    )
    search_link = f"{spec.base_url.rstrip('/')}/search?q={urllib.parse.quote_plus(query)}"
    try:
        raw = _get_text(
            url,
            session=session,
            headers={"Referer": spec.base_url, "Accept": "application/json"},
            prefer_json=True,
        )
        data = json.loads(raw)
    except Exception as exc:
        logger.warning("shopify search failed vendor=%s err=%s", spec.id, exc)
        return _empty(spec, search_link, f"{spec.label} search failed: {exc}")

    products = (((data.get("resources") or {}).get("results") or {}).get("products") or [])
    out: List[Dict[str, Any]] = []
    for p in products:
        if not isinstance(p, dict):
            continue
        title = str(p.get("title") or "").strip()
        handle = str(p.get("handle") or "").strip()
        price = _parse_price_number(p.get("price"))
        if price is None or not title:
            continue
        link = f"{spec.base_url.rstrip('/')}/products/{handle}" if handle else str(p.get("url") or search_link)
        if link.startswith("/"):
            link = spec.base_url.rstrip("/") + link
        out.append(
            _mk_result(
                source=spec.id,
                title=title,
                price=price,
                currency="INR",
                link=link,
                method="shopify_suggest",
            )
        )
        if len(out) >= MAX_RESULTS_PER_VENDOR:
            break
    return out or _empty(spec, search_link, f"No priced {spec.label} results found")


def _search_wc_store(spec: VendorSpec, query: str, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    q = urllib.parse.quote(query)
    url = (
        f"{spec.base_url.rstrip('/')}/wp-json/wc/store/v1/products"
        f"?search={q}&per_page={MAX_RESULTS_PER_VENDOR}"
    )
    search_link = spec.search_url.format(q=urllib.parse.quote_plus(query)) if spec.search_url else spec.base_url
    try:
        raw = _get_text(
            url,
            session=session,
            headers={"Referer": spec.base_url, "Accept": "application/json"},
            prefer_json=True,
        )
        data = json.loads(raw)
    except Exception as exc:
        logger.warning("wc_store search failed vendor=%s err=%s", spec.id, exc)
        # Fall back to HTML search page when available.
        if spec.search_url:
            return _search_html(spec, query, session=session)
        return _empty(spec, search_link, f"{spec.label} search failed: {exc}")

    if not isinstance(data, list) or not data:
        if spec.search_url:
            return _search_html(spec, query, session=session)
        return _empty(spec, search_link, f"No priced {spec.label} results found")

    out: List[Dict[str, Any]] = []
    for p in data:
        if not isinstance(p, dict):
            continue
        title = str(p.get("name") or "").strip()
        link = str(p.get("permalink") or "").strip() or search_link
        prices = p.get("prices") if isinstance(p.get("prices"), dict) else {}
        raw_price = prices.get("sale_price") or prices.get("price") or prices.get("regular_price")
        price = None
        if raw_price is not None:
            try:
                minor = int(prices.get("currency_minor_unit") or 2)
                price = float(raw_price) / (10 ** max(0, minor))
            except (TypeError, ValueError):
                price = _parse_price_number(raw_price)
        currency = str(prices.get("currency_code") or "INR").upper()
        if price is None or price <= 0 or not title:
            continue
        out.append(
            _mk_result(
                source=spec.id,
                title=title,
                price=price,
                currency=currency if currency in ("INR", "USD") else "INR",
                link=link,
                method="wc_store",
            )
        )
        if len(out) >= MAX_RESULTS_PER_VENDOR:
            break
    return out or _empty(spec, search_link, f"No priced {spec.label} results found")


def _is_product_href(href: str) -> bool:
    low = (href or "").lower()
    if any(
        x in low
        for x in (
            "/cart",
            "/checkout",
            "/account",
            "/category",
            "/product-category",
            "/collections/",
            "/tag/",
            "/wp-",
            "/blog",
            "/contact",
            "/about",
            "/login",
            "/wishlist",
            "javascript:",
            "mailto:",
        )
    ):
        return False
    if "/product/" in low or "/products/" in low or "route=product/product" in low:
        return True
    path = urllib.parse.urlparse(href).path
    if path.endswith(".html") and len(path.strip("/").split("/")) <= 2 and len(path) > 12:
        return True
    # Root-level product slugs (UAV Store, Fab.to.Lab, etc.)
    parts = [p for p in path.strip("/").split("/") if p]
    if len(parts) == 1:
        slug = parts[0]
        if len(slug) >= 8 and "-" in slug and not slug.startswith("page"):
            return True
    return False


def _clean_title(raw: str) -> str:
    title = re.sub(r"<[^>]+>", " ", html_lib.unescape(raw or ""))
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _extract_html_products(html: str, base_url: str, source_id: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, re.I | re.S):
        href = html_lib.unescape(m.group(1)).strip()
        full = urllib.parse.urljoin(base_url.rstrip("/") + "/", href)
        if not _is_product_href(full):
            continue
        title = _clean_title(m.group(2))
        if len(title) < 4 or len(title) > 200:
            tm = re.search(r'title="([^"]+)"', m.group(0), re.I)
            title = _clean_title(tm.group(1)) if tm else title
        if len(title) < 4:
            nearby = html[max(0, m.start() - 280) : m.end() + 280]
            hm = re.search(r"<h[1-4][^>]*>(.*?)</h[1-4]>", nearby, re.I | re.S)
            if hm:
                title = _clean_title(hm.group(1))
        if len(title) < 4 or title.lower() in BAD_TITLES:
            continue
        window = html[m.end() : m.end() + 1200] + html[max(0, m.start() - 300) : m.start()]
        pm = PRICE_NEAR_RE.search(window)
        if not pm:
            continue
        price = _parse_price_number(pm.group(1))
        # Ignore tiny values that are usually specs (e.g. 2.4GHz), not INR prices.
        if price is None or price < 10 or price > 5_000_000:
            continue
        key = full.split("?")[0].rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            _mk_result(
                source=source_id,
                title=title,
                price=price,
                currency="INR",
                link=full.split("#")[0],
                method="html_search",
            )
        )
        if len(out) >= MAX_RESULTS_PER_VENDOR:
            break
    return out


def _search_html(spec: VendorSpec, query: str, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    q = urllib.parse.quote_plus(query)
    candidates = []
    if spec.search_url:
        candidates.append(spec.search_url.format(q=q))
    base = spec.base_url.rstrip("/")
    candidates.extend(
        [
            f"{base}/?s={q}&post_type=product",
            f"{base}/catalogsearch/result/?q={q}",
        ]
    )
    seen_u = set()
    urls = []
    for u in candidates:
        if u not in seen_u:
            seen_u.add(u)
            urls.append(u)

    last_err = "No priced results found"
    best: List[Dict[str, Any]] = []
    for url in urls[:4]:
        try:
            html = _get_text(url, session=session, headers={"Referer": base + "/"})
        except Exception as exc:
            last_err = str(exc)
            continue
        low = html.lower()
        if len(html) < 2000 and ("access denied" in low or "just a moment" in low):
            last_err = f"{spec.label} blocked the request"
            continue
        products = _extract_html_products(html, base, spec.id)
        if len(products) > len(best):
            best = products
        if len(best) >= MAX_RESULTS_PER_VENDOR:
            return best[:MAX_RESULTS_PER_VENDOR]
    if best:
        return best[:MAX_RESULTS_PER_VENDOR]
    return _empty(spec, urls[0], last_err if last_err else f"No priced {spec.label} results found")


def search_vendor(
    spec: VendorSpec,
    query: str,
    *,
    session: Optional[requests.Session] = None,
    pincode: Optional[str] = None,
    amazon_search: Optional[Callable[..., List[Dict[str, Any]]]] = None,
    robu_search: Optional[Callable[..., List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    query = (query or "").strip()
    if not query:
        return _empty(spec, spec.base_url, "Empty query")

    try:
        strategy = spec.strategy
        if strategy == "amazon":
            if not amazon_search:
                return _empty(spec, spec.base_url, "Amazon search unavailable")
            return amazon_search(query, session, pincode)
        if strategy == "robu":
            if not robu_search:
                return _empty(spec, spec.base_url, "Robu search unavailable")
            return robu_search(query, session)
        if strategy == "shopify":
            return _search_shopify(spec, query, session=session)
        if strategy == "wc_store":
            return _search_wc_store(spec, query, session=session)
        return _search_html(spec, query, session=session)
    except Exception as exc:
        logger.exception("vendor search failed id=%s", spec.id)
        link = spec.search_url.format(q=urllib.parse.quote_plus(query)) if spec.search_url else spec.base_url
        return _empty(spec, link, f"{spec.label} search failed: {exc}")


def search_vendors(
    query: str,
    source_ids: Optional[Sequence[str]] = None,
    *,
    profile: Optional[str] = None,
    pincode: Optional[str] = None,
    amazon_search: Optional[Callable[..., List[Dict[str, Any]]]] = None,
    robu_search: Optional[Callable[..., List[Dict[str, Any]]]] = None,
    max_workers: Optional[int] = None,
) -> tuple[List[Dict[str, Any]], List[str]]:
    """Run live searches across resolved vendors in parallel."""
    specs = resolve_vendors(sources=source_ids, profile=profile)
    results: List[Dict[str, Any]] = []
    errors: List[str] = []
    if not specs:
        return results, errors

    workers_default = get_runtime_defaults()["max_workers"]
    workers = max(1, min(int(max_workers or workers_default), len(specs)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        fut_map = {
            pool.submit(
                search_vendor,
                spec,
                query,
                pincode=pincode,
                amazon_search=amazon_search,
                robu_search=robu_search,
            ): spec
            for spec in specs
        }
        for fut in as_completed(fut_map):
            spec = fut_map[fut]
            try:
                results.extend(fut.result())
            except Exception as exc:
                logger.exception("vendor future failed id=%s", spec.id)
                errors.append(f"{spec.id}: {exc}")
                results.extend(_empty(spec, spec.base_url, str(exc)))
    return results, errors
