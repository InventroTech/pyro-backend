"""
Shared Circle state / district / party ID→name catalog.

Catalog file: geo_party_catalog.json (sourced from Circle spellings PDFs).
Store IDs on users and leads as numbers; use names only for UI labels like
"Krishna (13279)".
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

_CONFIG_PATH = Path(__file__).with_name("geo_party_catalog.json")


@lru_cache(maxsize=1)
def load_geo_party_catalog() -> dict[str, Any]:
    with _CONFIG_PATH.open(encoding="utf-8") as fh:
        return json.load(fh)


def _as_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def format_catalog_label(name: str, item_id: int) -> str:
    return f"{name} ({item_id})"


def catalog_options(
    kind: str,
    *,
    state_id: int | str | None = None,
) -> list[dict[str, Any]]:
    """
    Return select options as {value:int, label:str} for frontend dropdowns.

    kind: "states" | "districts" | "parties"
    """
    catalog = load_geo_party_catalog()
    items = catalog.get(kind) or []
    filter_state_id = _as_int(state_id) if state_id is not None and state_id != "" else None
    options: list[dict[str, Any]] = []
    for item in items:
        item_state_id = _as_int(item.get("state_id")) if "state_id" in item else None
        if filter_state_id is not None and item_state_id != filter_state_id:
            continue
        item_id = _as_int(item.get("id"))
        if item_id is None:
            continue
        name = str(item["name"])
        option: dict[str, Any] = {
            "value": item_id,
            "label": format_catalog_label(name, item_id),
            "name": name,
        }
        if item_state_id is not None:
            option["state_id"] = item_state_id
        options.append(option)
    return options


def normalize_to_catalog_ids(kind: str, values: list) -> list[str]:
    """
    Map a list of catalog IDs or English names to ID strings.

    Unknown values are kept as stripped strings so callers can still apply
    a legacy name fallback on the original lead field.
    """
    catalog = load_geo_party_catalog()
    items = catalog.get(kind) or []
    by_id: dict[str, str] = {}
    by_name: dict[str, str] = {}
    for item in items:
        item_id = _as_int(item.get("id"))
        if item_id is None:
            continue
        sid = str(item_id)
        by_id[sid] = sid
        name = str(item.get("name") or "").strip().lower()
        if name:
            by_name[name] = sid

    out: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        if raw is None or isinstance(raw, bool):
            continue
        as_int = _as_int(raw)
        if as_int is not None:
            key = str(as_int)
        else:
            text = str(raw).strip()
            if not text:
                continue
            key = by_id.get(text) or by_name.get(text.lower()) or text
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out
