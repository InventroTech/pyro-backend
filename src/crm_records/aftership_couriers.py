"""AfterShip courier catalog used by the inventory tracking combobox."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import List, TypedDict


class AftershipCourier(TypedDict):
    name: str
    slug: str


@lru_cache(maxsize=1)
def load_aftership_couriers() -> List[AftershipCourier]:
    path = Path(__file__).with_name("aftership_couriers.json")
    with path.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, list):
        return []
    out: List[AftershipCourier] = []
    seen: set[str] = set()
    for row in raw:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        slug = str(row.get("slug") or "").strip()
        if not name or not slug or slug in seen:
            continue
        seen.add(slug)
        out.append({"name": name, "slug": slug})
    return out
