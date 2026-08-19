"""Inventory request workflow helpers (cart status transitions)."""

from __future__ import annotations

from typing import Any, Mapping, Optional


def normalize_inventory_status(raw: Any) -> str:
    if raw is None:
        return ""
    return str(raw).strip().upper().replace(" ", "_")


def apply_inventory_cart_status_side_effects(
    data: Mapping[str, Any],
    previous: Optional[Mapping[str, Any]] = None,
) -> dict:
    """
    When an item is removed from the cart (IN_CART → VENDOR_IDENTIFIED),
    drop cart_id so it is no longer linked to a cart.
    """
    out = dict(data)
    prev = previous if isinstance(previous, Mapping) else {}
    previous_status = normalize_inventory_status(prev.get("status"))
    next_status = normalize_inventory_status(out.get("status"))
    if next_status == "VENDOR_IDENTIFIED" and previous_status == "IN_CART":
        out["cart_id"] = None
    return out
