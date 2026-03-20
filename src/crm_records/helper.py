"""
Helper utilities for filtering and parsing CRM JSON data payloads.

Extracted into a dedicated module so the same logic can be reused in multiple views.
"""

# Numeric comparison lookups for data JSON field (so total_price__gte=50000 works as numeric >=)
NUMERIC_LOOKUPS = ("__gt", "__gte", "__lt", "__lte")


def parse_numeric_lookup(field_name):
    """
    If field_name is like 'total_price__gte', return ('total_price', '__gte'); otherwise return None.
    """
    for suffix in NUMERIC_LOOKUPS:
        if field_name.endswith(suffix):
            base = field_name[: -len(suffix)]
            if base:
                return base, suffix
    return None


def coerce_numeric(value):
    """
    Coerce string to int or float for use in numeric filters.

    Returns:
      (coerced_value, True) if coercion succeeded
      (original_value, False) otherwise
    """
    if value is None or value == "":
        return None, False

    if isinstance(value, (int, float)):
        return value, True

    s = str(value).strip()
    if not s:
        return None, False

    try:
        if "." in s:
            return float(s), True
        return int(s), True
    except (ValueError, TypeError):
        return value, False

