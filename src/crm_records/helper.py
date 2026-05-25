"""
Helper utilities for filtering and parsing CRM JSON data payloads.

Extracted into a dedicated module so the same logic can be reused in multiple views.
"""

from datetime import date, datetime

try:
    from dateutil import parser as date_parser  # type: ignore
except ImportError:  # pragma: no cover
    date_parser = None

# Numeric / date comparison lookups on JSON data keys (total_price__gte, po_date__lte, …)
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


def coerce_date_bound(value):
    """
    Parse a filter value into a date for JSON field comparisons (stored as YYYY-MM-DD strings).

    Returns:
      (date_value, True) if parsing succeeded
      (None, False) otherwise
    """
    if value is None or value == "":
        return None, False

    if isinstance(value, datetime):
        return value.date(), True

    if isinstance(value, date):
        return value, True

    s = str(value).strip()
    if not s:
        return None, False

    if date_parser is not None:
        try:
            return date_parser.parse(s).date(), True
        except (ValueError, TypeError, OverflowError):
            pass

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:10] if fmt == "%Y-%m-%d" else s, fmt).date(), True
        except ValueError:
            continue

    return None, False


def coerce_json_contains_value(value):
    """
    Coerce query-string values for JSONB @> filters.
    Dispatch booleans are stored as true/false, not the strings "true"/"false".
    """
    if isinstance(value, bool):
        return value
    if not isinstance(value, str):
        return value
    s = value.strip()
    lower = s.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if s.isdigit():
        try:
            return int(s)
        except ValueError:
            pass
    return s


def json_field_contains_q(field_name: str, field_value) -> "Q":
    """
    Build Q for exact JSON key match; false also matches null/missing (empty sheet cells).
    """
    from django.db.models import Q

    match_val = coerce_json_contains_value(field_value)
    if match_val is False:
        return Q(data__contains={field_name: False}) | Q(
            **{f"data__{field_name}__isnull": True}
        )
    return Q(data__contains={field_name: match_val})

