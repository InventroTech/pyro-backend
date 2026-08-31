"""
Helper utilities for filtering and parsing CRM JSON data payloads.

Extracted into a dedicated module so the same logic can be reused in multiple views.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Iterable, Optional

from django.db.models import Q
from django.db.models.fields.json import KeyTextTransform

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


def json_field_contains_q(field_name: str, field_value) -> Q:
    """
    Build Q for exact JSON key match; false also matches null/missing (empty sheet cells).
    """
    match_val = coerce_json_contains_value(field_value)
    if match_val is False:
        return Q(data__contains={field_name: False}) | Q(
            **{f"data__{field_name}__isnull": True}
        )
    return Q(data__contains={field_name: match_val})


_JSON_TEXT_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_JSON_TEXT_COLUMNS = frozenset({"data", "pyro_data"})


def json_text_equals_value(value) -> Optional[str]:
    """Normalize an identity value for data->>'field' comparison."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    text = str(value).strip()
    return text or None


def filter_json_text_equals(qs, field: str, value, *, json_column: str = "data"):
    """
    Filter where ``json_column->>'field' = value`` (text).

    Django ``data__praja_id=x`` compiles to ``(data -> 'praja_id') = '"x"'::jsonb``,
    which cannot use btree expression indexes such as ``records_praja_id_idx``
    or ``records_lead_praja_id_tenant_unique``. ``->>`` matches those indexes
    and also matches JSON numbers stored in the key (both become text).
    """
    text = json_text_equals_value(value)
    if text is None:
        return qs.none()
    if not _JSON_TEXT_KEY_RE.match(field):
        raise ValueError(f"Invalid JSON key: {field!r}")
    if json_column not in _JSON_TEXT_COLUMNS:
        raise ValueError(f"Invalid JSON column: {json_column!r}")
    alias = f"_jt_{json_column}_{field}"
    return qs.alias(**{alias: KeyTextTransform(field, json_column)}).filter(**{alias: text})


def filter_json_text_equals_any(qs, fields: Iterable[str], value, *, json_column: str = "data"):
    """OR of ``filter_json_text_equals`` across several JSON keys (one scan)."""
    text = json_text_equals_value(value)
    field_list = list(fields)
    if text is None or not field_list:
        return qs.none()
    if json_column not in _JSON_TEXT_COLUMNS:
        raise ValueError(f"Invalid JSON column: {json_column!r}")
    aliases = {}
    q = Q()
    for field in field_list:
        if not _JSON_TEXT_KEY_RE.match(field):
            raise ValueError(f"Invalid JSON key: {field!r}")
        alias = f"_jt_{json_column}_{field}"
        aliases[alias] = KeyTextTransform(field, json_column)
        q |= Q(**{alias: text})
    return qs.alias(**aliases).filter(q)

