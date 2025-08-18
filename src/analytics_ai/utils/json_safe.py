import datetime, math
from decimal import Decimal
from uuid import UUID

def coerce_json_safe(v):
    if v is None:
        return None
    if isinstance(v, (datetime.datetime, datetime.date)):
        if isinstance(v, datetime.datetime) and v.tzinfo is None:
            return v.isoformat() + "Z"
        return v.isoformat()
    if isinstance(v, Decimal):
        try:
            f = float(v)
            return None if (math.isinf(f) or math.isnan(f)) else f
        except Exception:
            return None
    if isinstance(v, float):
        return None if (math.isinf(v) or math.isnan(v)) else v
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, (list, tuple)):
        return [coerce_json_safe(x) for x in v]
    if isinstance(v, dict):
        return {k: coerce_json_safe(val) for k, val in v.items()}
    return v
