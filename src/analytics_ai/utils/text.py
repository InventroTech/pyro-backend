import json
from hashlib import sha256

def truncate(s: str | None, limit: int) -> str | None:
    if s is None:
        return None
    return s if len(s) <= limit else s[:limit] + "...(truncated)"

def sha12(s: str) -> str:
    try:
        return sha256(s.encode("utf-8")).hexdigest()[:12]
    except Exception:
        return "na"

def safe_json(obj, max_len: int = 2000) -> str:
    """
    Safe stringify for log payloads. Never raises.
    """
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        try:
            s = json.dumps(str(obj), ensure_ascii=False)
        except Exception:
            s = '"<unserializable>"'
    if len(s) > max_len:
        s = s[:max_len] + "...(truncated)"
    return s
