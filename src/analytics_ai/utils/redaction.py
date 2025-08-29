import re

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_UUID_RE  = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b", re.I)

def redact_pii(s: str) -> str:
    if s is None:
        return s
    s = _EMAIL_RE.sub("[email]", s)
    s = _UUID_RE.sub("[uuid]", s)
    return s
