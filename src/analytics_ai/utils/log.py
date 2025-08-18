import logging
from typing import Any, Optional
from .redaction import redact_pii

logger = logging.getLogger("analytics_ai")
# configure level/handlers in Django settings or project logging config

def dbg(module: str, message: str, obj: Optional[Any] = None, mask_pii: bool = False, preview_len: int = 300):
    try:
        suffix = ""
        if obj is not None:
            txt = str(obj)
            if mask_pii:
                txt = redact_pii(txt)
            suffix = f" | Preview: {txt[:preview_len]}"
        logger.debug(f"[{module}] {message}{suffix}")
    except Exception:
        logger.debug(f"[{module}] {message} | <unprintable>")

def err(module: str, message: str, exc: Optional[BaseException] = None):
    logger.error(f"[{module}] {message}", exc_info=exc)
