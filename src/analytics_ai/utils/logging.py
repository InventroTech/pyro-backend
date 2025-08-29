import logging
import pprint
from .redaction import redact_pii

logger = logging.getLogger("analytics_ai")
logger.setLevel(logging.DEBUG)

def log_debug(module, message, obj=None, mask_pii=False, preview_len=300):
    msg = f"[{module}] {message}"
    if obj is not None:
        try:
            if mask_pii:
                obj = redact_pii(str(obj))
            preview = str(obj)[:preview_len]
            msg += f" | Preview: {preview}"
        except Exception:
            msg += " | <unprintable>"
    logger.debug(msg)

def log_error(module, message, exc=None):
    logger.error(f"[{module}] {message}")
    if exc:
        logger.exception(exc)
