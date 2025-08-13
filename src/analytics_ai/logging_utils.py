import logging
import os
import time
from logging.handlers import RotatingFileHandler
from contextvars import ContextVar

from .utils.redaction import redact_pii
from .utils.text import truncate, sha12, safe_json

# -------- Config (env‑tunable) --------
LOG_PATH = os.getenv("ANALYTICS_LOG_PATH", "analytics_ai.log")
LOG_LEVEL = os.getenv("ANALYTICS_LOG_LEVEL", "INFO").upper()
LOG_MAX_BYTES = int(os.getenv("ANALYTICS_LOG_MAX_BYTES", 10 * 1024 * 1024))  # 10 MB
LOG_BACKUPS = int(os.getenv("ANALYTICS_LOG_BACKUPS", 5))
LOG_TO_CONSOLE = os.getenv("ANALYTICS_LOG_TO_CONSOLE", "false").lower() in {"1", "true", "yes"}

# -------- Logger init (idempotent) --------
analytics_logger = logging.getLogger("analytics_ai")
analytics_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

if not analytics_logger.handlers:
    # Ensure directory exists
    try:
        os.makedirs(os.path.dirname(LOG_PATH) or ".", exist_ok=True)
    except Exception:
        pass

    file_handler = RotatingFileHandler(LOG_PATH, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8")

    class UtcFormatter(logging.Formatter):
        converter = time.gmtime

    formatter = UtcFormatter('%(asctime)s %(levelname)s %(message)s')
    file_handler.setFormatter(formatter)
    analytics_logger.addHandler(file_handler)

    if LOG_TO_CONSOLE:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        analytics_logger.addHandler(console)

# -------- Per-request context --------
_log_ctx: ContextVar[dict] = ContextVar("analytics_log_ctx", default={})

def set_log_context(**kwargs):
    """
    Call at request start to attach context to all subsequent logs:
    set_log_context(trace_id=..., run_id=..., user_email=..., workspace_id=...)
    """
    ctx = dict(_log_ctx.get())
    ctx.update({k: v for k, v in kwargs.items() if v is not None})
    _log_ctx.set(ctx)

def clear_log_context():
    _log_ctx.set({})

# -------- Public logging API (backward-compatible) --------
def log_analytics_event(event_type, user_id, question, llm_prompt=None, sql_query=None, result=None, error=None):
    """
    Logs analytics events and errors for audit/debugging.
    Backward compatible with existing calls/signature.
    """
    # Prepare redacted, bounded fields
    q_snippet = truncate(redact_pii(question or ""), 500)

    red_llm_prompt = None
    if llm_prompt:
        red_llm_prompt = truncate(redact_pii(llm_prompt), 500)

    red_sql = truncate(redact_pii(sql_query), 2000) if sql_query else None
    red_result = None
    if result is not None:
        # If it's a structure, stringify safely first, then truncate
        red_result = truncate(safe_json(result), 2000)

    red_error = truncate(redact_pii(str(error))) if error else None

    # Hashes for correlation without leaking contents
    prompt_hash = sha12(llm_prompt or "") if llm_prompt else None
    sql_hash = sha12(sql_query or "") if sql_query else None

    # Merge with per-request context
    ctx = dict(_log_ctx.get())

    entry = {
        "event_type": event_type,
        "user_id": user_id,
        "question": q_snippet,
        "llm_prompt": red_llm_prompt,
        "llm_prompt_hash": prompt_hash,
        "sql_query": red_sql,
        "sql_hash": sql_hash,
        "result": red_result,
        "error": red_error,
        **ctx,  # allow callers to set trace_id/run_id/etc.
    }

    try:
        analytics_logger.info(safe_json(entry, max_len=8000))
    except Exception:
        # Never let logging crash the app
        try:
            analytics_logger.error("log_analytics_event: failed to serialize entry")
        except Exception:
            pass
