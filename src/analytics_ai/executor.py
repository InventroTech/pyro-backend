from django.db import connection, DatabaseError, OperationalError
from typing import Any, Iterable, List, Optional, Sequence, Tuple, Mapping, MutableMapping, Union
import time
import traceback
import re
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta


from .utils.redaction import redact_pii
from .utils.table import rows_to_dicts
from .utils import log as log

DB_STATEMENT_TIMEOUT_MS = 15000   # 15s guardrail for runaway queries
FETCHMANY_SIZE = 1000             # stream in chunks to avoid RAM spikes
ENABLE_READ_ONLY_TXN = True       # keep prod DB safe from accidental writes
LOG_ROW_PREVIEW = 5               # how many raw rows to preview in logs (masked)
REDACT_PII_IN_LOGS = True

MODULE = "SQL EXECUTOR"

# --- Placeholder normalizers / defaults --------------------------------------

_BRACKET_REPLACEMENTS = {
    r"\[start\]": "%(start)s",
    r"\[end\]": "%(end)s",
    r"\[today\]": "%(today)s",
}
_BRACKET_RE = re.compile(r"\[(start|end|today)\]", re.IGNORECASE)

def _normalize_bracket_placeholders(sql: str) -> str:
    """
    Replace legacy [start]/[end]/[today] tokens with psycopg2 named params.
    """
    out = sql
    for pat, repl in _BRACKET_REPLACEMENTS.items():
        out = re.sub(pat, repl, out, flags=re.IGNORECASE)
    return out

def _past_month_range(now: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """
    Return (start, end) for the past rolling month as [start, end) in UTC.
    """
    now = now or datetime.now(timezone.utc)
    end = now.replace(microsecond=0)
    start = (end - relativedelta(months=1))
    return start, end

def _default_date_params_for(sql: str) -> dict:
    """
    Provide default bindings if the SQL references %(start)s/%(end)s/%(today)s.
    """
    s = sql.lower()
    out = {}
    if "%(start)s" in s or "%(end)s" in s:
        start, end = _past_month_range()
        out.setdefault("start", start)
        out.setdefault("end", end)
    if "%(today)s" in s:
        out.setdefault("today", datetime.now(timezone.utc).replace(microsecond=0))
    return out

def _merge_params(sql: str,
                  params: Optional[Union[Sequence[Any], Mapping[str, Any]]]
                 ) -> Tuple[str, Union[List[Any], Mapping[str, Any]]]:
    """
    Unify param handling:
    - Convert legacy [start]/[end]/[today] to named params.
    - If named params appear, auto-supply defaults for missing date params.
    - Preserve caller-provided params (named or positional).
    """
    # Step 1: normalize legacy placeholders to named ones
    normalized_sql = _normalize_bracket_placeholders(sql)

    # Step 2: if caller gave named params, merge defaults; if positional, keep as-is
    if isinstance(params, Mapping) or "%(" in normalized_sql:
        # Start with auto-defaults if needed
        merged: MutableMapping[str, Any] = _default_date_params_for(normalized_sql)
        # Then overlay caller's named params (if any)
        if isinstance(params, Mapping):
            merged.update(params)
        return normalized_sql, merged

    # Positional: keep as list (or empty list)
    return normalized_sql, list(params or [])

# --- Safety & execution helpers ----------------------------------------------

def _set_pg_safety(cursor) -> None:
    """
    Apply per-transaction safety rails for Postgres.
    """
    cursor.execute("SET LOCAL statement_timeout TO %s;", [DB_STATEMENT_TIMEOUT_MS])
    if ENABLE_READ_ONLY_TXN:
        cursor.execute("SET LOCAL default_transaction_read_only = on;")

def _fetch_all_streaming(cursor, chunk_size: int) -> List[Sequence[Any]]:
    """
    Stream results in bounded chunks to avoid RAM spikes.
    """
    rows: List[Sequence[Any]] = []
    while True:
        chunk = cursor.fetchmany(chunk_size)
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < chunk_size:
            break
    return rows

def _log_preview(columns: Sequence[str], rows: List[Sequence[Any]]) -> None:
    """
    Log a tiny preview, optionally masking PII.
    """
    preview = rows[:LOG_ROW_PREVIEW]
    if REDACT_PII_IN_LOGS:
        masked = [[redact_pii(str(v)) for v in r] for r in preview]
        log.dbg(MODULE, f"Columns: {list(columns)} | Rows (masked, up to {LOG_ROW_PREVIEW})", masked, mask_pii=False)
    else:
        log.dbg(MODULE, f"Columns: {list(columns)} | Rows (up to {LOG_ROW_PREVIEW})", preview, mask_pii=False)

# --- Public API ---------------------------------------------------------------

def execute_safe_sql(
    sql_query: str,
    params: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None
) -> Tuple[Optional[List[dict]], Optional[str]]:
    """
    Execute a read-only SELECT with safety rails.
    Returns (list[dict], None) on success or (None, 'error message') on failure.

    * Normalizes legacy [start]/[end]/[today] -> %(start)s/%(end)s/%(today)s
    * Auto-supplies date params when named placeholders are present and missing
    * SET LOCAL statement_timeout
    * Enforce read-only transaction (Postgres)
    * Stream results with fetchmany() to bound memory
    * Log columns + small sample with optional PII redaction
    * Preserves original return contract
    """
    # Normalize SQL & unify params first
    normalized_sql, final_params = _merge_params(sql_query, params)

    t0 = time.perf_counter()
    # Log with PII masking, but include whether params are named or positional
    param_meta = {
        "style": "named" if isinstance(final_params, Mapping) else "positional",
        "keys": list(final_params.keys()) if isinstance(final_params, Mapping) else None,
        "len": len(final_params) if isinstance(final_params, Sequence) and not isinstance(final_params, (str, bytes)) else None,
    }
    log.dbg(MODULE, "Executing SQL", {"sql": normalized_sql, "param_meta": param_meta}, mask_pii=True)

    with connection.cursor() as cursor:
        try:
            _set_pg_safety(cursor)

            cursor.execute(normalized_sql, final_params)
            log.dbg(MODULE, "Query executed successfully.")

            columns: Sequence[str] = [c[0] for c in (cursor.description or [])]
            if not columns:
                # Shouldn't happen for SELECTs; remain defensive
                log.dbg(MODULE, "No columns returned (possibly DDL statement?)")
                return [], None

            rows = _fetch_all_streaming(cursor, FETCHMANY_SIZE)
            _log_preview(columns, rows)

            dict_rows = rows_to_dicts(columns, rows)

            dur_ms = int((time.perf_counter() - t0) * 1000)
            log.dbg(MODULE, f"Row count: {len(rows)} | Col count: {len(columns)} | Duration_ms: {dur_ms}")
            log.dbg(MODULE, "Dict rows preview (up to 3)", dict_rows[:3], mask_pii=True)

            return dict_rows, None

        except (OperationalError, DatabaseError) as e:
            detail = ""
            diag = getattr(e, "diag", None)
            if diag is not None and hasattr(diag, "message_primary"):
                detail += f" | detail={getattr(diag, 'message_primary', '')}"
            pgcode = getattr(e, "pgcode", None)
            if pgcode:
                detail += f" | pgcode={pgcode}"

            log.err(MODULE, f"DB ERROR: {e}{detail} | SQL: {normalized_sql}", exc=e)
            traceback.print_exc()
            return None, f"{str(e)}{detail}"

        except Exception as e:
            log.err(MODULE, f"UNEXPECTED ERROR: {e} | SQL: {normalized_sql}", exc=e)
            traceback.print_exc()
            return None, str(e)
