from django.db import connection, DatabaseError, OperationalError
from typing import Any, Iterable, List, Optional, Sequence, Tuple
import time
import traceback

from .utils.redaction import redact_pii
from .utils.table import rows_to_dicts
from .utils import log as log

DB_STATEMENT_TIMEOUT_MS = 15000   # 15s guardrail for runaway queries
FETCHMANY_SIZE = 1000             # stream in chunks to avoid RAM spikes
ENABLE_READ_ONLY_TXN = True       # keep prod DB safe from accidental writes
LOG_ROW_PREVIEW = 5               # how many raw rows to preview in logs (masked)
REDACT_PII_IN_LOGS = True

MODULE = "SQL EXECUTOR"


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


def execute_safe_sql(sql_query: str, params: Optional[Sequence[Any]] = None) -> Tuple[Optional[List[dict]], Optional[str]]:
    """
    Execute a read-only SELECT with safety rails.
    Returns (list[dict], None) on success or (None, 'error message') on failure.

    * SET LOCAL statement_timeout
    * Enforce read-only transaction (Postgres)
    * Stream results with fetchmany() to bound memory
    * Log columns + small sample with optional PII redaction
    * Preserve original return contract
    """
    t0 = time.perf_counter()
    log.dbg(MODULE, "Executing SQL", {"sql": sql_query, "params": params}, mask_pii=True)

    with connection.cursor() as cursor:
        try:
            _set_pg_safety(cursor)

            cursor.execute(sql_query, params or [])
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

            log.err(MODULE, f"DB ERROR: {e}{detail} | SQL: {sql_query}", exc=e)
            traceback.print_exc()
            return None, f"{str(e)}{detail}"

        except Exception as e:
            log.err(MODULE, f"UNEXPECTED ERROR: {e} | SQL: {sql_query}", exc=e)
            traceback.print_exc()
            return None, str(e)
