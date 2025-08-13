import re
import sqlparse
from typing import List, Set, Tuple
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import DML, Keyword, Name, Wildcard, Punctuation

# --- Tunables / defaults (kept unchanged) ---
ENFORCE_LIMIT = True
DEFAULT_LIMIT = 1000

DISALLOWED_KEYWORDS = {
    "insert", "update", "delete", "drop", "alter", "truncate", "create",
    "copy", "set ", "show ", "grant", "revoke", "refresh materialized",
    "vacuum", "analyze",
}
DISALLOWED_SCHEMAS = {"pg_", "pgcatalog", "pg_catalog", "information_schema"}
ALLOWED_COMMAND = "select"

# --- Precompiled regexes ---
_LIMIT_RE = re.compile(r"\blimit\b\s+\d+", re.IGNORECASE)

# Build a conservative disallowed-keywords pattern with word boundaries
# Notes:
#  - Keep multi-word tokens intact (e.g., "refresh materialized")
#  - Preserve legacy "set " and "show " semantics by keeping the trailing space
#  - Use (?i) for case-insensitivity
_disallowed_sorted = sorted(DISALLOWED_KEYWORDS, key=len, reverse=True)
_disallowed_pattern = "|".join(re.escape(k) for k in _disallowed_sorted)
_DISALLOWED_RE = re.compile(rf"(?i)\b(?:{_disallowed_pattern})")

# --- Small helpers ------------------------------------------------------------

def _dbg(enabled: bool, *args):
    if enabled:
        # Safe minimal debug; swap to your central logger if desired
        print(*args)

def _has_multiple_statements(sql: str) -> bool:
    """Allow a single trailing semicolon; reject other internal semicolons."""
    body = (sql or "").strip()
    if body.endswith(";"):
        body = body[:-1]
    return ";" in body

def _first_non_ws_token(statement):
    for t in statement.tokens:
        if not t.is_whitespace:
            return t
    return None

def _normalize_ident_name(ident) -> str:
    """
    Try to normalize an Identifier to its real/table name.
    Removes quotes and schema qualifiers; lowercases result.
    """
    name = None
    try:
        name = ident.get_real_name() or ident.get_name()
    except Exception:
        name = None
    if not name:
        # fallback: last chunk
        name = str(ident).strip().split()[-1] if str(ident).strip() else None
    if not name:
        return ""
    # Drop quotes and schema prefix
    name = name.strip('"').split(".")[-1].lower()
    return name

def _extract_tables(statement) -> Set[str]:
    """
    Best‑effort extraction of table identifiers from FROM and JOIN clauses,
    including simple CTE main SELECT.
    """
    tables: Set[str] = set()
    from_seen = False

    for token in statement.tokens:
        if token.is_whitespace:
            continue

        # WITH ... AS (...) — we do not validate CTE names here; main SELECT will be validated
        if token.ttype is Keyword and token.value.upper() == "WITH":
            # No-op; continue scanning
            pass

        if token.ttype is Keyword and token.value.upper() in {"FROM", "JOIN"}:
            from_seen = True
            continue

        if from_seen:
            if isinstance(token, IdentifierList):
                for ident in token.get_identifiers():
                    name = _normalize_ident_name(ident)
                    if name and name != "select":
                        tables.add(name)
            elif isinstance(token, Identifier):
                name = _normalize_ident_name(token)
                if name and name != "select":
                    tables.add(name)
            elif token.ttype is Punctuation and token.value == ",":
                # continue the list
                pass
            else:
                # stop scanning until next FROM/JOIN
                from_seen = False

    return tables

def _extract_selected_columns(statement) -> List[str]:
    """
    Very light column extraction from the SELECT list; used only when allowed_columns is passed.
    Best‑effort and intentionally permissive.
    """
    cols: List[str] = []
    seen_select = False

    for token in statement.tokens:
        if token.is_whitespace:
            continue
        if token.ttype is DML and token.value.upper() == "SELECT":
            seen_select = True
            continue

        if seen_select:
            if isinstance(token, IdentifierList):
                for ident in token.get_identifiers():
                    if isinstance(ident, Function):
                        continue
                    if any(t.ttype is Wildcard for t in ident.tokens):
                        cols.append("*")
                        continue
                    name = (ident.get_real_name() or ident.get_name() or "").lower()
                    if name:
                        cols.append(name)
            elif isinstance(token, Identifier):
                if any(t.ttype is Wildcard for t in token.tokens):
                    cols.append("*")
                else:
                    name = (token.get_real_name() or token.get_name() or "").lower()
                    if name:
                        cols.append(name)

            # Stop at FROM
            if token.ttype is Keyword and token.value.upper() == "FROM":
                break

    return cols

def _has_disallowed_schema(sql_lower: str) -> bool:
    # Keep legacy semantics: substring guard against system schemas
    return any(s in sql_lower for s in DISALLOWED_SCHEMAS)

def _enforce_limit_present(sql_lower: str) -> bool:
    return bool(_LIMIT_RE.search(sql_lower))

# --- Public validator ---------------------------------------------------------

def is_safe_sql(
    sql_query: str,
    allowed_tables: Set[str],
    allowed_columns: Set[str] = None,
    enforce_limit: bool = False,
    debug: bool = True
) -> Tuple[bool, str]:
    """
    Validate that the SQL query is read-only, only accesses allowed tables/columns,
    and contains no disallowed statements.
    Returns (is_safe: bool, reason: str).
    """
    _dbg(debug, "[SQL SAFETY] Raw SQL Query:", sql_query)

    if not sql_query or not isinstance(sql_query, str):
        return False, "Empty SQL."

    lower_sql = sql_query.lower()

    # Quick rejects
    if _has_multiple_statements(sql_query):
        return False, "Multiple statements are not allowed; submit a single SELECT."

    # Disallowed keywords (conservative word-boundary match)
    if _DISALLOWED_RE.search(lower_sql):
        # Try to echo which one we saw (best-effort)
        for bad in _disallowed_sorted:
            if bad in lower_sql:
                _dbg(debug, f"[SQL SAFETY] Disallowed keyword detected: {bad.upper()}")
                return False, f"Query contains disallowed keyword: {bad.upper()}"
        # Fallback
        return False, "Query contains disallowed keywords."

    if _has_disallowed_schema(lower_sql):
        return False, "Access to system schemas (pg_catalog/information_schema) is not allowed."

    # Parse
    parsed = sqlparse.parse(sql_query)
    if not parsed or not parsed[0].tokens:
        _dbg(debug, "[SQL SAFETY] Could not parse SQL tokens.")
        return False, "SQL could not be parsed."

    statement = parsed[0]

    # First non‑whitespace must be SELECT
    first_token = _first_non_ws_token(statement)
    _dbg(debug, "[SQL SAFETY] First token:", first_token)
    if not first_token:
        return False, "Empty statement."
    if (getattr(first_token, "ttype", None) is not DML) and first_token.value.lower() != ALLOWED_COMMAND:
        return False, "Only SELECT queries are allowed."

    # Table usage check
    tables_used = _extract_tables(statement)
    _dbg(debug, f"[SQL SAFETY] Tables detected in SQL: {tables_used}")

    if allowed_tables:
        allowed_tables_lc = {a.lower() for a in allowed_tables}

        if not tables_used:
            # Fallback substring check for very complex queries
            found_allowed = any(t in lower_sql for t in allowed_tables_lc)
            if not found_allowed:
                return False, f"Query must reference at least one allowed table ({', '.join(sorted(allowed_tables))})."

        for t in tables_used:
            if t not in allowed_tables_lc:
                return False, f"Query tries to access disallowed table: {t}"

    # Optional columns check (best‑effort)
    if allowed_columns:
        raw_allowed = {c.lower() for c in allowed_columns}
        cols = _extract_selected_columns(statement)
        _dbg(debug, f"[SQL SAFETY] Selected columns (best-effort): {cols}")

        for c in cols:
            if c == "*" or c in raw_allowed:
                continue
            # Only guard simple identifiers; ignore function outputs/aliases
            if re.match(r"^[a-z_][a-z0-9_]*$", c) and c not in raw_allowed:
                return False, f"Selected column '{c}' is not in the allowed columns set."

    # Optional LIMIT enforcement
    if enforce_limit and not _enforce_limit_present(lower_sql):
        return False, f"Query must include a LIMIT clause to prevent large result sets (e.g., LIMIT {DEFAULT_LIMIT})."

    _dbg(debug, "[SQL SAFETY] Query is safe.")
    return True, "Query is safe."
