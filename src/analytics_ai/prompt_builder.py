import hashlib
import datetime
import re
from typing import List, Optional

from .utils import log as log

PROMPT_VERSION = "v2"
MAX_SCHEMA_CHARS = 8000           # soft cap to reduce LLM tokens
DEFAULT_LIMIT_HINT = 1000         # only a hint; validator/enforcer lives elsewhere
INCLUDE_LIMIT_HINT = True         # keep True for safer LLM output
INCLUDE_DATE_GUIDANCE = True

MODULE = "PROMPT_BUILDER"

# Pre-compiled, case-insensitive table+field finder
def _has_field(schema_str: str, table: str, field: str) -> bool:
    pat = re.compile(rf"(?i)Table:\s*`?{re.escape(table)}`?.*?\b{re.escape(field)}\b", flags=re.DOTALL)
    return bool(pat.search(schema_str or ""))

def _schema_fingerprint(schema_str: str) -> str:
    try:
        return hashlib.sha256((schema_str or "").encode("utf-8")).hexdigest()[:12]
    except Exception:
        return "na"

def _maybe_trim_schema(schema_str: str) -> str:
    if not schema_str:
        return ""
    if len(schema_str) <= MAX_SCHEMA_CHARS:
        return schema_str
    head = schema_str[:MAX_SCHEMA_CHARS]
    note = f"\n\n-- NOTE: Schema truncated to {MAX_SCHEMA_CHARS} characters for brevity --"
    return head + note

def _default_core_instructions() -> str:
    core: List[str] = [
        "You are a backend analytics agent.",
        "Generate a single efficient SQL SELECT query using ONLY the tables/fields from the schema below.",
        "Do NOT invent tables/columns.",
        "Avoid Cartesian products; only join when necessary and explicitly indicated by the schema.",
    ]
    if INCLUDE_DATE_GUIDANCE:
        core.append(
            "If the question implies a time range (e.g., 'today', 'last week', 'last month'), convert it into explicit "
            "predicates using placeholders like [start] and [end]."
        )
    if INCLUDE_LIMIT_HINT:
        core.append(f"Prefer including an explicit LIMIT {DEFAULT_LIMIT_HINT} unless the query naturally aggregates to few rows.")
    return " ".join(core)

def _final_constraints() -> str:
    return "\n".join([
        "OUTPUT RULES:",
        "- Return ONLY the SQL query (no prose, comments, or markdown).",
        "- The query MUST be a single SELECT statement.",
        "- Do NOT use DDL/DML (CREATE/INSERT/UPDATE/DELETE/ALTER/TRUNCATE).",
        "- Use only columns/tables that exist in the schema.",
        "- Use explicit date predicates with [start]/[end] placeholders when the question implies a timeframe.",
    ])

def build_llm_prompt(
    user_question: str,
    schema_str: str,
    instructions: Optional[str] = None,
    examples: Optional[str] = None,
    debug: bool = True
) -> str:
    """
    Build the final prompt for SQL generation.

    Emits:
      - [meta] header
      - instructions (default or provided)
      - trimmed schema
      - optional field hints
      - user question
      - optional examples
      - final constraints

    Returns: str
    """
    schema_str = schema_str or ""
    schema_fp = _schema_fingerprint(schema_str)
    schema_trimmed = _maybe_trim_schema(schema_str)

    sections: List[str] = []

    # 0) Header for traceability
    sections.append(
        f"[meta] prompt_version={PROMPT_VERSION} schema_fp={schema_fp} generated_at={datetime.datetime.utcnow().isoformat()}Z"
    )

    # 1) Core instructions
    sections.append(instructions or _default_core_instructions())

    # 2) Database schema (trimmed)
    sections.append("Database schema:\n" + schema_trimmed)

    # 3) Field-specific safety/transform hints (conditional)
    if _has_field(schema_str, "support_ticket", "resolution_time"):
        sections.append(
            "IMPORTANT FIELD NOTE:\n"
            "- support_ticket.resolution_time is a string in 'MM:SS'. To aggregate, convert to seconds using:\n"
            "  (SPLIT_PART(resolution_time, ':', 1)::int * 60 + SPLIT_PART(resolution_time, ':', 2)::int)\n"
            "  Use this inside AVG/SUM as needed."
        )

    # 4) User question
    sections.append(f'User question:\n"{user_question}"')

    # 5) Example queries (optional)
    if examples:
        sections.append("Example queries:\n" + examples)

    # 6) Final guardrails
    sections.append(_final_constraints())

    prompt = "\n\n".join(sections)

    if debug:
        # Bounded preview (no PII redaction here—this is a developer-facing debug gated by flag)
        try:
            snippet = prompt[:1200]
            suffix = " ...[truncated]" if len(prompt) > 1200 else ""
            log.dbg(MODULE, "LLM PROMPT PREVIEW", snippet + suffix)
        except Exception:
            pass

    return prompt
