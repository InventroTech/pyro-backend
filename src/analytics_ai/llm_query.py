import os
import traceback
import re
import json
import time
import random
from typing import Optional, Tuple

import openai

from .utils.redaction import redact_pii
from .utils.text import sha12, truncate, safe_json
from .utils import log as log

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# ---- Tunables ----
PRIMARY_MODEL = os.getenv("LLM_PRIMARY_MODEL", "gpt-4.1")
FALLBACK_MODELS = [m.strip() for m in os.getenv("LLM_FALLBACK_MODELS", "gpt-4o-mini").split(",") if m.strip()]  # tried in order if primary fails
DEFAULT_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.0"))
MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "512"))
MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))
BASE_BACKOFF = float(os.getenv("LLM_BASE_BACKOFF", "0.8"))  # seconds
MAX_BACKOFF = float(os.getenv("LLM_MAX_BACKOFF", "6.0"))    # seconds
LOG_PROMPT_SNIPPET_CHARS = int(os.getenv("LLM_LOG_PROMPT_SNIPPET", "240"))

MODULE = "LLM"


def _sleep_backoff(attempt: int) -> None:
    # Exponential backoff with jitter
    delay = min(MAX_BACKOFF, BASE_BACKOFF * (2 ** (attempt - 1)))
    delay = delay * (0.6 + 0.8 * random.random())
    time.sleep(delay)


_SELECT_FINDER = re.compile(r"(?is)\bselect\b.*?(?=;|$)")

def _extract_first_select_block(text: str) -> str:
    """
    Extract the first SELECT ... statement; strip code fences/comments/trailing junk.
    Return "" if none found.
    """
    if not text:
        return ""

    original = text.strip()

    # Strip common fenced code wrappers
    text = re.sub(r"^```sql\s*", "", original, flags=re.IGNORECASE | re.MULTILINE)
    text = re.sub(r"^```\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"```$", "", text)

    # Strip leading labels like "SQL:" or "Here is the query:"
    text = re.sub(r"^\s*(here.*?query|sql\s*[:\-])\s*", "", text, flags=re.IGNORECASE)

    # Remove SQL line + block comments
    text = re.sub(r"--.*?$", "", text, flags=re.MULTILINE)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)

    # Normalize whitespace
    text = re.sub(r"[ \t]+", " ", text).strip()

    m = _SELECT_FINDER.search(text)
    if not m:
        return ""

    candidate = m.group(0).strip().rstrip(";").strip()
    return candidate


def clean_llm_sql_output(sql_text: str) -> str:
    """
    Legacy-compatible name. Returns cleaned first SELECT statement or "".
    """
    cleaned = _extract_first_select_block(sql_text)
    # Safe diagnostics (debug level)
    try:
        log.dbg(MODULE, "SQL CLEANER original (trimmed)", str(sql_text)[:300], mask_pii=True)
        log.dbg(MODULE, "SQL CLEANER extracted SELECT", cleaned, mask_pii=True)
    except Exception:
        pass
    return cleaned


def _call_openai(prompt: str, model: str, temperature: float):
    """
    Low-level single attempt to call OpenAI Chat Completions.
    Returns the raw response object.
    """
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    t0 = time.perf_counter()
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an expert backend SQL query generator. "
                    "ONLY output a single valid SQL SELECT query. "
                    "Do NOT include explanations, comments, or markdown."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
        max_tokens=MAX_TOKENS,
    )
    dt_ms = int((time.perf_counter() - t0) * 1000)
    usage = getattr(resp, "usage", None)
    try:
        usage_dump = getattr(usage, "model_dump", lambda: {})() if usage else None
        log.dbg(MODULE, f"model={model} t_ms={dt_ms} usage", usage_dump)
    except Exception:
        pass
    return resp


def _try_models_with_retries(prompt: str, model: str, temperature: float) -> Tuple[Optional[str], object | str]:
    """
    Try primary model with retries, then fall back to alternatives (each with retries).
    Returns (cleaned_sql, response_or_error_string).
    """
    models_to_try = [model] + [m for m in FALLBACK_MODELS if m and m != model]
    last_exc: Optional[BaseException] = None

    for m in models_to_try:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = _call_openai(prompt, m, temperature)
                content = resp.choices[0].message.content.strip() if resp and resp.choices else ""
                cleaned = clean_llm_sql_output(content)

                if not cleaned:
                    raise ValueError("Empty SQL after cleaning")

                if not cleaned.lower().startswith("select"):
                    log.dbg(MODULE, "Cleaned SQL does not start with SELECT (may be invalid)", cleaned, mask_pii=True)

                return cleaned, resp

            except Exception as e:
                last_exc = e
                log.dbg(MODULE, f"attempt {attempt}/{MAX_RETRIES} failed for model {m}", repr(e))
                if attempt < MAX_RETRIES:
                    _sleep_backoff(attempt)
                # else move to next model

    err_str = f"LLM failed after retries: {repr(last_exc)}"
    log.err(MODULE, err_str, exc=last_exc)
    return None, err_str


def get_sql_from_llm(prompt: str, model: str = PRIMARY_MODEL, temperature: float = DEFAULT_TEMPERATURE):
    """
    Calls the LLM to get a SQL query based on the given prompt.
    Returns (sql_query, raw_response_or_error_string).
    """
    if not OPENAI_API_KEY:
        log.err(MODULE, "OPENAI_API_KEY not set")
        return None, None

    # Safer diagnostics: never log full prompt; log hash + small redacted snippet
    try:
        prompt_hash = sha12(prompt)
        prompt_snippet = redact_pii(prompt[:LOG_PROMPT_SNIPPET_CHARS])
        log.dbg(MODULE, f"Prompt hash={prompt_hash} snippet", json.dumps(prompt_snippet))
    except Exception:
        pass

    try:
        sql, resp = _try_models_with_retries(prompt, model, temperature)

        if sql:
            try:
                log.dbg(MODULE, "SQL (clean)", sql, mask_pii=True)
            except Exception:
                pass
        else:
            log.dbg(MODULE, "No SQL generated.")

        # Safely preview raw response (truncated)
        try:
            dump = getattr(resp, "model_dump", None)
            if callable(dump):
                log.dbg(MODULE, "Raw response (truncated)", safe_json(dump())[:1000])
            else:
                log.dbg(MODULE, "Raw response (repr, truncated)", repr(resp)[:1000])
        except Exception:
            pass

        return sql, resp

    except Exception as e:
        log.err(MODULE, f"Exception while calling LLM: {repr(e)}", exc=e)
        # Preserve legacy behavior
        try:
            traceback.print_exc()
        except Exception:
            pass
        return None, str(e)
