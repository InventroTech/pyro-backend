from typing import Iterable, List, Optional
from django.apps import apps
from django.db.models.fields.related import ForeignKey, OneToOneField, ManyToManyField
from django.db.models import Field, Model
from django.core.cache import cache as _dj_cache
import hashlib
import datetime
import json
import re

from .utils import log as log

MODULE = "SCHEMA"

def _schema_fp(models: Iterable[Model]) -> str:
    """Build a short fingerprint from model/field names to drive cache/debugging."""
    h = hashlib.sha256()
    try:
        for m in models:
            h.update(m._meta.db_table.encode())
            for f in m._meta.get_fields():
                if getattr(f, "auto_created", False) and not getattr(f, "concrete", False):
                    continue
                h.update(f.name.encode())
                h.update(f.__class__.__name__.encode())
        return h.hexdigest()[:12]
    except Exception:
        return "na"

def _field_flags(field: Field) -> str:
    """Return compact flags for a field (null/unique/index/len/default/choices)."""
    parts: List[str] = []
    try:
        if getattr(field, "null", False): parts.append("nullable")
        if getattr(field, "unique", False): parts.append("unique")
        if getattr(field, "db_index", False): parts.append("indexed")
        if hasattr(field, "max_length") and field.max_length: parts.append(f"len={field.max_length}")
        if field.has_default():
            default = field.default() if callable(field.default) else field.default
            parts.append(f"default={repr(default)[:40]}")
        if getattr(field, "choices", None):
            choices = [c[0] for c in field.choices][:6]
            parts.append(f"choices={choices}" + ("..." if len(field.choices) > 6 else ""))
    except Exception:
        # keep flags best-effort and quiet
        pass
    return ", ".join(parts)

def _rel_hint(field: Field) -> Optional[str]:
    """Hint like: → other_table.id (FK/OneToOne) or ↔ other_table (M2M)."""
    try:
        if isinstance(field, (ForeignKey, OneToOneField)):
            target = field.target_field.name if getattr(field, "target_field", None) else "id"
            return f"→ {field.related_model._meta.db_table}.{target}"
        if isinstance(field, ManyToManyField):
            return f"↔ {field.related_model._meta.db_table}"
    except Exception:
        return None
    return None

def _join_examples(model: Model) -> str:
    """Small, generic join examples derived from FK fields (max 3)."""
    bits: List[str] = []
    try:
        for f in model._meta.get_fields():
            if isinstance(f, (ForeignKey, OneToOneField)):
                tgt = f.related_model._meta.db_table
                src_tbl = model._meta.db_table
                src_col = f.column or f.name
                tgt_pk = f.target_field.column if getattr(f, "target_field", None) else "id"
                bits.append(f"-- JOIN example: {src_tbl} JOIN {tgt} ON {src_tbl}.{src_col} = {tgt}.{tgt_pk}")
                if len(bits) >= 3:
                    break
    except Exception:
        pass
    return "\n".join(bits)

def _table_block(model: Model, max_fields: int, include_relationships: bool, include_index_flags: bool) -> str:
    tbl = model._meta.db_table
    fields_out: List[str] = []
    shown = 0

    for field in model._meta.get_fields():
        if field.auto_created and not field.concrete:
            continue

        fname = field.name
        ftype = field.get_internal_type()
        help_txt = (getattr(field, "help_text", "") or "").strip()
        flags = _field_flags(field) if include_index_flags else ""
        rel = _rel_hint(field) if include_relationships else None

        line = f"- {fname} ({ftype})"
        extras: List[str] = []
        if flags: extras.append(flags)
        if rel: extras.append(rel)
        if help_txt: extras.append(help_txt)
        if extras:
            line += ": " + " | ".join(extras)
        fields_out.append(line)

        shown += 1
        if shown >= max_fields:
            fields_out.append(f"... and more fields (showing first {max_fields})")
            break

    # Table-specific extra notes (kept + extended)
    if tbl == "support_ticket":
        fields_out.append("- cse_name (CharField): Name of the agent (customer support executive) who handled the ticket.")
        fields_out.append("- assigned_to (UUIDField): User ID of the assigned agent; links to auth.users.id")
        # Conditional resolution_time hint
        has_resolution_time = any(re.match(r"^- *resolution_time\b", f) for f in fields_out) or any(
            getattr(f, "name", "") == "resolution_time" for f in model._meta.get_fields()
        )
        if has_resolution_time:
            fields_out.append(
                "- resolution_time (CharField): 'MM:SS' text; convert to seconds for aggregates using "
                "SPLIT_PART(resolution_time, ':', 1)::int * 60 + SPLIT_PART(resolution_time, ':', 2)::int"
            )

    block = (
        f"### Table: `{tbl}` (Django model: {model.__name__})\n"
        f"{(model.__doc__ or '').strip()}\n"
        "Fields:\n" + "\n".join(fields_out)
    )

    if include_relationships:
        join_hints = _join_examples(model)
        if join_hints:
            block += "\n" + join_hints

    return block

def generate_schema_summary(
    app_labels: Optional[List[str]] = None,
    debug: bool = True,
    max_fields: int = 30,
    *,
    include_relationships: bool = True,
    include_index_flags: bool = True,
    use_cache: bool = True,
    cache_ttl: int = 300,
    truncate_chars: int = 12000
) -> str:
    """
    Generate a compact, LLM-friendly schema summary string.

    Backward compatible defaults:
      - returns a string
      - respects app_labels and max_fields
      - prints debug lines when debug=True

    Enhancements:
      - caching (use_cache, cache_ttl)
      - field flags (nullable/unique/indexed/len/default/choices)
      - FK/M2M hints & join examples
      - header with fingerprint & timestamp
      - safe truncation for huge schemas
    """
    # Resolve + filter models
    models = apps.get_models()
    if app_labels:
        models = [m for m in models if m._meta.app_label in app_labels]

    # Stable order for deterministic output
    models = sorted(models, key=lambda m: m._meta.db_table)

    if debug:
        try:
            log.dbg(MODULE, "Generating schema for models", [m._meta.object_name for m in models])
        except Exception:
            pass

    # Cache lookup
    fp_seed = _schema_fp(models)
    cache_key = None
    if use_cache:
        key_bits = dict(
            app_labels=tuple(sorted(app_labels)) if app_labels else None,
            max_fields=max_fields,
            rel=include_relationships,
            idx=include_index_flags,
            fp=fp_seed,
        )
        try:
            cache_key = "schema_summary:" + hashlib.sha1(json.dumps(key_bits, sort_keys=True).encode()).hexdigest()
            cached = _dj_cache.get(cache_key)
            if cached:
                if debug:
                    log.dbg(MODULE, f"Using cached schema summary (key={cache_key})")
                return cached
        except Exception:
            # cache not configured/available—continue without failing
            pass

    schema_lines: List[str] = []
    header = f"[schema] fp={fp_seed} generated_at={datetime.datetime.utcnow().isoformat()}Z"
    schema_lines.append(header)

    for model in models:
        tbl = model._meta.db_table
        if tbl.startswith("django_"):  # skip Django system tables
            continue

        try:
            block = _table_block(model, max_fields, include_relationships, include_index_flags)
            schema_lines.append(block)
            if debug:
                log.dbg(MODULE, f"Added table block", tbl)
        except Exception:
            # Keep robust: skip problematic models rather than failing whole summary
            if debug:
                log.dbg(MODULE, "Skipping model due to exception", tbl)

    summary = "\n\n".join(schema_lines)

    # Truncate extremely long summaries to control prompt tokens
    if truncate_chars and len(summary) > truncate_chars:
        summary = summary[:truncate_chars] + f"\n\n-- NOTE: Schema truncated at {truncate_chars} characters --"

    if debug:
        try:
            snippet = summary[:1500]
            suffix = "..." if len(summary) > 1500 else ""
            log.dbg(MODULE, "FINAL GENERATED SCHEMA SUMMARY (preview)", snippet + suffix)
        except Exception:
            pass

    # Cache store
    if use_cache and cache_key:
        try:
            _dj_cache.set(cache_key, summary, cache_ttl)
        except Exception:
            pass

    return summary
