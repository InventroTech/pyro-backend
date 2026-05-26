"""Shared SQL fragments for ``records.data`` JSONB field lookups."""

# ``->>`` yields text; backfill / float JSON may store "1.0". ::numeric::int accepts both.
CALL_ATTEMPTS_INT_EXPR = "COALESCE((data->>'call_attempts')::numeric::int, 0)"
