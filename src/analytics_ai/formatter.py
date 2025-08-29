import pprint
import math
import datetime
from decimal import Decimal

from .utils.json_safe import coerce_json_safe

MODULE = "FORMAT"


def _infer_type(sample):
    if sample is None:
        return "null"
    if isinstance(sample, bool):
        return "boolean"
    if isinstance(sample, (int, float, Decimal)):
        return "number"
    if isinstance(sample, (datetime.date, datetime.datetime)):
        return "date"
    return "string"


def _detect_columns_union(results):
    """
    Keep first-row key order, then append any new keys seen later (alpha-sorted)
    to keep deterministic column order.
    """
    first_row = next((r for r in results if isinstance(r, dict)), None)
    if not first_row:
        return []
    base_cols = list(first_row.keys())
    extra_cols = set()
    for r in results:
        if isinstance(r, dict):
            for k in r.keys():
                if k not in base_cols:
                    extra_cols.add(k)
    return base_cols + sorted(extra_cols)


def format_results_for_table(results, max_rows=100, *, extras=False):
    """
    Formats a list of dict results for tabular frontend consumption.

    Returns (unchanged shape):
      { "type": "table", "columns": [...], "rows": [[...],[...]], "message": str|None }

    If extras=True, adds:
      - meta: { row_count, columns_count, generated_at }
      - column_types: { col: "number"|"date"|"string"|"boolean"|"null" }
      - summary: { totals: { numeric_col: sum } }
    """
    # Safe bounded preview
    try:
        preview = str(results)[:300]
    except Exception:
        preview = "<unprintable>"
    print(f"[{MODULE}] Raw results type: {type(results)} | Value: {preview}")

    if not results or not isinstance(results, list):
        print(f"[{MODULE}] Results is empty or not a list.")
        return {"type": "table", "columns": [], "rows": [], "message": "No results found."}

    # columns
    columns = _detect_columns_union(results)
    if not columns:
        print(f"[{MODULE}] Malformed results: no dict rows.")
        return {"type": "table", "columns": [], "rows": [], "message": "Malformed results: no valid data rows."}

    print(f"[{MODULE}] Columns detected (union): {columns}")
    try:
        pprint.pprint(results[:3], width=120, compact=True)
    except Exception:
        print(f"[{MODULE}] Could not pretty print preview rows.")

    # rows (bounded by max_rows)
    table_rows = []
    for idx, row in enumerate(results[:max_rows]):
        if not isinstance(row, dict):
            print(f"[{MODULE}] Skipping non-dict row at index {idx}: {row}")
            continue
        table_rows.append([coerce_json_safe(row.get(col)) for col in columns])

    message = None
    if len(results) > max_rows:
        message = f"Showing only first {max_rows} rows out of {len(results)} total."
        print(f"[{MODULE}] {message}")

    try:
        print(f"[{MODULE}] First 2 formatted rows: {table_rows[:2]}")
    except Exception:
        pass

    payload = {
        "type": "table",
        "columns": columns,
        "rows": table_rows,
        "message": message,
    }

    if extras:
        # infer column types (first non-None)
        column_types = {}
        for c in columns:
            sample_val = None
            for r in results:
                if isinstance(r, dict) and r.get(c) is not None:
                    sample_val = r.get(c)
                    break
            column_types[c] = _infer_type(sample_val)

        # numeric totals (bounded to max_rows)
        totals = {}
        for col_idx, c in enumerate(columns):
            if column_types[c] == "number":
                s = 0.0
                for r in results[:max_rows]:
                    if isinstance(r, dict):
                        v = r.get(c)
                        if isinstance(v, Decimal):
                            v = float(v)
                        if isinstance(v, (int, float)) and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                            s += v
                totals[c] = s

        payload["column_types"] = column_types
        payload["meta"] = {
            "row_count": len(results),
            "columns_count": len(columns),
            "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        }
        if any(t == "number" for t in column_types.values()):
            payload["summary"] = {"totals": totals}

    return payload
