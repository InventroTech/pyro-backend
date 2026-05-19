#!/usr/bin/env python3
"""Regenerate prototype/data from Supabase sync CSV export."""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV = Path.home() / "Desktop" / "Supabase Snippet Fetch Latest Sync Job Status.csv"


def load_csv(path: Path) -> dict:
    records = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            data = json.loads(row["data"]) if row.get("data") else {}
            pyro_raw = row.get("pyro_data") or "{}"
            pyro = json.loads(pyro_raw) if pyro_raw not in ("", "{}") else {}
            records.append(
                {
                    "id": int(row["id"]),
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "entity_type": row["entity_type"],
                    "tenant_id": row["tenant_id"],
                    "is_deleted": str(row.get("is_deleted", "")).lower() == "true",
                    "deleted_at": row.get("deleted_at") or None,
                    "data": data,
                    "pyro_data": pyro,
                }
            )
    return {
        "source": path.name,
        "exported_at": records[-1]["updated_at"] if records else None,
        "records": records,
    }


def main() -> None:
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV
    if not csv_path.is_file():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    payload = load_csv(csv_path)
    data_dir = ROOT / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    (data_dir / "dispatch-sync-records.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    (data_dir / "dispatch-sync-records.js").write_text(
        "/** Auto-generated — run: python prototype/scripts/csv_to_data.py */\n"
        f"window.PYRO_SYNC_DATA = {json.dumps(payload, indent=2)};\n",
        encoding="utf-8",
    )
    print(f"OK: {len(payload['records'])} record(s) → {data_dir}")


if __name__ == "__main__":
    main()
