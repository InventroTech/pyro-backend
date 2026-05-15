"""
sync_dispatch_to_records job
============================

Reads raw dispatch rows from the Airbyte-managed source table
``dispatch_dataDispatchData`` (full-refresh every 8 hours from a Google Sheet),
restructures them, and upserts them into ``records`` as
``entity_type='dispatch_request'`` rows for a single tenant.

Storage decisions
-----------------
The ``records`` table does not have ``source_row_id`` / ``synced_at`` columns,
so both live inside the JSONB ``data`` blob:

    data.source_row_id   -> Google Sheet DC# (column_B), used as the upsert key
    data.synced_at       -> ISO timestamp of the most recent sync

Soft delete
-----------
Rows that disappear from the source sheet are soft-deleted on the records side
(``is_deleted=True``, ``deleted_at=now()``) rather than hard-deleted. If a row
re-appears in a later sync we revive it (``is_deleted=False``, ``deleted_at=None``).
"""
from __future__ import annotations

import logging
import traceback
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional

from django.db import connection, transaction
from django.utils import timezone

from crm_records.models import Record

from .job_handlers import JobHandler
from .models import BackgroundJob

logger = logging.getLogger(__name__)


# =====================================================================
# Configuration
# =====================================================================

# TODO(tenant): replace with the real tenant UUID for the dispatch tenant.
# Hardcoded per agreement; change here when you have the value.
DISPATCH_TENANT_ID = "00000000-0000-0000-0000-000000000000"

# Confirmed entity_type for dispatch rows in the records table.
DISPATCH_ENTITY_TYPE = "dispatch_request"

# Source table name (Airbyte uses the sheet's tab name verbatim, mixed case).
# Quoted in raw SQL so Postgres preserves casing.
SOURCE_TABLE = "dispatch_dataDispatchData"

# Airbyte metadata columns we never map to the destination data dict.
_AIRBYTE_META_COLUMNS = {
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta",
    "_airbyte_generation_id",
}

# (source column name, destination data key, type tag).
# Listed in the same groups as the spec for readability.
_FIELD_MAP: List[tuple] = [
    # CORE ORDER INFO
    ("column_A", "sr_no", "str"),
    ("column_B", "dc_number", "str"),
    ("column_C", "dc_date", "date"),
    ("column_D", "account_name", "str"),
    ("column_E", "products", "str"),
    ("column_F", "terms", "str"),
    ("column_G", "quantity", "int"),
    ("column_H", "amount", "decimal"),
    ("column_I", "po_number", "str"),
    ("column_J", "po_date", "date"),
    ("column_K", "engineer", "str"),
    ("column_L", "sales_order_number", "str"),
    ("column_M", "consignee_city", "str"),
    ("column_N", "serial_numbers", "str"),
    ("column_R", "remarks", "str"),
    ("column_S", "dc_received_in_office", "bool"),
    # LOGISTICS / GODOWN
    ("Godown_O1", "date_of_material_dispatch", "date"),
    ("Godown_P1", "date_dispatch_godown_dc_to_office", "date"),
    ("Godown_Q1", "date_scanned_copy_dc_to_office", "date"),
    ("Godown_U1", "e_way_bill_number", "str"),
    ("Godown_W1", "transporter_name", "str"),
    ("Godown_X1", "vehicle_number", "str"),
    ("GODOWN_AU1", "godown_in_time", "str"),
    ("GODOWN_AV1", "godown_out_time", "str"),
    ("Godown_AD1", "date_lr_dispatch_to_office", "date"),
    ("Godown___Check", "e_way_updated_in_server", "str"),
    # FREIGHT / LR
    ("ArvindG_Y1", "lr_number", "str"),
    ("ArvindG_Z1", "lr_date", "date"),
    ("ArvindG_AA1", "freight_mode", "str"),
    ("ArvindG_AB1", "freight_amount", "decimal"),
    ("ArvindG_AC1", "date_delivery_at_consignee", "date"),
    ("ArvindG_AF1", "date_email_vehicle_dispatch_details", "date"),
    ("Umesh_AE1", "lr_received_in_office", "str"),
    # CUSTOMER COMMUNICATION
    ("Tulsi_AI1", "date_email_inv_details", "date"),
    ("Tulsi_AJ1", "date_email_tc_details", "date"),
    ("Tulsi_AK1", "date_courier_to_customer", "date"),
    # SIS / CTF
    ("Umesh_AL1", "sis_ctf_pump_model", "str"),
    ("Umesh_AM1", "sis_ctf_model_serial_number", "str"),
    ("Umesh_AN1", "sis_ctf_crm_number", "str"),
    ("Umesh_AO1", "sis_ctf_date", "date"),
    ("Umesh_AP1", "sis_ctf_done", "str"),
    ("Umesh_AQ1", "sis_ctf_mail", "bool"),
    # WARRANTY / CHECKS
    ("column_AH", "e_warranty_number", "str"),
    ("Akshay", "e_warranty_updated_date", "date"),
    ("Umesh_Akshay", "dc_in_office", "bool"),
    ("column_AR", "note", "str"),
    # VERIFICATION
    ("DarshanS_AS1", "checked_gather", "date"),
    ("DarshanS_AT1", "barcode", "date"),
]


# =====================================================================
# Field-level transformers
# =====================================================================

_DATE_FORMATS = ("%d-%b-%y", "%d-%b-%Y")


def _clean(value: Any) -> Optional[str]:
    """Strip whitespace, treat empty string as None. Stringify non-strings."""
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        return s if s else None
    # Some Airbyte source columns can come back as datetime/date/numeric directly.
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value).strip() or None


def _to_str(value: Any) -> Optional[str]:
    return _clean(value)


def _to_date(value: Any) -> Optional[str]:
    """
    Parse "07-Apr-26" / "30-Mar-26" (dd-Mon-yy) to ISO "YYYY-MM-DD".
    Anything unparseable -> None (logged at DEBUG).
    """
    raw = _clean(value)
    if raw is None:
        return None
    # Already ISO? Accept it.
    if isinstance(value, (date, datetime)):
        return (value.date() if isinstance(value, datetime) else value).isoformat()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    logger.debug("[DispatchSync] Unparseable date value %r", raw)
    return None


def _to_int(value: Any) -> Optional[int]:
    raw = _clean(value)
    if raw is None:
        return None
    try:
        # Allow "12.0" / "12" -> 12.
        return int(float(raw.replace(",", "")))
    except (TypeError, ValueError):
        logger.debug("[DispatchSync] Unparseable int value %r", raw)
        return None


def _to_decimal(value: Any) -> Optional[float]:
    raw = _clean(value)
    if raw is None:
        return None
    cleaned = raw.replace(",", "")
    try:
        return float(Decimal(cleaned))
    except (InvalidOperation, ValueError):
        logger.debug("[DispatchSync] Unparseable decimal value %r", raw)
        return None


def _to_bool(value: Any) -> Optional[bool]:
    raw = _clean(value)
    if raw is None:
        return None
    upper = raw.upper()
    if upper == "TRUE":
        return True
    if upper == "FALSE":
        return False
    logger.debug("[DispatchSync] Unparseable bool value %r", raw)
    return None


_TRANSFORMERS = {
    "str": _to_str,
    "date": _to_date,
    "int": _to_int,
    "decimal": _to_decimal,
    "bool": _to_bool,
}


# =====================================================================
# Step 1 — fetch
# =====================================================================

def _fetch_source_rows() -> List[Dict[str, Any]]:
    """
    Pull all non-header rows from the Airbyte source table.

    Returns a list of dicts keyed by source column name.
    """
    sql = (
        f'SELECT * FROM "{SOURCE_TABLE}" '
        f'WHERE "column_B" IS NOT NULL AND "column_B" <> %s'
    )
    with connection.cursor() as cursor:
        cursor.execute(sql, ["DC# No"])
        col_names = [c[0] for c in cursor.description]
        return [dict(zip(col_names, row)) for row in cursor.fetchall()]


# =====================================================================
# Step 2 — transform
# =====================================================================

def _transform_row(row: Dict[str, Any], synced_at_iso: str) -> Optional[Dict[str, Any]]:
    """
    Map a single source row to the records-table-shaped dict.

    Per-field parse failures are swallowed (None stored for that field) so a
    single bad cell doesn't fail the whole job. Returns None only when the
    upsert key (column_B / dc_number) is missing — those rows can't be
    keyed, so we skip them with a warning.
    """
    source_row_id = _clean(row.get("column_B"))
    if not source_row_id:
        return None

    data: Dict[str, Any] = {}
    for src_col, dest_key, type_tag in _FIELD_MAP:
        if src_col in _AIRBYTE_META_COLUMNS:
            continue
        try:
            data[dest_key] = _TRANSFORMERS[type_tag](row.get(src_col))
        except Exception:  # pragma: no cover — extreme paranoia, transformers swallow
            logger.exception(
                "[DispatchSync] Transformer crashed for column=%s key=%s; storing None",
                src_col, dest_key,
            )
            data[dest_key] = None

    # Embedded keys (recorded in data because the records table has no real columns for them).
    data["source_row_id"] = source_row_id
    data["synced_at"] = synced_at_iso

    return {
        "source_row_id": source_row_id,
        "tenant_id": DISPATCH_TENANT_ID,
        "entity_type": DISPATCH_ENTITY_TYPE,
        "is_deleted": False,
        "deleted_at": None,
        "synced_at": synced_at_iso,
        "pyro_data": {},
        "data": data,
    }


# =====================================================================
# Step 3 — upsert
# =====================================================================

def _upsert_records(transformed: Iterable[Dict[str, Any]], now) -> int:
    """
    Upsert each transformed row keyed by data->>'source_row_id'.

    Uses ``Record.all_objects`` so we can revive previously soft-deleted rows
    when the same DC# re-appears in the sheet.

    Returns the number of rows upserted (created + updated).
    """
    count = 0
    for payload in transformed:
        source_row_id = payload["source_row_id"]
        existing = (
            Record.all_objects
            .filter(
                tenant_id=DISPATCH_TENANT_ID,
                entity_type=DISPATCH_ENTITY_TYPE,
                data__source_row_id=source_row_id,
            )
            .first()
        )
        if existing is not None:
            existing.data = payload["data"]
            existing.pyro_data = payload["pyro_data"]
            existing.is_deleted = False
            existing.deleted_at = None
            existing.updated_at = now
            existing.save(
                update_fields=[
                    "data",
                    "pyro_data",
                    "is_deleted",
                    "deleted_at",
                    "updated_at",
                ]
            )
        else:
            Record.objects.create(
                tenant_id=DISPATCH_TENANT_ID,
                entity_type=DISPATCH_ENTITY_TYPE,
                data=payload["data"],
                pyro_data=payload["pyro_data"],
                is_deleted=False,
                deleted_at=None,
            )
        count += 1
    return count


# =====================================================================
# Step 4 — soft delete
# =====================================================================

def _soft_delete_missing(seen_source_row_ids: set, now) -> int:
    """
    Soft-delete still-alive dispatch records whose source_row_id is no longer
    present in the sheet. Updates ``data.synced_at`` on those rows so we have
    an audit of when they were marked deleted.
    """
    qs = (
        Record.objects
        .filter(
            tenant_id=DISPATCH_TENANT_ID,
            entity_type=DISPATCH_ENTITY_TYPE,
            is_deleted=False,
        )
        .extra(where=["data->>'source_row_id' IS NOT NULL"])
    )

    deleted = 0
    synced_at_iso = now.isoformat()
    for record in qs.iterator(chunk_size=500):
        data = record.data if isinstance(record.data, dict) else {}
        source_row_id = data.get("source_row_id")
        if source_row_id is None or source_row_id in seen_source_row_ids:
            continue
        new_data = {**data, "synced_at": synced_at_iso}
        record.data = new_data
        record.is_deleted = True
        record.deleted_at = now
        record.updated_at = now
        record.save(
            update_fields=[
                "data",
                "is_deleted",
                "deleted_at",
                "updated_at",
            ]
        )
        deleted += 1
    return deleted


# =====================================================================
# Public entrypoint
# =====================================================================

def run_dispatch_sync() -> Dict[str, int]:
    """
    Execute the four-step sync. Importable from the management command and
    the JobHandler. Re-raises on any error after logging the traceback so the
    job scheduler marks the run as failed.

    Returns a dict with counts: ``{"fetched": n, "upserted": n, "soft_deleted": n}``.
    """
    logger.info("[DispatchSync] Starting dispatch sync")
    try:
        # Settings has TIME_ZONE='UTC' and USE_TZ=False, so timezone.now() is naive UTC.
        now = timezone.now()
        synced_at_iso = now.isoformat()

        rows = _fetch_source_rows()
        logger.info(
            "[DispatchSync] Fetched %s rows from %s", len(rows), SOURCE_TABLE
        )

        transformed: List[Dict[str, Any]] = []
        skipped = 0
        for row in rows:
            payload = _transform_row(row, synced_at_iso)
            if payload is None:
                skipped += 1
                continue
            transformed.append(payload)
        if skipped:
            logger.warning(
                "[DispatchSync] Skipped %s source row(s) with empty column_B", skipped
            )

        with transaction.atomic():
            upserted = _upsert_records(transformed, now)
            logger.info("[DispatchSync] Upserted %s rows into records", upserted)

            seen_ids = {p["source_row_id"] for p in transformed}
            soft_deleted = _soft_delete_missing(seen_ids, now)
            logger.info("[DispatchSync] Soft deleted %s rows", soft_deleted)

        logger.info("[DispatchSync] Dispatch sync completed successfully")
        return {
            "fetched": len(rows),
            "upserted": upserted,
            "soft_deleted": soft_deleted,
            "skipped": skipped,
        }
    except Exception:
        logger.error(
            "[DispatchSync] Dispatch sync failed:\n%s", traceback.format_exc()
        )
        raise


# =====================================================================
# JobHandler integration
# =====================================================================

class SyncDispatchToRecordsJobHandler(JobHandler):
    """Wraps :func:`run_dispatch_sync` for the background-jobs queue."""

    def process(self, job: BackgroundJob) -> bool:
        stats = run_dispatch_sync()
        job.result = {
            "success": True,
            "fetched": stats["fetched"],
            "upserted": stats["upserted"],
            "soft_deleted": stats["soft_deleted"],
            "skipped": stats["skipped"],
            "timestamp": timezone.now().isoformat(),
        }
        return True

    def get_retry_delay(self, attempt: int) -> int:
        # Sync is heavy, retries should be modest. Mirrors lead-cron handlers.
        delays = [60, 300, 900]
        return delays[min(attempt - 1, len(delays) - 1)]

    def validate_payload(self, payload: Dict[str, Any]) -> bool:  # noqa: D401
        # No payload required.
        return True
