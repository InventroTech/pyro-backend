"""
Dispatch Data Sync Job
======================
Reads raw dispatch rows from the Airbyte-managed source table
``dispatch_dataDispatchData`` (synced from a Google Sheet),
transforms them, and upserts into the ``records`` table as
entity_type='dispatch_request' rows.

Soft deletes rows that disappear from the source sheet.
"""
from __future__ import annotations

import logging
import os
import re
import traceback
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional

from django.db import connection, transaction
from django.utils import timezone

from crm_records.models import Record

logger = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────

_PLACEHOLDER_TENANT_ID = "00000000-0000-0000-0000-000000000000"
DISPATCH_TENANT_ID = (os.getenv("DISPATCH_SYNC_TENANT_ID") or "").strip() or _PLACEHOLDER_TENANT_ID
DISPATCH_ENTITY_TYPE = "dispatch_request"
SOURCE_TABLE = "dispatch_dataDispatchData"

_AIRBYTE_META_COLUMNS = {
    "_airbyte_raw_id",
    "_airbyte_extracted_at",
    "_airbyte_meta",
    "_airbyte_generation_id",
}

_FIELD_SPECS: List[tuple] = [
    (("sr no", "s no", "srno"), "sr_no", "str"),
    (("dc no", "dc number"), "dc_number", "str"),
    (("dc date",), "dc_date", "date"),
    (("account name", "party name", "customer name"), "account_name", "str"),
    (("products", "product"), "products", "str"),
    (("terms", "payment terms"), "terms", "str"),
    (("quantity", "qty"), "quantity", "int"),
    (("amount", "value"), "amount", "decimal"),
    (("po number", "po no", "p o number", "po"), "po_number", "str"),
    (("po date",), "po_date", "date"),
    (("engineer", "engg"), "engineer", "str"),
    (("sales order number", "so number", "sales order no", "salesorder"), "sales_order_number", "str"),
    (("consignee city", "city"), "consignee_city", "str"),
    (("serial numbers", "serial"), "serial_numbers", "str"),
    (("remarks", "remark"), "remarks", "str"),
    (("dc received in office", "dc recd in office"), "dc_received_in_office", "bool"),
    (("date of material dispatch",), "date_of_material_dispatch", "date"),
    (("date dispatch godown dc to office", "date of dispatch of godown dc to office"), "date_dispatch_godown_dc_to_office", "date"),
    (("date scanned copy dc to office", "date of scanned copy dc sent to office"), "date_scanned_copy_dc_to_office", "date"),
    (("e way bill number", "eway bill number", "e way bill no"), "e_way_bill_number", "str"),
    (("transporter name", "transporter courier name"), "transporter_name", "str"),
    (("vehicle number", "vehicle no"), "vehicle_number", "str"),
    (("godown in time", "in time"), "godown_in_time", "str"),
    (("godown out time", "out time"), "godown_out_time", "str"),
    (("date lr dispatch to office", "date of dispatch of l r to office"), "date_lr_dispatch_to_office", "date"),
    (("e way updated in server",), "e_way_updated_in_server", "str"),
    (("lr number", "l r no"), "lr_number", "str"),
    (("lr date", "l r date"), "lr_date", "date"),
    (("freight mode",), "freight_mode", "str"),
    (("freight amount",), "freight_amount", "decimal"),
    (("date delivery at consignee", "date of delivery at consignee"), "date_delivery_at_consignee", "date"),
    (("date email vehicle dispatch details", "date of email to customer with vehicle dispatch details"), "date_email_vehicle_dispatch_details", "date"),
    (("lr received in office", "lr recd in office"), "lr_received_in_office", "str"),
    (("date email inv details", "invoice email date", "date of email to customer with inv details"), "date_email_inv_details", "date"),
    (("date email tc details", "date of email to customer with tc details"), "date_email_tc_details", "date"),
    (("date courier to customer", "date of courier sent to customer"), "date_courier_to_customer", "date"),
    (("sis ctf pump model",), "sis_ctf_pump_model", "str"),
    (("sis ctf model serial number", "model serial no"), "sis_ctf_model_serial_number", "str"),
    (("sis ctf crm number", "sis ctf crm no"), "sis_ctf_crm_number", "str"),
    (("sis ctf date",), "sis_ctf_date", "date"),
    (("sis ctf done",), "sis_ctf_done", "str"),
    (("sis ctf mail",), "sis_ctf_mail", "bool"),
    (("e warranty number", "ewarranty number", "e warranty"), "e_warranty_number", "str"),
    (("e warranty updated date",), "e_warranty_updated_date", "date"),
    (("dc in office",), "dc_in_office", "bool"),
    (("note", "notes"), "note", "str"),
    (("checked gather",), "checked_gather", "date"),
    (("barcode",), "barcode", "date"),
]

_LEGACY_FIELD_MAP: List[tuple] = [
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
    ("ArvindG_Y1", "lr_number", "str"),
    ("ArvindG_Z1", "lr_date", "date"),
    ("ArvindG_AA1", "freight_mode", "str"),
    ("ArvindG_AB1", "freight_amount", "decimal"),
    ("ArvindG_AC1", "date_delivery_at_consignee", "date"),
    ("ArvindG_AF1", "date_email_vehicle_dispatch_details", "date"),
    ("Umesh_AE1", "lr_received_in_office", "str"),
    ("Tulsi_AI1", "date_email_inv_details", "date"),
    ("Tulsi_AJ1", "date_email_tc_details", "date"),
    ("Tulsi_AK1", "date_courier_to_customer", "date"),
    ("Umesh_AL1", "sis_ctf_pump_model", "str"),
    ("Umesh_AM1", "sis_ctf_model_serial_number", "str"),
    ("Umesh_AN1", "sis_ctf_crm_number", "str"),
    ("Umesh_AO1", "sis_ctf_date", "date"),
    ("Umesh_AP1", "sis_ctf_done", "str"),
    ("Umesh_AQ1", "sis_ctf_mail", "bool"),
    ("column_AH", "e_warranty_number", "str"),
    ("Akshay", "e_warranty_updated_date", "date"),
    ("Umesh_Akshay", "dc_in_office", "bool"),
    ("column_AR", "note", "str"),
    ("DarshanS_AS1", "checked_gather", "date"),
    ("DarshanS_AT1", "barcode", "date"),
]

_DC_HEADER_LABELS = frozenset({"dc no", "dc number"})


# ── Field transformers ────────────────────────────────────────────────

_DATE_FORMATS = ("%d-%b-%y", "%d-%b-%Y")


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        return s if s else None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value).strip() or None


def _to_str(value: Any) -> Optional[str]:
    return _clean(value)


def _to_date(value: Any) -> Optional[str]:
    raw = _clean(value)
    if raw is None:
        return None
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
        return int(float(raw.replace(",", "")))
    except (TypeError, ValueError):
        logger.debug("[DispatchSync] Unparseable int value %r", raw)
        return None


def _to_decimal(value: Any) -> Optional[float]:
    raw = _clean(value)
    if raw is None:
        return None
    try:
        return float(Decimal(raw.replace(",", "")))
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


def _normalize_header_label(value: Any) -> Optional[str]:
    raw = _clean(value)
    if raw is None:
        return None
    s = raw.lower().replace("#", " ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split()) or None


def _build_header_alias_index() -> Dict[str, tuple]:
    index: Dict[str, tuple] = {}
    for aliases, dest_key, type_tag in _FIELD_SPECS:
        for alias in aliases:
            norm = _normalize_header_label(alias)
            if norm:
                index[norm] = (dest_key, type_tag)
    return index


_HEADER_ALIAS_INDEX = _build_header_alias_index()


# ── Step 1: Fetch ─────────────────────────────────────────────────────

def _is_header_row(row: Dict[str, Any]) -> bool:
    for col_key, cell in row.items():
        if col_key in _AIRBYTE_META_COLUMNS:
            continue
        label = _normalize_header_label(cell)
        if label in _DC_HEADER_LABELS:
            return True
    return False


def _fetch_header_and_data_rows() -> tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    sql = f'SELECT * FROM "{SOURCE_TABLE}"'
    with connection.cursor() as cursor:
        cursor.execute(sql)
        col_names = [c[0] for c in cursor.description]
        all_rows = [dict(zip(col_names, row)) for row in cursor.fetchall()]

    header_row: Optional[Dict[str, Any]] = None
    data_rows: List[Dict[str, Any]] = []
    for row in all_rows:
        if _is_header_row(row):
            if header_row is None:
                header_row = row
            continue
        data_rows.append(row)

    return header_row, data_rows


# ── Step 2: Transform ─────────────────────────────────────────────────

def _legacy_column_mapping() -> Dict[str, tuple]:
    return {
        src_col: (dest_key, type_tag)
        for src_col, dest_key, type_tag in _LEGACY_FIELD_MAP
        if src_col not in _AIRBYTE_META_COLUMNS
    }


def _mapping_from_header_row(header_row: Dict[str, Any]) -> Dict[str, tuple]:
    header_map: Dict[str, tuple] = {}
    for col_key, cell in header_row.items():
        if col_key in _AIRBYTE_META_COLUMNS:
            continue
        label = _normalize_header_label(cell)
        if not label:
            continue
        spec = _HEADER_ALIAS_INDEX.get(label)
        if spec:
            header_map[col_key] = spec
    return header_map


def _build_column_mapping(header_row: Optional[Dict[str, Any]]) -> Dict[str, tuple]:
    mapping = _legacy_column_mapping()
    if header_row:
        header_map = _mapping_from_header_row(header_row)
        if header_map:
            mapping.update(header_map)
            return mapping
    return mapping


def _find_dc_column_key(header_row, col_mapping) -> Optional[str]:
    if header_row:
        for col_key, cell in header_row.items():
            if col_key in _AIRBYTE_META_COLUMNS:
                continue
            if _normalize_header_label(cell) in _DC_HEADER_LABELS:
                return col_key
    for col_key, (dest_key, _) in col_mapping.items():
        if dest_key == "dc_number":
            return col_key
    return "column_B"


def _row_dc_value(row: Dict[str, Any], dc_col_key: str) -> Optional[str]:
    raw = _clean(row.get(dc_col_key))
    if not raw:
        return None
    if _normalize_header_label(raw) in _DC_HEADER_LABELS:
        return None
    return raw


def _transform_row(row, col_mapping, dc_col_key, synced_at_iso) -> Optional[Dict[str, Any]]:
    source_row_id = _row_dc_value(row, dc_col_key)
    if not source_row_id:
        return None

    data: Dict[str, Any] = {}
    for src_col, (dest_key, type_tag) in col_mapping.items():
        if src_col in _AIRBYTE_META_COLUMNS:
            continue
        try:
            data[dest_key] = _TRANSFORMERS[type_tag](row.get(src_col))
        except Exception:
            data[dest_key] = None

    data["source_row_id"] = source_row_id
    data["synced_at"] = synced_at_iso
    if "dc_number" not in data or data.get("dc_number") is None:
        data["dc_number"] = source_row_id

    return {
        "source_row_id": source_row_id,
        "tenant_id": DISPATCH_TENANT_ID,
        "entity_type": DISPATCH_ENTITY_TYPE,
        "data": data,
    }


# ── Step 3: Upsert ────────────────────────────────────────────────────

def _upsert_records(transformed: Iterable[Dict[str, Any]], now) -> int:
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
            existing.is_deleted = False
            existing.deleted_at = None
            existing.updated_at = now
            existing.save(update_fields=["data", "is_deleted", "deleted_at", "updated_at"])
        else:
            Record.objects.create(
                tenant_id=DISPATCH_TENANT_ID,
                entity_type=DISPATCH_ENTITY_TYPE,
                data=payload["data"],
                is_deleted=False,
                deleted_at=None,
            )
        count += 1
    return count


# ── Step 4: Soft delete ───────────────────────────────────────────────

def _soft_delete_missing(seen_source_row_ids: set, now) -> int:
    qs = (
        Record.objects
        .filter(tenant_id=DISPATCH_TENANT_ID, entity_type=DISPATCH_ENTITY_TYPE, is_deleted=False)
        .extra(where=["data->>'source_row_id' IS NOT NULL"])
    )
    deleted = 0
    synced_at_iso = now.isoformat()
    for record in qs.iterator(chunk_size=500):
        data = record.data if isinstance(record.data, dict) else {}
        source_row_id = data.get("source_row_id")
        if source_row_id is None or source_row_id in seen_source_row_ids:
            continue
        record.data = {**data, "synced_at": synced_at_iso}
        record.is_deleted = True
        record.deleted_at = now
        record.updated_at = now
        record.save(update_fields=["data", "is_deleted", "deleted_at", "updated_at"])
        deleted += 1
    return deleted


# ── Public entrypoint ─────────────────────────────────────────────────

def run_dispatch_sync(payload: dict) -> None:
    tid = (DISPATCH_TENANT_ID or "").strip()
    if not tid or tid == _PLACEHOLDER_TENANT_ID:
        raise ValueError("DISPATCH_SYNC_TENANT_ID env variable is not set.")

    logger.info("[DispatchSync] Starting")
    try:
        now = timezone.now()
        synced_at_iso = now.isoformat()

        header_row, rows = _fetch_header_and_data_rows()
        col_mapping = _build_column_mapping(header_row)
        dc_col_key = _find_dc_column_key(header_row, col_mapping)

        logger.info("[DispatchSync] Fetched %s rows from source table", len(rows))

        transformed = []
        skipped = 0
        for row in rows:
            result = _transform_row(row, col_mapping, dc_col_key, synced_at_iso)
            if result is None:
                skipped += 1
            else:
                transformed.append(result)

        if skipped:
            logger.warning("[DispatchSync] Skipped %s rows with empty DC#", skipped)

        with transaction.atomic():
            upserted = _upsert_records(transformed, now)
            seen_ids = {p["source_row_id"] for p in transformed}
            soft_deleted = _soft_delete_missing(seen_ids, now)

        logger.info(
            "[DispatchSync] Done — fetched=%s upserted=%s soft_deleted=%s skipped=%s",
            len(rows), upserted, soft_deleted, skipped,
        )

    except Exception:
        logger.error("[DispatchSync] Failed:\n%s", traceback.format_exc())
        raise
