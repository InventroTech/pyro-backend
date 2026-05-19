"""
Tests for background_jobs.dispatch_sync (sync_dispatch_to_records).

Two layers:

1. ``DispatchTransformerTests`` — pure unit tests for the field-level
   transformers and ``_transform_row``. No DB required, so they run fast and
   pinpoint regressions in date/decimal/bool parsing without touching anything.

2. ``DispatchSyncEndToEndTests`` / ``SyncDispatchToRecordsJobHandlerTests`` —
   exercise ``run_dispatch_sync`` end-to-end against a real test database
   with ``_fetch_header_and_data_rows`` patched (the Airbyte ``dispatch_dataDispatchData``
   table is not in our schema and is owned by Airbyte). They verify upsert,
   revival of soft-deleted rows, soft-delete of disappeared rows, and the
   handler's job.result payload.

Run (from pyro-backend/):

  # everything in this file
  pytest src/tests/rest/background_jobs/test_sync_dispatch_to_records.py -v

  # just the fast unit tests, no DB needed
  pytest src/tests/rest/background_jobs/test_sync_dispatch_to_records.py::DispatchTransformerTests -v

  # one specific test
  pytest src/tests/rest/background_jobs/test_sync_dispatch_to_records.py::DispatchSyncEndToEndTests::test_inserts_new_records_for_new_source_rows -v
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from background_jobs import dispatch_sync as ds
from background_jobs.dispatch_sync import (
    DISPATCH_ENTITY_TYPE,
    _build_column_mapping,
    _find_dc_column_key,
    _to_bool,
    _to_date,
    _to_decimal,
    _to_int,
    _to_str,
    _transform_row,
    run_dispatch_sync,
)
from background_jobs.job_handlers import SyncDispatchToRecordsJobHandler
from background_jobs.models import JobType
from crm_records.models import Record

from tests.factories import BackgroundJobFactory, TenantFactory


# =====================================================================
# Helpers
# =====================================================================

def _source_row(**overrides):
    """
    Build a single source row dict matching what _fetch_header_and_data_rows
    returns as a data row. Sensible defaults are used for the fields we don't override so a
    test can pass a minimal kwargs and still get a row that round-trips.
    """
    defaults = {
        "column_A": "1",
        "column_B": "DC-001",
        "column_C": "07-Apr-26",
        "column_D": "Acme Pumps Pvt Ltd",
        "column_E": "Pump Model X",
        "column_F": "30 Days Credit",
        "column_G": "5",
        "column_H": "5,500",
        "column_I": "PO-12345",
        "column_J": "01-Mar-26",
        "column_K": "Eng A",
        "column_L": "SO-9001",
        "column_M": "Mumbai",
        "column_N": "SN-AAA,SN-BBB",
        "column_R": "rush",
        "column_S": "TRUE",
        "Godown_O1": "08-Apr-26",
        "Godown_P1": "09-Apr-26",
        "Godown_Q1": "10-Apr-26",
        "Godown_U1": "EWB-987",
        "Godown_W1": "BlueDart",
        "Godown_X1": "MH-12-AB-1234",
        "GODOWN_AU1": "10:00",
        "GODOWN_AV1": "12:30",
        "Godown_AD1": "11-Apr-26",
        "Godown___Check": "yes",
        "ArvindG_Y1": "LR-555",
        "ArvindG_Z1": "12-Apr-26",
        "ArvindG_AA1": "Road",
        "ArvindG_AB1": "1,200",
        "ArvindG_AC1": "15-Apr-26",
        "ArvindG_AF1": "16-Apr-26",
        "Umesh_AE1": "yes",
        "Tulsi_AI1": "17-Apr-26",
        "Tulsi_AJ1": "18-Apr-26",
        "Tulsi_AK1": "19-Apr-26",
        "Umesh_AL1": "PumpY",
        "Umesh_AM1": "MS-7001",
        "Umesh_AN1": "CRM-9",
        "Umesh_AO1": "20-Apr-26",
        "Umesh_AP1": "yes",
        "Umesh_AQ1": "TRUE",
        "column_AH": "EW-100",
        "Akshay": "21-Apr-26",
        "Umesh_Akshay": "TRUE",
        "column_AR": "note line",
        "DarshanS_AS1": "22-Apr-26",
        "DarshanS_AT1": "23-Apr-26",
        # Airbyte metadata — must be ignored by the transformer.
        "_airbyte_raw_id": "ignore-me",
        "_airbyte_extracted_at": "2026-04-08T00:00:00Z",
        "_airbyte_meta": {},
        "_airbyte_generation_id": 1,
    }
    defaults.update(overrides)
    return defaults


def _sheet_header_row() -> dict:
    """Production row-1 labels (dispatch_dataDispatchData header row)."""
    from tests.rest.background_jobs.test_production_header_aliases import (
        PRODUCTION_HEADER_ROW,
    )

    return dict(PRODUCTION_HEADER_ROW)


def _transform_row_for_test(row: dict, synced_at: str):
    header = _sheet_header_row()
    col_mapping = _build_column_mapping(header)
    dc_col = _find_dc_column_key(header, col_mapping)
    return _transform_row(row, col_mapping, dc_col, synced_at)


# =====================================================================
# 1. Pure unit tests — transformers
# =====================================================================

class DispatchTransformerTests(unittest.TestCase):
    """Field-level parse rules. No DB. Run with: pytest -k DispatchTransformerTests"""

    # --- _to_str / _clean ---

    def test_str_strips_and_treats_blank_as_none(self):
        self.assertEqual(_to_str("  hello  "), "hello")
        self.assertIsNone(_to_str("   "))
        self.assertIsNone(_to_str(""))
        self.assertIsNone(_to_str(None))

    # --- _to_date ---

    def test_date_parses_dd_mon_yy_format(self):
        self.assertEqual(_to_date("07-Apr-26"), "2026-04-07")
        self.assertEqual(_to_date("30-Mar-26"), "2026-03-30")

    def test_date_parses_dd_mon_yyyy_format(self):
        self.assertEqual(_to_date("07-Apr-2026"), "2026-04-07")

    def test_date_strips_whitespace_before_parsing(self):
        self.assertEqual(_to_date("  07-Apr-26 "), "2026-04-07")

    def test_date_returns_none_for_unparseable(self):
        self.assertIsNone(_to_date("not a date"))
        self.assertIsNone(_to_date("2026/04/07"))  # wrong separator

    def test_date_returns_none_for_empty_or_none(self):
        self.assertIsNone(_to_date(""))
        self.assertIsNone(_to_date("   "))
        self.assertIsNone(_to_date(None))

    # --- _to_int ---

    def test_int_parses_plain_integers(self):
        self.assertEqual(_to_int("5"), 5)
        self.assertEqual(_to_int(" 12 "), 12)

    def test_int_strips_commas(self):
        self.assertEqual(_to_int("1,200"), 1200)

    def test_int_returns_none_for_blank_or_unparseable(self):
        self.assertIsNone(_to_int(""))
        self.assertIsNone(_to_int(None))
        self.assertIsNone(_to_int("abc"))

    # --- _to_decimal ---

    def test_decimal_strips_commas(self):
        self.assertEqual(_to_decimal("5,500"), 5500.0)
        self.assertEqual(_to_decimal("1,234,567.89"), 1234567.89)

    def test_decimal_handles_plain_numbers(self):
        self.assertEqual(_to_decimal("12.34"), 12.34)

    def test_decimal_returns_none_for_blank_or_unparseable(self):
        self.assertIsNone(_to_decimal(""))
        self.assertIsNone(_to_decimal(None))
        self.assertIsNone(_to_decimal("not money"))

    # --- _to_bool ---

    def test_bool_parses_TRUE_and_FALSE_case_insensitively(self):
        self.assertTrue(_to_bool("TRUE"))
        self.assertFalse(_to_bool("FALSE"))
        self.assertTrue(_to_bool("true"))
        self.assertFalse(_to_bool("false"))
        self.assertTrue(_to_bool(" TRUE "))

    def test_bool_returns_none_for_blank_or_unknown(self):
        self.assertIsNone(_to_bool(""))
        self.assertIsNone(_to_bool(None))
        self.assertIsNone(_to_bool("yes"))  # spec says only TRUE/FALSE map

    # --- _transform_row ---

    def test_transform_row_maps_full_set_of_fields(self):
        synced_at = "2026-05-15T08:05:00"
        result = _transform_row_for_test(_source_row(), synced_at)

        self.assertIsNotNone(result)
        self.assertEqual(result["source_row_id"], "DC-001")
        self.assertEqual(result["entity_type"], DISPATCH_ENTITY_TYPE)
        self.assertEqual(result["tenant_id"], ds.DISPATCH_TENANT_ID)
        self.assertFalse(result["is_deleted"])
        self.assertIsNone(result["deleted_at"])
        self.assertEqual(result["pyro_data"], {})
        self.assertEqual(result["synced_at"], synced_at)

        data = result["data"]
        # Spot-check each type bucket.
        self.assertEqual(data["dc_number"], "DC-001")
        self.assertEqual(data["dc_date"], "2026-04-07")           # date
        self.assertEqual(data["quantity"], 5)                     # int
        self.assertEqual(data["amount"], 5500.0)                  # decimal w/ comma
        self.assertEqual(data["freight_amount"], 1200.0)
        self.assertTrue(data["dc_received_in_office"])            # bool
        self.assertTrue(data["sis_ctf_mail"])
        self.assertTrue(data["dc_in_office"])
        self.assertEqual(data["account_name"], "Acme Pumps Pvt Ltd")
        self.assertEqual(data["e_warranty_updated_date"], "2026-04-21")
        # Embedded keys
        self.assertEqual(data["source_row_id"], "DC-001")
        self.assertEqual(data["synced_at"], synced_at)

    def test_transform_row_skips_when_column_b_blank(self):
        self.assertIsNone(_transform_row_for_test(_source_row(column_B=""), "2026-05-15T00:00:00"))
        self.assertIsNone(_transform_row_for_test(_source_row(column_B=None), "2026-05-15T00:00:00"))
        self.assertIsNone(_transform_row_for_test(_source_row(column_B="   "), "2026-05-15T00:00:00"))

    def test_transform_row_does_not_map_airbyte_metadata_columns(self):
        result = _transform_row_for_test(_source_row(), "2026-05-15T00:00:00")
        for meta_key in ("_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta", "_airbyte_generation_id"):
            self.assertNotIn(meta_key, result["data"])

    def test_transform_row_stores_none_for_unparseable_field_without_failing(self):
        """A bad date should not blow up the row — it should land as None."""
        row = _source_row(column_C="garbage", column_G="not-a-number")
        result = _transform_row_for_test(row, "2026-05-15T00:00:00")
        self.assertIsNotNone(result)
        self.assertIsNone(result["data"]["dc_date"])
        self.assertIsNone(result["data"]["quantity"])

    def test_transform_row_treats_empty_string_as_none_for_all_types(self):
        row = _source_row(
            column_C="", column_G="", column_H="", column_S="", Akshay="",
        )
        result = _transform_row_for_test(row, "2026-05-15T00:00:00")
        self.assertIsNone(result["data"]["dc_date"])
        self.assertIsNone(result["data"]["quantity"])
        self.assertIsNone(result["data"]["amount"])
        self.assertIsNone(result["data"]["dc_received_in_office"])
        self.assertIsNone(result["data"]["e_warranty_updated_date"])

    def test_header_labels_map_even_when_physical_column_names_change(self):
        """Ops may rename Airbyte cols; row-1 header text is the stable key."""
        header = {
            "new_col_dc": "DC# No",
            "new_col_name": "Account Name",
            "new_col_inv_email": "Date Email Inv Details",
        }
        col_mapping = _build_column_mapping(header)
        dc_col = _find_dc_column_key(header, col_mapping)
        row = {
            "new_col_dc": "DC-RENAMED-99",
            "new_col_name": "Renamed Customer",
            "new_col_inv_email": "07-Apr-26",
        }
        result = _transform_row(row, col_mapping, dc_col, "2026-05-15T00:00:00")
        self.assertEqual(result["source_row_id"], "DC-RENAMED-99")
        self.assertEqual(result["data"]["account_name"], "Renamed Customer")
        self.assertEqual(result["data"]["date_email_inv_details"], "2026-04-07")


# =====================================================================
# 2. End-to-end sync against a real DB (fetch is mocked)
# =====================================================================

class _DispatchTenantPatchMixin:
    """
    Ensure DISPATCH_TENANT_ID points at a real test Tenant for the duration of
    each test method (avoids the all-zeros placeholder, which has no Tenant row).
    """

    def setUp(self):
        super().setUp()
        self.tenant = TenantFactory()
        self._tenant_patcher = patch.object(ds, "DISPATCH_TENANT_ID", str(self.tenant.id))
        self._tenant_patcher.start()
        self.addCleanup(self._tenant_patcher.stop)

    def _patch_source_rows(self, rows):
        """Patch fetch to return a sheet header row plus data rows."""
        return patch.object(
            ds,
            "_fetch_header_and_data_rows",
            return_value=(_sheet_header_row(), rows),
        )


class DispatchSyncEndToEndTests(_DispatchTenantPatchMixin, TestCase):
    """End-to-end ``run_dispatch_sync`` runs with the source fetch mocked."""

    # -----------------------------------------------------------------
    # Insert
    # -----------------------------------------------------------------

    def test_inserts_new_records_for_new_source_rows(self):
        rows = [_source_row(column_B="DC-A"), _source_row(column_B="DC-B")]
        with self._patch_source_rows(rows):
            stats = run_dispatch_sync()

        self.assertEqual(stats["fetched"], 2)
        self.assertEqual(stats["upserted"], 2)
        self.assertEqual(stats["soft_deleted"], 0)
        self.assertEqual(stats["skipped"], 0)

        records = list(
            Record.objects.filter(
                tenant_id=self.tenant.id, entity_type=DISPATCH_ENTITY_TYPE
            ).order_by("id")
        )
        self.assertEqual(len(records), 2)
        srids = {r.data["source_row_id"] for r in records}
        self.assertEqual(srids, {"DC-A", "DC-B"})
        for r in records:
            self.assertFalse(r.is_deleted)
            self.assertIsNone(r.deleted_at)
            self.assertIn("synced_at", r.data)

    # -----------------------------------------------------------------
    # Update branch of upsert
    # -----------------------------------------------------------------

    def test_update_branch_overwrites_data_for_existing_source_row_id(self):
        Record.objects.create(
            tenant_id=self.tenant.id,
            entity_type=DISPATCH_ENTITY_TYPE,
            data={
                "source_row_id": "DC-A",
                "dc_number": "DC-A",
                "account_name": "OLD NAME",
                "synced_at": "2026-01-01T00:00:00",
            },
            pyro_data={},
        )

        rows = [_source_row(column_B="DC-A", column_D="NEW NAME")]
        with self._patch_source_rows(rows):
            stats = run_dispatch_sync()

        self.assertEqual(stats["upserted"], 1)
        record = Record.objects.get(
            tenant_id=self.tenant.id,
            entity_type=DISPATCH_ENTITY_TYPE,
            data__source_row_id="DC-A",
        )
        self.assertEqual(record.data["account_name"], "NEW NAME")
        # synced_at must have been refreshed.
        self.assertNotEqual(record.data["synced_at"], "2026-01-01T00:00:00")

    # -----------------------------------------------------------------
    # Revival
    # -----------------------------------------------------------------

    def test_upsert_revives_previously_soft_deleted_record(self):
        old = Record.objects.create(
            tenant_id=self.tenant.id,
            entity_type=DISPATCH_ENTITY_TYPE,
            data={
                "source_row_id": "DC-Z",
                "dc_number": "DC-Z",
                "synced_at": "2026-01-01T00:00:00",
            },
            pyro_data={},
            is_deleted=True,
            deleted_at=timezone.now(),
        )
        # Sanity: default manager hides it.
        self.assertFalse(
            Record.objects.filter(pk=old.pk).exists()
        )

        rows = [_source_row(column_B="DC-Z")]
        with self._patch_source_rows(rows):
            stats = run_dispatch_sync()

        self.assertEqual(stats["upserted"], 1)
        revived = Record.objects.get(pk=old.pk)
        self.assertFalse(revived.is_deleted)
        self.assertIsNone(revived.deleted_at)

    # -----------------------------------------------------------------
    # Soft-delete missing
    # -----------------------------------------------------------------

    def test_soft_deletes_records_no_longer_in_source(self):
        # Two existing records: one will appear in the next sync, one won't.
        keep = Record.objects.create(
            tenant_id=self.tenant.id,
            entity_type=DISPATCH_ENTITY_TYPE,
            data={"source_row_id": "DC-KEEP", "dc_number": "DC-KEEP"},
            pyro_data={},
        )
        drop = Record.objects.create(
            tenant_id=self.tenant.id,
            entity_type=DISPATCH_ENTITY_TYPE,
            data={"source_row_id": "DC-DROP", "dc_number": "DC-DROP"},
            pyro_data={},
        )

        rows = [_source_row(column_B="DC-KEEP")]
        with self._patch_source_rows(rows):
            stats = run_dispatch_sync()

        self.assertEqual(stats["upserted"], 1)
        self.assertEqual(stats["soft_deleted"], 1)

        keep.refresh_from_db()
        self.assertFalse(keep.is_deleted)

        # Soft-deleted, so default manager cannot find it; use all_objects.
        gone = Record.all_objects.get(pk=drop.pk)
        self.assertTrue(gone.is_deleted)
        self.assertIsNotNone(gone.deleted_at)
        # synced_at must be stamped on the soft-deleted row too.
        self.assertIn("synced_at", gone.data)

    def test_does_not_soft_delete_other_tenants_records(self):
        other_tenant = TenantFactory()
        other_record = Record.objects.create(
            tenant_id=other_tenant.id,
            entity_type=DISPATCH_ENTITY_TYPE,
            data={"source_row_id": "DC-OTHER", "dc_number": "DC-OTHER"},
            pyro_data={},
        )

        with self._patch_source_rows([]):
            run_dispatch_sync()

        other_record.refresh_from_db()
        self.assertFalse(other_record.is_deleted)

    def test_does_not_soft_delete_other_entity_types_in_same_tenant(self):
        other_entity = Record.objects.create(
            tenant_id=self.tenant.id,
            entity_type="lead",
            data={"source_row_id": "OTHER-ENTITY"},
            pyro_data={},
        )
        with self._patch_source_rows([]):
            run_dispatch_sync()
        other_entity.refresh_from_db()
        self.assertFalse(other_entity.is_deleted)

    # -----------------------------------------------------------------
    # Skipped rows
    # -----------------------------------------------------------------

    def test_rows_with_blank_column_b_are_skipped_and_counted(self):
        rows = [
            _source_row(column_B="DC-OK"),
            _source_row(column_B=""),
            _source_row(column_B=None),
        ]
        with self._patch_source_rows(rows):
            stats = run_dispatch_sync()
        self.assertEqual(stats["fetched"], 3)
        self.assertEqual(stats["upserted"], 1)
        self.assertEqual(stats["skipped"], 2)

    # -----------------------------------------------------------------
    # Exception path
    # -----------------------------------------------------------------

    def test_exception_in_fetch_is_re_raised_for_scheduler(self):
        with patch.object(ds, "_fetch_header_and_data_rows", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                run_dispatch_sync()


class SyncDispatchToRecordsJobHandlerTests(_DispatchTenantPatchMixin, TestCase):
    """Wrapper around the JobHandler — confirms job.result is populated."""

    def test_handler_populates_job_result_with_counts(self):
        handler = SyncDispatchToRecordsJobHandler()
        job = BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.SYNC_DISPATCH_TO_RECORDS,
            payload={},
        )
        rows = [_source_row(column_B="DC-J1"), _source_row(column_B="DC-J2")]
        with self._patch_source_rows(rows):
            ok = handler.process(job)
        self.assertTrue(ok)
        self.assertTrue(job.result["success"])
        self.assertEqual(job.result["fetched"], 2)
        self.assertEqual(job.result["upserted"], 2)
        self.assertEqual(job.result["soft_deleted"], 0)
        self.assertIn("timestamp", job.result)

    def test_handler_get_retry_delay_matches_lead_cron_handlers(self):
        handler = SyncDispatchToRecordsJobHandler()
        self.assertEqual(handler.get_retry_delay(1), 60)
        self.assertEqual(handler.get_retry_delay(2), 300)
        self.assertEqual(handler.get_retry_delay(3), 900)
        self.assertEqual(handler.get_retry_delay(99), 900)

    def test_handler_validate_payload_accepts_anything(self):
        handler = SyncDispatchToRecordsJobHandler()
        self.assertTrue(handler.validate_payload({}))
        self.assertTrue(handler.validate_payload({"unused": "value"}))
