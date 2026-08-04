"""Tests for RefreshInventoryShipmentTrackingJobHandler."""

from __future__ import annotations

from unittest.mock import patch

from django.test import TestCase

from background_jobs.job_handlers import RefreshInventoryShipmentTrackingJobHandler
from background_jobs.models import JobType
from tests.factories.crm_records_factory import BackgroundJobFactory, RecordFactory
from tests.factories.core_factory import TenantFactory


class RefreshInventoryShipmentTrackingJobHandlerTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()
        self.handler = RefreshInventoryShipmentTrackingJobHandler()

    def _make_in_shipping_record(self, **data_overrides):
        data = {
            "status": "IN_SHIPPING",
            "tracking_number": "AWB123456789",
            "tracking_link": "",
            "courier_name": "",
            "shipment_status": "ORDERED",
            "eta": None,
        }
        data.update(data_overrides)
        return RecordFactory(
            tenant=self.tenant,
            entity_type="inventory_request",
            data=data,
        )

    def test_updates_shipment_status_when_track_returns_new_status(self):
        record = self._make_in_shipping_record()
        job = BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.REFRESH_INVENTORY_SHIPMENT_TRACKING,
            payload={"max_per_run": 50},
        )
        with patch(
            "crm_records.inventory_shipment_live_track.track_shipment",
            return_value={
                "ok": True,
                "shipment_status": "IN_TRANSIT",
                "tracking_number": "AWB123456789",
                "tracking_link": "https://www.aftership.com/track/AWB123456789",
                "courier_name": "Delhivery",
                "eta": "2026-07-28",
                "method": "aftership_api",
            },
        ) as mocked:
            ok = self.handler.process(job)

        self.assertTrue(ok)
        mocked.assert_called_once()
        record.refresh_from_db()
        self.assertEqual(record.data.get("shipment_status"), "IN_TRANSIT")
        self.assertEqual(record.data.get("courier_name"), "Delhivery")
        self.assertEqual(record.data.get("eta"), "2026-07-28")
        self.assertTrue(record.data.get("tracking_updated_at"))
        self.assertEqual(job.result["updated"], 1)
        self.assertEqual(job.result["checked"], 1)

    def test_skips_when_track_not_ok(self):
        record = self._make_in_shipping_record()
        job = BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.REFRESH_INVENTORY_SHIPMENT_TRACKING,
            payload={},
        )
        with patch(
            "crm_records.inventory_shipment_live_track.track_shipment",
            return_value={
                "ok": False,
                "shipment_status": None,
                "error": "Pending scans",
            },
        ):
            ok = self.handler.process(job)

        self.assertTrue(ok)
        record.refresh_from_db()
        self.assertEqual(record.data.get("shipment_status"), "ORDERED")
        self.assertEqual(job.result["updated"], 0)
        self.assertEqual(job.result["unchanged"], 1)

    def test_skips_delivered_shipments(self):
        self._make_in_shipping_record(shipment_status="DELIVERED")
        job = BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.REFRESH_INVENTORY_SHIPMENT_TRACKING,
            payload={},
        )
        with patch(
            "crm_records.inventory_shipment_live_track.track_shipment",
        ) as mocked:
            ok = self.handler.process(job)

        self.assertTrue(ok)
        mocked.assert_not_called()
        self.assertEqual(job.result["checked"], 0)

    def test_get_retry_delay(self):
        self.assertEqual(self.handler.get_retry_delay(1), 60)
        self.assertEqual(self.handler.get_retry_delay(2), 300)
        self.assertEqual(self.handler.get_retry_delay(3), 900)
