"""Tests for inventory shipment tracking normalization."""

from django.test import SimpleTestCase

from crm_records.inventory_shipment_tracking import (
    DEFAULT_SHIPMENT_STATUS,
    apply_shipment_tracking_normalization,
    extract_tracking_number_from_url,
    normalize_tracking_paste,
)


class InventoryShipmentTrackingTests(SimpleTestCase):
    def test_normalize_paste_url_extracts_awb(self):
        out = normalize_tracking_paste(
            "https://www.delhivery.com/track/package/?waybill=123456789012"
        )
        self.assertEqual(out["tracking_link"], "https://www.delhivery.com/track/package/?waybill=123456789012")
        self.assertEqual(out["tracking_number"], "123456789012")

    def test_normalize_paste_plain_number(self):
        out = normalize_tracking_paste("ABCD123456789")
        self.assertIsNone(out["tracking_link"])
        self.assertEqual(out["tracking_number"], "ABCD123456789")

    def test_extract_aftership_path(self):
        awb = extract_tracking_number_from_url("https://www.aftership.com/track/delhivery/XYZ999888")
        self.assertEqual(awb, "XYZ999888")

    def test_apply_sets_updated_at_on_change(self):
        data = {
            "tracking_link": "https://example.com/track?awb=111222333444",
            "shipment_status": "in transit",
        }
        out = apply_shipment_tracking_normalization(data, previous={})
        self.assertEqual(out["tracking_number"], "111222333444")
        self.assertEqual(out["shipment_status"], "IN_TRANSIT")
        self.assertTrue(out.get("tracking_updated_at"))

    def test_apply_no_update_when_unchanged(self):
        prev = {
            "tracking_number": "111",
            "tracking_link": None,
            "courier_name": None,
            "shipment_status": DEFAULT_SHIPMENT_STATUS,
            "eta": None,
            "tracking_updated_at": "2020-01-01T00:00:00+00:00",
        }
        data = dict(prev)
        out = apply_shipment_tracking_normalization(data, previous=prev)
        self.assertEqual(out.get("tracking_updated_at"), "2020-01-01T00:00:00+00:00")
