"""Unit tests for live shipment status mapping (no live network)."""

from django.test import SimpleTestCase

from crm_records.inventory_shipment_live_track import (
    ShipmentTrackError,
    _assert_safe_track_url,
    _parse_vendor_tracking_html,
    map_status_text,
    track_shipment,
)


class ShipmentLiveTrackMappingTests(SimpleTestCase):
    def test_map_status_keywords(self):
        self.assertEqual(map_status_text("Out for Delivery"), "OUT_FOR_DELIVERY")
        self.assertEqual(map_status_text("In Transit - Hub"), "IN_TRANSIT")
        self.assertEqual(map_status_text("Successfully Delivered"), "DELIVERED")
        self.assertEqual(map_status_text("RTO Undelivered"), "EXCEPTION")
        self.assertEqual(map_status_text("Soft data uploaded"), "ORDERED")

    def test_allowlist_rejects_unknown_non_track_host(self):
        with self.assertRaises(ShipmentTrackError):
            _assert_safe_track_url("https://evil.example/about")

    def test_allowlist_accepts_delhivery(self):
        safe, courier = _assert_safe_track_url(
            "https://www.delhivery.com/track/package/?waybill=123456789012"
        )
        self.assertEqual(courier, "delhivery")
        self.assertTrue(safe.startswith("https://www.delhivery.com/"))

    def test_vendor_track_url_allowed(self):
        safe, courier = _assert_safe_track_url(
            "https://genxbattery.com/track?order=GX-26-07974&token=abc"
        )
        self.assertEqual(courier, "vendor")
        self.assertIn("genxbattery.com", safe)

    def test_parse_genx_style_vendor_html(self):
        html = """
        Mission Tracking · Order GX-26-07974
        Current Status Delivered
        Delivered On 15 Jul 2026
        Live scans from BlueDart · Surface · AWB 50939779914
        SHIPMENT DELIVERED YESWANTHPUR
        SHIPMENT OUT FOR DELIVERY
        Registered Picked Up In Transit Out for Delivery Delivered
        """
        parsed = _parse_vendor_tracking_html(html)
        self.assertEqual(parsed["shipment_status"], "DELIVERED")
        self.assertEqual(parsed["tracking_number"], "50939779914")
        self.assertIn("Blue", parsed["courier_name"] or "")

    def test_map_bluedart_shipment_delivered(self):
        self.assertEqual(map_status_text("Shipment Delivered"), "DELIVERED")
        self.assertEqual(map_status_text("Shipment Out for Delivery"), "OUT_FOR_DELIVERY")

    def test_eleven_digit_awb_prefers_bluedart_first(self):
        from crm_records.inventory_shipment_live_track import _carrier_trackers_for_awb

        names = [n for n, _ in _carrier_trackers_for_awb("90591653202", None)]
        self.assertEqual(names[0], "bluedart")
        self.assertIn("fedex", names)
        self.assertIn("dhl", names)

    def test_twelve_digit_awb_prefers_fedex_first(self):
        from crm_records.inventory_shipment_live_track import _carrier_trackers_for_awb

        names = [n for n, _ in _carrier_trackers_for_awb("471904076719", None)]
        self.assertEqual(names[0], "fedex")

    def test_ten_digit_awb_prefers_dhl_first(self):
        from crm_records.inventory_shipment_live_track import _carrier_trackers_for_awb

        names = [n for n, _ in _carrier_trackers_for_awb("1215523352", None)]
        self.assertEqual(names[0], "dhl")

    def test_track_shipment_requires_input(self):
        with self.assertRaises(ShipmentTrackError):
            track_shipment()
