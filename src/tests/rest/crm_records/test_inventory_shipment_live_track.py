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

    def test_eleven_digit_awb_does_not_guess_bluedart(self):
        from crm_records.inventory_shipment_live_track import _carrier_trackers_for_awb

        self.assertEqual(_carrier_trackers_for_awb("90591653202", None), [])
        names = [n for n, _ in _carrier_trackers_for_awb("90591653202", "bluedart")]
        self.assertEqual(names, ["bluedart"])
        self.assertEqual(
            [n for n, _ in _carrier_trackers_for_awb("123456789012", "delhivery")],
            ["delhivery"],
        )
        self.assertEqual(_carrier_trackers_for_awb("471904076719", "fedex"), [])

    def test_aftership_slugs_follow_user_courier_only(self):
        from crm_records.inventory_shipment_live_track import _aftership_slugs_for_courier

        self.assertEqual(_aftership_slugs_for_courier("FedEx"), ["fedex"])
        self.assertEqual(_aftership_slugs_for_courier("fedex"), ["fedex"])
        self.assertEqual(_aftership_slugs_for_courier("FEDEX"), ["fedex"])
        self.assertEqual(_aftership_slugs_for_courier("Fed Ex"), ["fedex"])
        self.assertNotIn("dhl", _aftership_slugs_for_courier("FedEx"))
        self.assertNotIn("amazon", _aftership_slugs_for_courier("DHL"))
        dhl = _aftership_slugs_for_courier("DHL")
        self.assertEqual(dhl[0], "dhl")
        self.assertEqual(_aftership_slugs_for_courier("dhl")[0], "dhl")
        self.assertIn("dhl-express", dhl)
        self.assertEqual(_aftership_slugs_for_courier("Amazon"), ["amazon", "amazon-order", "amazon-mcf"])
        self.assertEqual(_aftership_slugs_for_courier(None), [])
        self.assertEqual(_aftership_slugs_for_courier("vendor"), [])

    def test_twelve_digit_amazonish_not_assumed_fedex_by_shape_alone(self):
        """12-digit ids like 371022899423 are not a courier by themselves."""
        from crm_records.inventory_shipment_live_track import looks_like_amazon_tracking_id

        self.assertFalse(looks_like_amazon_tracking_id("371022899423"))
        self.assertEqual(len("371022899423"), 12)

    def test_bluedart_error_chrome_detected(self):
        from crm_records.inventory_shipment_live_track import (
            _bluedart_page_looks_like_error_chrome,
        )

        junk = "Status available only for UNDELIVERED Waybills CLOSE × CLOSE × CL"
        self.assertTrue(_bluedart_page_looks_like_error_chrome(junk))
        self.assertFalse(
            _bluedart_page_looks_like_error_chrome("Status Shipment Delivered Bangalore")
        )

    def test_map_aftership_tags(self):
        from crm_records.inventory_shipment_live_track import _map_aftership_tag

        self.assertEqual(_map_aftership_tag("Pending"), "ORDERED")
        self.assertEqual(_map_aftership_tag("InfoReceived"), "ORDERED")
        self.assertEqual(_map_aftership_tag("InTransit"), "IN_TRANSIT")
        self.assertEqual(_map_aftership_tag("OutForDelivery"), "OUT_FOR_DELIVERY")
        self.assertEqual(_map_aftership_tag("Delivered"), "DELIVERED")
        self.assertEqual(_map_aftership_tag("Exception"), "EXCEPTION")

    def test_aftership_prefers_checkpoint_over_pending_tag(self):
        from crm_records.inventory_shipment_live_track import _aftership_status_from_tracking

        status, detail = _aftership_status_from_tracking(
            {
                "tag": "Pending",
                "subtag_message": "No recent updates",
                "checkpoints": [
                    {"tag": "InTransit", "message": "Departed facility"},
                    {"tag": "Delivered", "message": "Delivered"},
                ],
            }
        )
        self.assertEqual(status, "DELIVERED")
        self.assertIn("Delivered", detail or "")

    def test_aftership_weak_pending(self):
        from crm_records.inventory_shipment_live_track import _aftership_is_weak_pending

        self.assertTrue(
            _aftership_is_weak_pending({"tag": "Pending", "subtag": "Pending_005", "checkpoints": []})
        )
        self.assertFalse(
            _aftership_is_weak_pending(
                {"tag": "Delivered", "checkpoints": [{"tag": "Delivered", "message": "Done"}]}
            )
        )





    def test_aftership_shipment_details(self):
        from crm_records.inventory_shipment_live_track import _aftership_shipment_details

        details = _aftership_shipment_details(
            {
                "origin_city": "Mumbai",
                "origin_country_region": "IND",
                "destination_city": "Bengaluru",
                "destination_postal_code": "560001",
                "destination_country_region": "IND",
                "checkpoints": [
                    {"tag": "InfoReceived", "message": "Label created", "location": "Mumbai, IN", "checkpoint_time": "2026-07-01T10:00:00Z"},
                    {"tag": "InTransit", "message": "Departed facility", "city": "Pune", "country_region": "IND", "checkpoint_time": "2026-07-02T08:00:00Z"},
                    {"tag": "OutForDelivery", "message": "Out for delivery", "location": "Bengaluru, IN", "checkpoint_time": "2026-07-03T09:00:00Z"},
                ],
            }
        )
        self.assertEqual(details["origin"], "Mumbai, IND")
        self.assertIn("Bengaluru", details["destination"] or "")
        self.assertEqual(details["current_location"], "Bengaluru, IN")
        self.assertEqual(len(details["events"]), 3)
        self.assertEqual(details["events"][0]["message"], "Out for delivery")
        self.assertEqual(details["events"][0]["status"], "OUT_FOR_DELIVERY")


    def test_public_tracking_link(self):
        from crm_records.inventory_shipment_live_track import (
            _public_tracking_link,
            _resolve_tracking_link,
        )

        self.assertIn("fedextrack", _public_tracking_link("471904076719", "FedEx") or "")
        self.assertIn("AWB=1215523352", _public_tracking_link("1215523352", "DHL") or "")
        self.assertIn("waybill=123", _public_tracking_link("123", "Delhivery") or "")
        self.assertEqual(
            _public_tracking_link("471904076719", None),
            "https://www.aftership.com/track/471904076719",
        )
        # User paste wins
        self.assertEqual(
            _resolve_tracking_link(
                awb="471904076719",
                courier="fedex",
                user_link="https://vendor.example/track/1",
                carrier_link="https://www.fedex.com/fedextrack/?trknbr=471904076719",
            ),
            "https://vendor.example/track/1",
        )
        # Carrier official link preferred over synthesized AfterShip page
        self.assertEqual(
            _resolve_tracking_link(
                awb="471904076719",
                courier="fedex",
                carrier_link="https://www.fedex.com/fedextrack/?trknbr=471904076719",
            ),
            "https://www.fedex.com/fedextrack/?trknbr=471904076719",
        )
        # Number only → always get a link
        self.assertTrue(
            (_resolve_tracking_link(awb="471904076719", courier="FedEx") or "").startswith("http")
        )


    def test_amazon_id_not_fedex(self):
        """Amazon order / TBA ids are recognized as Amazon only when the user says so."""
        from crm_records.inventory_shipment_live_track import (
            detect_courier,
            looks_like_amazon_tracking_id,
            _public_tracking_link,
        )

        self.assertTrue(looks_like_amazon_tracking_id("402-1234567-1234567"))
        self.assertTrue(looks_like_amazon_tracking_id("TBA123456789012"))
        self.assertFalse(looks_like_amazon_tracking_id("471904076719"))
        self.assertIsNone(detect_courier(tracking_link=None, courier_name=None))
        self.assertEqual(detect_courier(courier_name="Amazon"), "amazon")
        for typed in ("FedEx", "fedex", "FEDEX", "fed-ex", "Fed Ex"):
            self.assertEqual(detect_courier(courier_name=typed), "fedex", typed)
        for typed in ("DHL", "dhl", "Dhl"):
            self.assertEqual(detect_courier(courier_name=typed), "dhl", typed)
        self.assertEqual(detect_courier(courier_name="BLUE DART"), "bluedart")
        self.assertEqual(
            detect_courier(tracking_link="https://www.aftership.com/track/fedex/471904076719"),
            "fedex",
        )
        self.assertIn("amazon", (_public_tracking_link("402-1234567-1234567", "amazon") or ""))

    def test_track_shipment_requires_input(self):
        with self.assertRaises(ShipmentTrackError):
            track_shipment()

    def test_track_shipment_requires_courier_with_number(self):
        with self.assertRaises(ShipmentTrackError) as ctx:
            track_shipment(tracking_number="471904076719")
        self.assertIn("courier_name", str(ctx.exception))

    def test_track_shipment_accepts_number_with_courier(self):
        """Number + courier is enough to start tracking (AfterShip may be unset)."""
        from unittest.mock import patch

        with patch.dict("os.environ", {"AFTERSHIP_API_KEY": ""}, clear=False):
            result = track_shipment(tracking_number="471904076719", courier_name="FedEx")
        self.assertEqual(result.get("tracking_number"), "471904076719")
        self.assertEqual((result.get("courier") or "").lower(), "fedex")
        self.assertIn("fedex", str(result.get("courier_name") or "").lower())

    def test_aftership_queries_only_user_courier_slug(self):
        from unittest.mock import MagicMock, patch
        from crm_records.inventory_shipment_live_track import _track_aftership_api

        get_resp = MagicMock()
        get_resp.status_code = 200
        get_resp.json.return_value = {
            "data": {
                "trackings": [
                    {
                        "id": "trk_1",
                        "slug": "fedex",
                        "tag": "InTransit",
                        "checkpoints": [{"tag": "InTransit", "message": "Departed"}],
                    }
                ]
            }
        }
        with patch.dict("os.environ", {"AFTERSHIP_API_KEY": "test-key"}, clear=False):
            with patch(
                "crm_records.inventory_shipment_live_track.requests.get",
                return_value=get_resp,
            ) as get_mock:
                with patch(
                    "crm_records.inventory_shipment_live_track.requests.post",
                ) as post_mock:
                    result = _track_aftership_api("471904076719", courier="fedex")

        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("courier"), "fedex")
        params = get_mock.call_args.kwargs.get("params") or {}
        self.assertEqual(params.get("slug"), "fedex")
        self.assertEqual(params.get("tracking_numbers"), "471904076719")
        for call in post_mock.call_args_list:
            url = call.args[0] if call.args else ""
            self.assertNotIn("couriers/detect", url)


class AftershipCourierCatalogTests(SimpleTestCase):
    def test_catalog_includes_common_slugs(self):
        from crm_records.aftership_couriers import load_aftership_couriers

        couriers = load_aftership_couriers()
        slugs = {row["slug"] for row in couriers}
        self.assertGreater(len(couriers), 900)
        for slug in (
            "amazon",
            "fedex",
            "dhl",
            "bluedart",
            "delhivery",
            "dtdc",
            "shiprocket",
            "india-post",
            "ecom-express",
        ):
            self.assertIn(slug, slugs)
