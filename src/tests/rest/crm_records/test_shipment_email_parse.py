"""Unit tests for shipment email parsing (no Zoho network calls)."""

from django.test import SimpleTestCase

from crm_records.shipment_email_parse import (
    is_delivery_partner_sender,
    parse_shipment_email,
)


class ShipmentEmailParseTests(SimpleTestCase):
    def test_extracts_awb_courier_and_link_from_partner(self):
        subject = "Your Blue Dart shipment is in transit"
        body = """
        Hello,<br>
        AWB: 123456789012<br>
        Track: https://www.bluedart.com/tracking?awb=123456789012<br>
        """
        out = parse_shipment_email(
            subject=subject,
            html_or_text=body,
            from_address="Blue Dart <noreply@bluedart.com>",
        )
        self.assertTrue(out["is_shipment"])
        self.assertEqual(out["tracking_number"], "123456789012")
        self.assertIn("bluedart", (out["tracking_link"] or "").lower())
        self.assertRegex(out["courier_name"] or "", r"(?i)blue\s*dart")

    def test_ignores_same_body_from_non_partner(self):
        out = parse_shipment_email(
            subject="Your shipment is in transit",
            html_or_text="AWB: 123456789012 Track: https://www.bluedart.com/tracking?awb=123456789012",
            from_address="hr@company.com",
        )
        self.assertFalse(out["is_shipment"])
        # Tracking may still be parsed, but sync will skip non-partners.
        self.assertEqual(out["tracking_number"], "123456789012")

    def test_extracts_record_uuid(self):
        rid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        out = parse_shipment_email(
            subject="Shipment update",
            html_or_text=f"Request {rid} shipped. Tracking number: ABCD12345678",
            from_address="alerts@delhivery.com",
        )
        self.assertTrue(out["is_shipment"])
        self.assertIn(rid, out["match_keys"]["record_ids"])
        self.assertEqual(out["tracking_number"], "ABCD12345678")

    def test_ignores_non_partner_mail(self):
        out = parse_shipment_email(
            subject="Weekly standup notes",
            html_or_text="Please review the agenda for Monday.",
            from_address="boss@company.com",
        )
        self.assertFalse(out["is_shipment"])
        self.assertIsNone(out["tracking_number"])

    def test_vendor_subject_with_courier_awb_is_shipment(self):
        out = parse_shipment_email(
            subject="VendorCo - Order ID 1135941 is Shipped by DTDC - AWB : AB10083664",
            html_or_text="<p>Your order has left the warehouse.</p>",
            from_address="VendorCo <orders@vendorco.example>",
        )
        self.assertTrue(out["is_shipment"])
        self.assertEqual(out["tracking_number"], "AB10083664")
        self.assertEqual((out["courier_name"] or "").upper(), "DTDC")
        self.assertEqual(out["shipment_status"], "IN_TRANSIT")

    def test_out_for_delivery_subject_sets_status_without_awb(self):
        out = parse_shipment_email(
            subject='Out for delivery: 2 "Widget Cleaner Kit"',
            html_or_text="<p>Your package is arriving today. Once delivered you can return it.</p>",
            from_address="Amazon.in <shipment-tracking@amazon.in>",
        )
        self.assertTrue(out["is_shipment"])
        self.assertEqual(out["shipment_status"], "OUT_FOR_DELIVERY")
        self.assertIsNone(out["tracking_number"])
        self.assertIsNone(out["tracking_link"])

    def test_amazon_subject_sets_status_for_each_notice_type(self):
        cases = (
            ('Ordered: 1 "Widget Cleaner Kit"', "ORDERED"),
            ('Shipped: 1 "Widget Cleaner Kit"', "IN_TRANSIT"),
            ('Your package is on the way: "Widget Cleaner Kit"', "IN_TRANSIT"),
            ('Arriving today: 1 "Widget Cleaner Kit"', "OUT_FOR_DELIVERY"),
            ('Delivered: 1 "Widget Cleaner Kit"', "DELIVERED"),
            ('Delivery attempted: 1 "Widget Cleaner Kit"', "EXCEPTION"),
            ('Partially delivered: 1 "Widget Cleaner Kit"', "IN_TRANSIT"),
        )
        for subject, expected in cases:
            with self.subTest(subject=subject):
                out = parse_shipment_email(
                    subject=subject,
                    html_or_text="<p>Track your package. Once delivered you can return it.</p>",
                    from_address="Amazon.in <shipment-tracking@amazon.in>",
                )
                self.assertTrue(out["is_shipment"])
                self.assertEqual(out["shipment_status"], expected)
                self.assertIsNone(out["tracking_number"])

    def test_partner_domains(self):
        self.assertTrue(is_delivery_partner_sender("noreply@notify.delhivery.com")[0])
        self.assertTrue(is_delivery_partner_sender("Shiprocket <updates@shiprocket.in>")[0])
        self.assertTrue(is_delivery_partner_sender('"DTDC" <x@y.com>')[0])
        self.assertFalse(is_delivery_partner_sender("ritam@thepyro.ai")[0])
