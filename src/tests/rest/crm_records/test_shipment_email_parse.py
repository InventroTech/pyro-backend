"""Unit tests for shipment email parsing (no Zoho network calls)."""

from django.test import SimpleTestCase

from crm_records.shipment_email_parse import parse_shipment_email


class ShipmentEmailParseTests(SimpleTestCase):
    def test_extracts_awb_courier_and_link(self):
        subject = "Your Blue Dart shipment is in transit"
        body = """
        Hello,<br>
        AWB: 123456789012<br>
        Track: https://www.bluedart.com/tracking?awb=123456789012<br>
        """
        out = parse_shipment_email(subject=subject, html_or_text=body)
        self.assertTrue(out["is_shipment"])
        self.assertEqual(out["tracking_number"], "123456789012")
        self.assertIn("bluedart", (out["tracking_link"] or "").lower())
        self.assertRegex(out["courier_name"] or "", r"(?i)blue\s*dart")

    def test_extracts_record_uuid(self):
        rid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        out = parse_shipment_email(
            subject="Shipment update",
            html_or_text=f"Request {rid} shipped. Tracking number: ABCD12345678",
        )
        self.assertIn(rid, out["match_keys"]["record_ids"])
        self.assertEqual(out["tracking_number"], "ABCD12345678")

    def test_ignores_non_shipment_mail(self):
        out = parse_shipment_email(
            subject="Weekly standup notes",
            html_or_text="Please review the agenda for Monday.",
        )
        self.assertFalse(out["is_shipment"])
        self.assertIsNone(out["tracking_number"])
