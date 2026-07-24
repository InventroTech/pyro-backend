"""Unit tests for price_compare HTML/JSON-LD parsers (no live network)."""

from django.test import SimpleTestCase

from crm_records.price_compare import (
    extract_price_from_html,
    detect_source,
    _parse_price_number,
)


ROBU_PRODUCT_HTML = """
<html><head><title>Buy Arduino Uno R3</title>
<script type="application/ld+json">
{"@context":"https://schema.org","@type":"Product","name":"Arduino Uno R3 with Cable",
 "offers":{"@type":"Offer","url":"https://robu.in/product/arduino-uno-r3/",
 "priceCurrency":"INR","price":"422.73","availability":"https://schema.org/InStock"}}
</script>
</head><body></body></html>
"""

AMAZON_META_HTML = """
<html><head>
<meta property="product:price:amount" content="339.00" />
<meta property="product:price:currency" content="INR" />
<title>Robocraze UNO Board</title>
</head></html>
"""


class PriceCompareParserTests(SimpleTestCase):
    def test_parse_price_number(self):
        self.assertEqual(_parse_price_number("1,299.50"), 1299.50)
        self.assertEqual(_parse_price_number("₹355"), 355.0)
        self.assertIsNone(_parse_price_number(""))

    def test_detect_source(self):
        self.assertEqual(detect_source("https://www.amazon.in/dp/B07G4C4D8F"), "amazon")
        self.assertEqual(detect_source("https://robu.in/product/arduino-uno-r3/"), "robu")
        self.assertEqual(detect_source("https://www.flipkart.com/x/p/itm"), "flipkart")

    def test_extract_robu_json_ld(self):
        extracted = extract_price_from_html(ROBU_PRODUCT_HTML, "https://robu.in/product/arduino-uno-r3/")
        self.assertIsNotNone(extracted)
        self.assertEqual(extracted["price"], 422.73)
        self.assertEqual(extracted["currency"], "INR")
        self.assertIn("Arduino", extracted["title"])

    def test_extract_meta_price(self):
        extracted = extract_price_from_html(AMAZON_META_HTML, "https://www.amazon.in/dp/B07")
        self.assertIsNotNone(extracted)
        self.assertEqual(extracted["price"], 339.0)
        self.assertEqual(extracted["currency"], "INR")

    def test_extract_amazon_delivery(self):
        from crm_records.price_compare import _extract_amazon_delivery

        chunk = (
            '<div data-cy="delivery-block">'
            'FREE delivery <span class="a-text-bold">Fri, 24 Jul</span> on first order'
            '<div>Or fastest delivery <span class="a-text-bold">Tomorrow, 23 Jul</span></div>'
            "</div></div></div>"
        )
        self.assertEqual(
            _extract_amazon_delivery(chunk),
            "Fri, 24 Jul (fastest: Tomorrow, 23 Jul)",
        )

    def test_clean_delivery_text_prefers_date(self):
        from crm_records.price_compare import _clean_delivery_text

        self.assertEqual(_clean_delivery_text("FREE delivery Fri, 24 Jul on first order"), "Fri, 24 Jul")
        self.assertEqual(
            _clean_delivery_text("Usually dispatched in 1-2 business days"),
            "Usually dispatched in 1-2 business days",
        )

    def test_robu_schema_delivery_range(self):
        from crm_records.price_compare import _delivery_from_schema_delivery_time

        text = _delivery_from_schema_delivery_time(
            {
                "handlingTime": {"minValue": 0, "maxValue": 1, "unitCode": "DAY"},
                "transitTime": {"minValue": 2, "maxValue": 7, "unitCode": "DAY"},
            }
        )
        self.assertIsNotNone(text)
        self.assertIn("–", text)

    def test_normalize_indian_pincode(self):
        from crm_records.price_compare import normalize_indian_pincode

        self.assertEqual(normalize_indian_pincode("560001"), "560001")
        self.assertEqual(normalize_indian_pincode("PIN 560 001"), "560001")
        self.assertIsNone(normalize_indian_pincode("12345"))
        self.assertIsNone(normalize_indian_pincode("012345"))
        self.assertIsNone(normalize_indian_pincode(""))

    def test_robu_default_delivery_estimate(self):
        from crm_records.price_compare import _robu_default_delivery_estimate

        text = _robu_default_delivery_estimate()
        self.assertIn("–", text)
        self.assertRegex(text, r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)")
