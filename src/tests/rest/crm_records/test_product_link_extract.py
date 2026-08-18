"""Unit tests for product URL extract (no live ScrapingBee calls)."""

from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from crm_records.product_link_extract import (
    ProductLinkExtractError,
    assert_safe_product_url,
    extract_product_from_url,
    normalize_image_url,
    parse_price_number,
    vendor_from_host,
)


class ProductLinkExtractTests(SimpleTestCase):
    def test_parse_price_number(self):
        self.assertEqual(parse_price_number("₹2,989.50"), 2989.50)
        self.assertEqual(parse_price_number(500), 500.0)
        self.assertIsNone(parse_price_number(""))
        self.assertIsNone(parse_price_number(0))

    def test_vendor_from_host(self):
        self.assertEqual(vendor_from_host("www.amazon.in"), "AMAZON")
        self.assertEqual(vendor_from_host("robu.in"), "ROBU")
        self.assertEqual(vendor_from_host("shop.example.com"), "SHOP")

    def test_assert_safe_https_and_strip_tracking(self):
        safe = assert_safe_product_url(
            "https://www.amazon.in/dp/B0H1K35VB3?srsltid=abc&utm_source=x"
        )
        self.assertTrue(safe.startswith("https://www.amazon.in/dp/B0H1K35VB3"))
        self.assertNotIn("srsltid", safe)
        self.assertNotIn("utm_source", safe)

    def test_reject_http_and_localhost(self):
        with self.assertRaises(ProductLinkExtractError):
            assert_safe_product_url("http://robu.in/product/x")
        with self.assertRaises(ProductLinkExtractError):
            assert_safe_product_url("https://127.0.0.1/product")

    def test_missing_api_key(self):
        with override_settings(SCRAPINGBEE_API_KEY=""):
            with patch.dict("os.environ", {"SCRAPINGBEE_API_KEY": ""}, clear=False):
                out = extract_product_from_url("https://robu.in/product/arduino/")
        self.assertFalse(out["ok"])
        self.assertFalse(out["configured"])
        self.assertIn("SCRAPINGBEE_API_KEY", out["error"])

    def test_maps_scrapingbee_json(self):
        class FakeResp:
            status_code = 200
            text = "{}"

            def json(self):
                return {
                    "title": "Arduino Uno R3",
                    "price": "422.73",
                    "currency": "INR",
                    "image": "https://cdn.example/a.jpg",
                    "available": True,
                }

        with override_settings(SCRAPINGBEE_API_KEY="test-key"):
            with patch("crm_records.product_link_extract.requests.get", return_value=FakeResp()):
                out = extract_product_from_url("https://robu.in/product/arduino-uno-r3/")
        self.assertTrue(out["ok"])
        self.assertEqual(out["title"], "Arduino Uno R3")
        self.assertEqual(out["price"], 422.73)
        self.assertEqual(out["vendor"], "ROBU")
        self.assertEqual(out["method"], "scrapingbee")
        self.assertEqual(out["image"], "https://cdn.example/a.jpg")

    def test_normalize_image_url(self):
        self.assertEqual(normalize_image_url("//cdn.example/a.jpg"), "https://cdn.example/a.jpg")
        self.assertEqual(normalize_image_url("http://cdn.example/a.jpg"), "https://cdn.example/a.jpg")
        self.assertEqual(normalize_image_url("https://cdn.example/a.jpg"), "https://cdn.example/a.jpg")
        self.assertIsNone(normalize_image_url("/relative.jpg"))
        self.assertIsNone(normalize_image_url(""))
