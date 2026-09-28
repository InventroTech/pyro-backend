"""Unit tests for product URL extract (no live ScrapingBee calls)."""

from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from crm_records.product_link_extract import (
    ProductLinkExtractError,
    assert_safe_product_url,
    extract_product_from_url,
    normalize_image_url,
    parse_price_number,
    parse_storefront_html,
    title_matches_product_slug,
    vendor_from_host,
)

ROBU_HX711_HTML = """
<html>
<head>
<meta property="og:title" content="SmartElex Load Cell Amplifier – HX711 - Robu.in | Indian Online Store" />
<meta property="og:image" content="https://cdn.robu.in/product.jpg" />
<script type="application/ld+json">
{"@type":"Product","name":"SmartElex Load Cell Amplifier – HX711","image":"https://cdn.robu.in/product.jpg","offers":{"@type":"Offer","price":"127.00","priceCurrency":"INR","availability":"https://schema.org/InStock"}}
</script>
</head>
<body>
<h1 class="product_title entry-title">SmartElex Load Cell Amplifier – HX711</h1>
<p class="price"><span class="woocommerce-Price-amount amount"><bdi><span class="woocommerce-Price-currencySymbol">₹</span>127.00</bdi></span></p>
</body>
</html>
"""


class _FakeResp:
    def __init__(self, status_code=200, text="{}", payload=None):
        self.status_code = status_code
        self.text = text
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


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

    def test_strips_google_ads_params_from_robu_url(self):
        safe = assert_safe_product_url(
            "https://robu.in/product/smartelex-load-cell-amplifier-hx711/"
            "?gad_source=1&gad_campaignid=17416544847&gbraid=0AAAAADvLFWc9n4hQB_pKe8_2gI1fCmSI9"
        )
        self.assertEqual(safe, "https://robu.in/product/smartelex-load-cell-amplifier-hx711/")
        self.assertNotIn("gad_source", safe)
        self.assertNotIn("gbraid", safe)

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

    def test_maps_scrapingbee_json_for_amazon(self):
        resp = _FakeResp(
            text='{"title":"Arduino Uno R3"}',
            payload={
                "title": "Arduino Uno R3",
                "price": "422.73",
                "currency": "INR",
                "image": "https://cdn.example/a.jpg",
                "available": True,
            },
        )
        with override_settings(SCRAPINGBEE_API_KEY="test-key"):
            with patch("crm_records.product_link_extract.requests.get", return_value=resp) as mock_get:
                out = extract_product_from_url("https://www.amazon.in/dp/B0H1K35VB3")
                params = mock_get.call_args.kwargs["params"]
        self.assertTrue(out["ok"])
        self.assertEqual(out["title"], "Arduino Uno R3")
        self.assertEqual(out["price"], 422.73)
        self.assertEqual(out["vendor"], "AMAZON")
        self.assertEqual(out["debug"]["extract"], "ai")
        self.assertIn("ai_extract_rules", params)
        self.assertNotIn("wait", params)

    def test_robu_parses_html_without_ai_rules(self):
        url = (
            "https://robu.in/product/smartelex-load-cell-amplifier-hx711/"
            "?gad_source=1&gad_campaignid=17416544847"
        )
        resp = _FakeResp(text=ROBU_HX711_HTML)
        with override_settings(SCRAPINGBEE_API_KEY="test-key"):
            with patch("crm_records.product_link_extract.requests.get", return_value=resp) as mock_get:
                out = extract_product_from_url(url)
                params = mock_get.call_args.kwargs["params"]
        self.assertTrue(out["ok"])
        self.assertEqual(out["title"], "SmartElex Load Cell Amplifier – HX711")
        self.assertEqual(out["price"], 127.0)
        self.assertEqual(out["currency"], "INR")
        self.assertEqual(out["vendor"], "ROBU")
        self.assertEqual(out["image"], "https://cdn.robu.in/product.jpg")
        self.assertTrue(out["available"])
        self.assertEqual(out["debug"]["extract"], "html")
        self.assertNotIn("ai_extract_rules", params)
        self.assertEqual(params["wait"], "4000")
        self.assertEqual(params["wait_browser"], "domcontentloaded")
        self.assertEqual(params["premium_proxy"], "true")
        self.assertEqual(params["country_code"], "in")
        self.assertEqual(params["url"], "https://robu.in/product/smartelex-load-cell-amplifier-hx711/")

    def test_robu_ignores_cloudflare_script_on_real_product_page(self):
        html_text = (
            '<script src="https://challenges.cloudflare.com/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script>\n'
            + ROBU_HX711_HTML
        )
        resp = _FakeResp(text=html_text)
        with override_settings(SCRAPINGBEE_API_KEY="test-key"):
            with patch("crm_records.product_link_extract.requests.get", return_value=resp):
                out = extract_product_from_url(
                    "https://robu.in/product/smartelex-load-cell-amplifier-hx711/"
                )
        self.assertTrue(out["ok"], out)
        self.assertEqual(out["title"], "SmartElex Load Cell Amplifier – HX711")
        self.assertEqual(out["price"], 127.0)
        self.assertFalse(out["debug"].get("challenge"))

    def test_robu_rejects_unrelated_title(self):
        html_text = """
        <html><head>
        <meta property="og:title" content="Men's Performance T-Shirt" />
        <meta property="product:price:amount" content="29.99" />
        </head></html>
        """
        resp = _FakeResp(text=html_text)
        with override_settings(SCRAPINGBEE_API_KEY="test-key"):
            with patch("crm_records.product_link_extract.requests.get", return_value=resp):
                out = extract_product_from_url(
                    "https://robu.in/product/smartelex-load-cell-amplifier-hx711/"
                )
        self.assertFalse(out["ok"])
        self.assertTrue(out["debug"].get("slug_mismatch"))
        self.assertIsNone(out["title"])

    def test_parse_storefront_html_json_ld(self):
        parsed = parse_storefront_html(ROBU_HX711_HTML)
        self.assertEqual(parsed["title"], "SmartElex Load Cell Amplifier – HX711")
        self.assertEqual(parsed["price"], 127.0)
        self.assertEqual(parsed["currency"], "INR")
        self.assertEqual(parsed["image"], "https://cdn.robu.in/product.jpg")

    def test_title_matches_product_slug(self):
        url = "https://robu.in/product/smartelex-load-cell-amplifier-hx711/"
        self.assertTrue(title_matches_product_slug("SmartElex Load Cell Amplifier – HX711", url))
        self.assertFalse(title_matches_product_slug("Men's Performance T-Shirt", url))

    def test_normalize_image_url(self):
        self.assertEqual(normalize_image_url("//cdn.example/a.jpg"), "https://cdn.example/a.jpg")
        self.assertEqual(normalize_image_url("http://cdn.example/a.jpg"), "https://cdn.example/a.jpg")
        self.assertEqual(normalize_image_url("https://cdn.example/a.jpg"), "https://cdn.example/a.jpg")
        self.assertIsNone(normalize_image_url("/relative.jpg"))
        self.assertIsNone(normalize_image_url(""))
