"""Cache keys — every cached item gets a unique key."""
from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from tests.rest.analytics.page_cache.helpers import LOC_MEM_CACHES
from utils.cache_utils import (
    build_cache_key,
    cache_family,
    cache_key_fingerprint,
    reset_page_cache_stats,
)


@override_settings(CACHES=LOC_MEM_CACHES)
class CacheKeysTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        reset_page_cache_stats()

    def tearDown(self):
        cache.clear()
        reset_page_cache_stats()

    def test_key_includes_prefix_version_and_tenant(self):
        key = build_cache_key("cse:overview", 42, date="2026-01-15")
        self.assertTrue(key.startswith("cse:overview:v1:42:"))
        self.assertIn("date=2026-01-15", key)

    def test_key_stable_when_dict_part_order_differs(self):
        a = build_cache_key("cse:overview", 1, af={"b": [2, 1], "a": ["x"]})
        b = build_cache_key("cse:overview", 1, af={"a": ["x"], "b": [1, 2]})
        self.assertEqual(a, b)

    def test_different_tenants_get_different_keys(self):
        a = build_cache_key("team:overview", 1, date="2026-01-15")
        b = build_cache_key("team:overview", 2, date="2026-01-15")
        self.assertNotEqual(a, b)

    def test_different_filters_get_different_keys(self):
        a = build_cache_key("cse:overview", 1, from_="2026-01-01", to="2026-01-07")
        b = build_cache_key("cse:overview", 1, from_="2026-01-08", to="2026-01-14")
        self.assertNotEqual(a, b)

    def test_different_prefixes_get_different_keys(self):
        a = build_cache_key("cse:overview", 1, date="2026-01-15")
        b = build_cache_key("cse:members", 1, date="2026-01-15")
        self.assertNotEqual(a, b)

    def test_fingerprint_changes_when_filters_change(self):
        self.assertNotEqual(
            cache_key_fingerprint({"state": ["KA"]}),
            cache_key_fingerprint({"state": ["MH"]}),
        )

    def test_long_parts_are_hashed_into_short_key_segment(self):
        long_value = "x" * 200
        key = build_cache_key("cse:overview", 1, blob=long_value)
        self.assertNotIn(long_value, key)
        self.assertRegex(key, r"blob=[0-9a-f]{16}")

    def test_cache_family_from_prefix(self):
        self.assertEqual(cache_family("cse:overview"), "cse")
        self.assertEqual(cache_family("team:unassigned"), "team")
        self.assertEqual(cache_family("rm:daily_targets"), "rm")
