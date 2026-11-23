"""Cache misses — fetch/compute when cache does not contain the item."""
from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from tests.rest.analytics.page_cache.helpers import LOC_MEM_CACHES
from utils.cache_utils import cached_payload, get_page_cache_stats, reset_page_cache_stats


@override_settings(CACHES=LOC_MEM_CACHES)
class CacheMissAndHitTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        reset_page_cache_stats()

    def tearDown(self):
        cache.clear()
        reset_page_cache_stats()

    def test_miss_then_hit_reuses_payload(self):
        calls = {"n": 0}

        def compute():
            calls["n"] += 1
            return {"ok": True, "n": calls["n"]}

        first = cached_payload(
            prefix="test:payload",
            tenant_id=9,
            parts={"date": "2026-01-01"},
            compute=compute,
            ttl=30,
        )
        second = cached_payload(
            prefix="test:payload",
            tenant_id=9,
            parts={"date": "2026-01-01"},
            compute=compute,
            ttl=30,
        )
        self.assertEqual(first, {"ok": True, "n": 1})
        self.assertEqual(second, {"ok": True, "n": 1})
        self.assertEqual(calls["n"], 1)
        stats = get_page_cache_stats()
        self.assertEqual(stats["miss"], 1)
        self.assertEqual(stats["hit"], 1)
        self.assertEqual(stats["compute"], 1)
