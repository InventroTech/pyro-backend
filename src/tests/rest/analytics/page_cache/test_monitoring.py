"""Monitoring — track hits, misses, computes, invalidations."""
from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from tests.rest.analytics.page_cache.helpers import LOC_MEM_CACHES
from utils.cache_utils import (
    cached_payload,
    get_page_cache_stats,
    invalidate_cache_family,
    reset_page_cache_stats,
)


@override_settings(CACHES=LOC_MEM_CACHES)
class MonitoringTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        reset_page_cache_stats()

    def tearDown(self):
        cache.clear()
        reset_page_cache_stats()

    def test_stats_counters_for_hit_miss_invalidate(self):
        cached_payload(prefix="mon:a", tenant_id=1, compute=lambda: {"x": 1}, ttl=30)
        cached_payload(prefix="mon:a", tenant_id=1, compute=lambda: {"x": 2}, ttl=30)
        invalidate_cache_family(1, "mon")
        stats = get_page_cache_stats()
        self.assertEqual(stats["miss"], 1)
        self.assertEqual(stats["hit"], 1)
        self.assertEqual(stats["compute"], 1)
        self.assertEqual(stats["invalidate"], 1)

    def test_reset_stats(self):
        cached_payload(prefix="mon:b", tenant_id=1, compute=lambda: 1, ttl=30)
        reset_page_cache_stats()
        self.assertEqual(get_page_cache_stats()["miss"], 0)
        self.assertEqual(get_page_cache_stats()["hit"], 0)
