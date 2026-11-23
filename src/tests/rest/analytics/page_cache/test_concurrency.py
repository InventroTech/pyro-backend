"""Concurrency — stampede lock avoids multiple rebuilds of the same entry."""
import threading
import time

from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from tests.rest.analytics.page_cache.helpers import LOC_MEM_CACHES
from utils.cache_utils import (
    build_cache_key,
    cached_payload,
    get_page_cache_stats,
    reset_page_cache_stats,
)


@override_settings(CACHES=LOC_MEM_CACHES)
class ConcurrencyTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        reset_page_cache_stats()

    def tearDown(self):
        cache.clear()
        reset_page_cache_stats()

    def test_stampede_lock_waits_for_other_worker(self):
        calls = {"n": 0}

        def compute():
            calls["n"] += 1
            return {"n": calls["n"]}

        key = build_cache_key("test:stampede", 3)
        lock_key = f"{key}:lock"
        cache.add(lock_key, "1", timeout=10)

        def release_and_populate():
            time.sleep(0.08)
            cache.set(key, {"n": 99}, timeout=30)
            cache.delete(lock_key)

        threading.Thread(target=release_and_populate, daemon=True).start()
        payload = cached_payload(prefix="test:stampede", tenant_id=3, compute=compute, ttl=30)
        self.assertEqual(payload, {"n": 99})
        self.assertEqual(calls["n"], 0)
        self.assertGreaterEqual(get_page_cache_stats()["lock_wait"], 1)
