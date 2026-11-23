"""Expiration (TTL) — data remains cached only for the configured timeout."""
import time

from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from tests.rest.analytics.page_cache.helpers import LOC_MEM_CACHES
from utils.cache_utils import (
    DEFAULT_TTL_SECONDS,
    FILTER_OPTIONS_TTL_SECONDS,
    LOCK_TTL_SECONDS,
    cached_payload,
    reset_page_cache_stats,
)


@override_settings(CACHES=LOC_MEM_CACHES)
class ExpirationTtlTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        reset_page_cache_stats()

    def tearDown(self):
        cache.clear()
        reset_page_cache_stats()

    def test_default_ttl_constants(self):
        self.assertEqual(DEFAULT_TTL_SECONDS, 45)
        self.assertEqual(FILTER_OPTIONS_TTL_SECONDS, 60)
        self.assertEqual(LOCK_TTL_SECONDS, 10)

    def test_cached_value_expires_after_ttl(self):
        calls = {"n": 0}

        def compute():
            calls["n"] += 1
            return {"n": calls["n"]}

        cached_payload(prefix="test:ttl", tenant_id=1, compute=compute, ttl=1)
        self.assertEqual(calls["n"], 1)

        cached_payload(prefix="test:ttl", tenant_id=1, compute=compute, ttl=1)
        self.assertEqual(calls["n"], 1)

        time.sleep(1.1)
        cached_payload(prefix="test:ttl", tenant_id=1, compute=compute, ttl=1)
        self.assertEqual(calls["n"], 2)
