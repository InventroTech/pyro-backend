"""Stale data — refresh bypass and scoped keys avoid serving wrong/outdated data."""
from unittest.mock import MagicMock

from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from tests.rest.analytics.page_cache.helpers import LOC_MEM_CACHES
from utils.cache_utils import (
    cached_payload,
    get_page_cache_stats,
    reset_page_cache_stats,
    wants_refresh,
)


@override_settings(CACHES=LOC_MEM_CACHES)
class StaleDataTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        reset_page_cache_stats()

    def tearDown(self):
        cache.clear()
        reset_page_cache_stats()

    def test_refresh_bypasses_cache(self):
        calls = {"n": 0}

        def compute():
            calls["n"] += 1
            return {"n": calls["n"]}

        cached_payload(prefix="test:refresh", tenant_id=1, compute=compute, ttl=30)
        cached_payload(
            prefix="test:refresh",
            tenant_id=1,
            compute=compute,
            ttl=30,
            refresh=True,
        )
        self.assertEqual(calls["n"], 2)
        self.assertEqual(get_page_cache_stats()["refresh"], 1)

    def test_wants_refresh_parses_query_param(self):
        request = MagicMock()
        request.query_params = {"refresh": "1"}
        self.assertTrue(wants_refresh(request))
        request.query_params = {"refresh": "true"}
        self.assertTrue(wants_refresh(request))
        request.query_params = {"refresh": "0"}
        self.assertFalse(wants_refresh(request))
        request.query_params = {}
        self.assertFalse(wants_refresh(request))

    def test_scoped_keys_do_not_leak_across_managers(self):
        calls = {"a": 0, "b": 0}

        def compute_a():
            calls["a"] += 1
            return {"manager": "a"}

        def compute_b():
            calls["b"] += 1
            return {"manager": "b"}

        cached_payload(
            prefix="team:overview",
            tenant_id=1,
            parts={"manager": "a"},
            compute=compute_a,
            ttl=30,
        )
        cached_payload(
            prefix="team:overview",
            tenant_id=1,
            parts={"manager": "b"},
            compute=compute_b,
            ttl=30,
        )
        self.assertEqual(calls["a"], 1)
        self.assertEqual(calls["b"], 1)
