"""Error handling — app still works if Redis is unavailable."""
from unittest.mock import patch

from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from tests.rest.analytics.page_cache.helpers import LOC_MEM_CACHES
from utils.cache_utils import cached_payload, get_page_cache_stats, reset_page_cache_stats


@override_settings(CACHES=LOC_MEM_CACHES)
class ErrorHandlingTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        reset_page_cache_stats()

    def tearDown(self):
        cache.clear()
        reset_page_cache_stats()

    @patch("utils.cache_utils.cache.get", side_effect=RuntimeError("redis down"))
    def test_get_failure_falls_back_to_compute(self, _get):
        calls = {"n": 0}

        def compute():
            calls["n"] += 1
            return {"ok": True}

        with patch("utils.cache_utils.cache.set"), patch(
            "utils.cache_utils.cache.add", return_value=True
        ), patch("utils.cache_utils.cache.delete"):
            payload = cached_payload(
                prefix="test:fallback",
                tenant_id=1,
                compute=compute,
                ttl=30,
            )
        self.assertEqual(payload, {"ok": True})
        self.assertEqual(calls["n"], 1)
        self.assertGreaterEqual(get_page_cache_stats()["get_error"], 1)

    def test_set_failure_still_returns_computed_payload(self):
        calls = {"n": 0}

        def compute():
            calls["n"] += 1
            return {"ok": True}

        with patch("utils.cache_utils.cache.set", side_effect=RuntimeError("set failed")):
            payload = cached_payload(
                prefix="test:setfail",
                tenant_id=1,
                compute=compute,
                ttl=30,
            )
        self.assertEqual(payload, {"ok": True})
        self.assertEqual(calls["n"], 1)
        self.assertGreaterEqual(get_page_cache_stats()["set_error"], 1)
