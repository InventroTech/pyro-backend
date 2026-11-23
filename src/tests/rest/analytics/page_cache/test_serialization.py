"""Serialization — store Redis-safe JSON-like payloads."""
from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from tests.rest.analytics.page_cache.helpers import LOC_MEM_CACHES
from utils.cache_utils import cached_payload, reset_page_cache_stats


@override_settings(CACHES=LOC_MEM_CACHES)
class SerializationTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        reset_page_cache_stats()

    def tearDown(self):
        cache.clear()
        reset_page_cache_stats()

    def test_dict_list_and_primitives_roundtrip(self):
        payload = {
            "count": 3,
            "ratio": 1.5,
            "ok": True,
            "items": [{"id": 1}, {"id": 2}],
            "label": "CSE",
        }

        cached = cached_payload(
            prefix="test:ser",
            tenant_id=1,
            compute=lambda: payload,
            ttl=30,
        )
        again = cached_payload(
            prefix="test:ser",
            tenant_id=1,
            compute=lambda: {"should": "not-run"},
            ttl=30,
        )
        self.assertEqual(cached, payload)
        self.assertEqual(again, payload)
