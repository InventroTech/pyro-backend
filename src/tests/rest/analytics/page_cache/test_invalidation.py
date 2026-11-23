"""Invalidation — version bump makes old keys unused when DB data changes."""
from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from tests.rest.analytics.page_cache.helpers import LOC_MEM_CACHES
from utils.cache_utils import (
    build_cache_key,
    cached_payload,
    get_cache_version,
    invalidate_cache_families,
    invalidate_cache_family,
    invalidate_cse_analytics_cache,
    invalidate_rm_prd_cache,
    invalidate_team_analytics_cache,
    invalidate_ticket_filter_options_cache,
    reset_page_cache_stats,
)


@override_settings(CACHES=LOC_MEM_CACHES)
class InvalidationTests(SimpleTestCase):
    def setUp(self):
        cache.clear()
        reset_page_cache_stats()

    def tearDown(self):
        cache.clear()
        reset_page_cache_stats()

    def test_invalidate_family_bumps_version_and_forces_recompute(self):
        calls = {"n": 0}

        def compute():
            calls["n"] += 1
            return {"n": calls["n"]}

        cached_payload(prefix="cse:overview", tenant_id=7, compute=compute, ttl=60)
        self.assertEqual(get_cache_version("cse", 7), 1)

        invalidate_cache_family(7, "cse")
        self.assertEqual(get_cache_version("cse", 7), 2)
        self.assertIn(":v2:", build_cache_key("cse:overview", 7))

        cached_payload(prefix="cse:overview", tenant_id=7, compute=compute, ttl=60)
        self.assertEqual(calls["n"], 2)

    def test_invalidate_helpers_target_expected_families(self):
        invalidate_cse_analytics_cache(9)
        invalidate_team_analytics_cache(9)
        invalidate_rm_prd_cache(9)
        invalidate_ticket_filter_options_cache(9)
        self.assertEqual(get_cache_version("cse", 9), 2)
        self.assertEqual(get_cache_version("team", 9), 2)
        self.assertEqual(get_cache_version("rm", 9), 2)
        self.assertEqual(get_cache_version("ticket", 9), 2)

    def test_invalidate_families_batch(self):
        invalidate_cache_families(3, ["cse", "team"])
        self.assertEqual(get_cache_version("cse", 3), 2)
        self.assertEqual(get_cache_version("team", 3), 2)
        self.assertEqual(get_cache_version("rm", 3), 1)

    def test_invalidating_one_family_does_not_affect_another(self):
        calls = {"cse": 0, "team": 0}

        def cse_compute():
            calls["cse"] += 1
            return {"cse": calls["cse"]}

        def team_compute():
            calls["team"] += 1
            return {"team": calls["team"]}

        cached_payload(prefix="cse:overview", tenant_id=1, compute=cse_compute, ttl=60)
        cached_payload(prefix="team:overview", tenant_id=1, compute=team_compute, ttl=60)
        invalidate_cse_analytics_cache(1)

        cached_payload(prefix="cse:overview", tenant_id=1, compute=cse_compute, ttl=60)
        cached_payload(prefix="team:overview", tenant_id=1, compute=team_compute, ttl=60)
        self.assertEqual(calls["cse"], 2)
        self.assertEqual(calls["team"], 1)
