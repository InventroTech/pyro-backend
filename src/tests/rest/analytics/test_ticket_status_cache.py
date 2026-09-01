from datetime import date, timedelta
from unittest.mock import patch

from django.core.cache import cache
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from analytics.views import (
    _TICKET_STATUS_TTL_SECONDS,
    _ticket_status_cache_key,
    invalidate_ticket_status_cache,
)
from tests.base.test_setup import BaseAPITestCase

LOC_MEM_CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    }
}


@override_settings(CACHES=LOC_MEM_CACHES)
class TicketStatusCacheKeyTests(BaseAPITestCase):
    def test_cache_key_includes_date_for_midnight_rollover(self):
        tenant_id = self.tenant.id
        user_id = "user-abc"
        day_one = date(2026, 9, 1)
        day_two = day_one + timedelta(days=1)

        key_day_one = _ticket_status_cache_key(tenant_id, user_id, day_one)
        key_day_two = _ticket_status_cache_key(tenant_id, user_id, day_two)

        self.assertNotEqual(key_day_one, key_day_two)
        self.assertIn(day_one.isoformat(), key_day_one)
        self.assertIn(day_two.isoformat(), key_day_two)

    def test_invalidate_deletes_only_today_key(self):
        tenant_id = self.tenant.id
        user_id = "user-abc"
        today = timezone.now().date()
        yesterday = today - timedelta(days=1)

        today_key = _ticket_status_cache_key(tenant_id, user_id, today)
        yesterday_key = _ticket_status_cache_key(tenant_id, user_id, yesterday)
        cache.set(today_key, {"ticketStats": {"resolvedByYouToday": 1}}, timeout=60)
        cache.set(yesterday_key, {"ticketStats": {"resolvedByYouToday": 9}}, timeout=60)

        invalidate_ticket_status_cache(tenant_id, user_id)

        self.assertIsNone(cache.get(today_key))
        self.assertIsNotNone(cache.get(yesterday_key))


@override_settings(CACHES=LOC_MEM_CACHES)
class GetTicketStatusCacheTests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        cache.clear()

    def tearDown(self):
        cache.clear()
        super().tearDown()

    @patch("analytics.views._compute_ticket_status")
    def test_get_or_set_reuses_cached_payload(self, compute_status):
        compute_status.return_value = {
            "success": True,
            "ticketStats": {"resolvedByYouToday": 3},
            "dateRange": {},
        }

        url = reverse("analytics:get-ticket-status")
        response_one = self.client.get(url, **self.auth_headers)
        response_two = self.client.get(url, **self.auth_headers)

        self.assertEqual(response_one.status_code, 200)
        self.assertEqual(response_two.status_code, 200)
        self.assertEqual(response_one.json()["ticketStats"]["resolvedByYouToday"], 3)
        compute_status.assert_called_once()

    @patch("analytics.views.cache.get_or_set")
    @patch("analytics.views._compute_ticket_status")
    def test_get_or_set_uses_ticket_status_ttl(self, compute_status, get_or_set):
        compute_status.return_value = {"success": True, "ticketStats": {}, "dateRange": {}}
        get_or_set.return_value = compute_status.return_value

        url = reverse("analytics:get-ticket-status")
        self.client.get(url, **self.auth_headers)

        _args, kwargs = get_or_set.call_args
        self.assertEqual(kwargs["timeout"], _TICKET_STATUS_TTL_SECONDS)
