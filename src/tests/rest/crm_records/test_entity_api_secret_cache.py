"""
Unit tests for HasAPISecret permission caching (crm_records/permissions.py).
Verifies cache get/set/delete behavior for /entity/ API secret validation.
"""

import hashlib
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, override_settings

from crm_records.permissions import (
    API_SECRET_CACHE_KEY_PREFIX,
    API_SECRET_CACHE_TTL,
    HasAPISecret,
)


def _make_request(secret_header: str):
    """Build a minimal request with X-Secret-Pyro header."""
    request = MagicMock()
    request.headers = {"X-Secret-Pyro": secret_header, "x-secret-pyro": secret_header}
    request.META = {"HTTP_X_SECRET_PYRO": secret_header}
    return request


def _cache_key_for_secret(secret: str) -> str:
    return API_SECRET_CACHE_KEY_PREFIX + hashlib.sha256(secret.encode()).hexdigest()


@override_settings(
    PYRO_SECRET=None,  # Force DB/cache path in tests that need it
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    },
)
class HasAPISecretCacheTests(SimpleTestCase):
    """Tests for API secret permission caching."""

    def test_missing_header_returns_false(self):
        """No X-Secret-Pyro header → permission denied, cache not used."""
        request = _make_request("")
        request.headers = {}
        request.META = {}
        perm = HasAPISecret()
        with patch("crm_records.permissions.cache") as mock_cache:
            result = perm.has_permission(request, None)
        self.assertFalse(result)
        mock_cache.get.assert_not_called()
        mock_cache.set.assert_not_called()

    def test_pyro_secret_bypasses_cache(self):
        """When secret matches PYRO_SECRET, cache is never read or written."""
        request = _make_request("env-secret-123")
        perm = HasAPISecret()
        with patch("crm_records.permissions.cache") as mock_cache:
            with override_settings(PYRO_SECRET="env-secret-123"):
                result = perm.has_permission(request, None)
        self.assertTrue(result)
        mock_cache.get.assert_not_called()
        mock_cache.set.assert_not_called()
        self.assertTrue(getattr(request, "is_default_secret", False))

    def test_cache_miss_then_set_on_valid_db_secret(self):
        """Cache miss: cache.get returns None, DB lookup succeeds, cache.set called."""
        secret = "db-secret-456"
        request = _make_request(secret)
        perm = HasAPISecret()

        mock_secret_obj = MagicMock()
        mock_secret_obj.id = 42
        mock_secret_obj.tenant_id = "tenant-uuid-99"
        mock_secret_obj.tenant.slug = "test-tenant"

        mock_chain = MagicMock()
        mock_chain.extra.return_value.select_related.return_value.first.return_value = (
            mock_secret_obj
        )
        mock_chain.select_related.return_value.first.return_value = mock_secret_obj

        with patch("crm_records.permissions.cache") as mock_cache:
            mock_cache.get.return_value = None  # cache miss
            with patch("crm_records.models.ApiSecretKey") as MockApiSecretKey:
                MockApiSecretKey.objects.filter.return_value = mock_chain
                result = perm.has_permission(request, None)

        self.assertTrue(result)
        mock_cache.get.assert_called_once_with(_cache_key_for_secret(secret))
        mock_cache.set.assert_called_once()
        call_args = mock_cache.set.call_args[0]
        self.assertEqual(call_args[0], _cache_key_for_secret(secret))
        self.assertEqual(call_args[1]["api_secret_key_id"], 42)
        self.assertEqual(call_args[1]["tenant_id"], "tenant-uuid-99")
        self.assertEqual(call_args[2], API_SECRET_CACHE_TTL)
        self.assertFalse(getattr(request, "is_default_secret", True))

    def test_cache_hit_grants_permission_without_crypt_query(self):
        """Cache hit: cache.get returns cached id, only PK lookup used, permission granted."""
        secret = "cached-secret-789"
        request = _make_request(secret)
        perm = HasAPISecret()

        mock_secret_obj = MagicMock()
        mock_secret_obj.id = 99
        mock_secret_obj.tenant_id = "tenant-uuid-11"
        mock_secret_obj.tenant.slug = "cached-tenant"

        mock_chain = MagicMock()
        mock_chain.select_related.return_value.first.return_value = mock_secret_obj

        with patch("crm_records.permissions.cache") as mock_cache:
            mock_cache.get.return_value = {
                "api_secret_key_id": 99,
                "tenant_id": "tenant-uuid-11",
            }
            with patch("crm_records.models.ApiSecretKey") as MockApiSecretKey:
                MockApiSecretKey.objects.filter.return_value = mock_chain
                result = perm.has_permission(request, None)

        self.assertTrue(result)
        mock_cache.get.assert_called_once_with(_cache_key_for_secret(secret))
        mock_cache.set.assert_not_called()
        # Cache hit path uses filter(pk=..., is_active=True), not .extra(crypt)
        MockApiSecretKey.objects.filter.assert_called_once()
        call_kwargs = MockApiSecretKey.objects.filter.call_args[1]
        self.assertIn("pk", call_kwargs)
        self.assertEqual(call_kwargs["pk"], 99)
        self.assertTrue(call_kwargs["is_active"])

    def test_cache_hit_but_key_inactive_deletes_cache_and_denies(self):
        """Cache hit but key no longer active: cache.delete called, permission denied."""
        secret = "stale-cached-secret"
        request = _make_request(secret)
        perm = HasAPISecret()

        mock_chain = MagicMock()
        mock_chain.select_related.return_value.first.return_value = None  # inactive or deleted

        with patch("crm_records.permissions.cache") as mock_cache:
            mock_cache.get.return_value = {"api_secret_key_id": 1, "tenant_id": "t1"}
            with patch("crm_records.models.ApiSecretKey") as MockApiSecretKey:
                MockApiSecretKey.objects.filter.return_value = mock_chain
                # Crypt path also returns None so we don't re-grant
                mock_chain.extra.return_value.select_related.return_value.first.return_value = None
                result = perm.has_permission(request, None)

        self.assertFalse(result)
        mock_cache.delete.assert_called_once_with(_cache_key_for_secret(secret))
        mock_cache.set.assert_not_called()

    def test_invalid_secret_not_cached(self):
        """Invalid secret (DB lookup fails): cache.set never called."""
        secret = "invalid-secret"
        request = _make_request(secret)
        perm = HasAPISecret()

        mock_chain = MagicMock()
        mock_chain.extra.return_value.select_related.return_value.first.return_value = None
        mock_chain.select_related.return_value.first.return_value = None

        with patch("crm_records.permissions.cache") as mock_cache:
            mock_cache.get.return_value = None
            with patch("crm_records.models.ApiSecretKey") as MockApiSecretKey:
                MockApiSecretKey.objects.filter.return_value = mock_chain
                result = perm.has_permission(request, None)

        self.assertFalse(result)
        mock_cache.get.assert_called_once()
        mock_cache.set.assert_not_called()

    def test_header_case_insensitive(self):
        """x-secret-pyro (lowercase) is accepted like X-Secret-Pyro."""
        request = MagicMock()
        request.headers = {"x-secret-pyro": "lowercase-secret"}
        request.META = {}
        perm = HasAPISecret()

        mock_secret_obj = MagicMock()
        mock_secret_obj.id = 1
        mock_secret_obj.tenant_id = "tid"
        mock_secret_obj.tenant.slug = "t"

        mock_chain = MagicMock()
        mock_chain.extra.return_value.select_related.return_value.first.return_value = (
            mock_secret_obj
        )
        mock_chain.select_related.return_value.first.return_value = mock_secret_obj

        with patch("crm_records.permissions.cache") as mock_cache:
            mock_cache.get.return_value = None
            with patch("crm_records.models.ApiSecretKey") as MockApiSecretKey:
                MockApiSecretKey.objects.filter.return_value = mock_chain
                result = perm.has_permission(request, None)

        self.assertTrue(result)
        mock_cache.get.assert_called_once_with(
            _cache_key_for_secret("lowercase-secret")
        )
