"""
Unit tests for HasAPISecret permission behavior (crm_records/permissions.py).
These tests intentionally avoid cache-specific assertions.
"""

from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, override_settings

from crm_records.permissions import HasAPISecret


def _make_request(secret_header: str):
    request = MagicMock()
    request.headers = {"X-Secret-Pyro": secret_header, "x-secret-pyro": secret_header}
    request.META = {"HTTP_X_SECRET_PYRO": secret_header}
    return request


@override_settings(
    PYRO_SECRET=None,
)
class HasAPISecretTests(SimpleTestCase):
    def test_missing_header_returns_false(self):
        request = _make_request("")
        request.headers = {}
        request.META = {}
        perm = HasAPISecret()
        result = perm.has_permission(request, None)
        self.assertFalse(result)

    def test_pyro_secret_grants_permission(self):
        request = _make_request("env-secret-123")
        perm = HasAPISecret()
        with override_settings(PYRO_SECRET="env-secret-123"):
            result = perm.has_permission(request, None)
        self.assertTrue(result)
        self.assertTrue(getattr(request, "is_default_secret", False))
        self.assertEqual(getattr(request, "api_secret_key", ""), "env-secret-123")

    def test_valid_db_secret_grants_permission(self):
        secret = "db-secret-456"
        request = _make_request(secret)
        perm = HasAPISecret()

        mock_secret_obj = MagicMock()
        mock_secret_obj.id = 42
        mock_secret_obj.tenant_id = "tenant-uuid-99"
        mock_secret_obj.tenant.slug = "test-tenant"

        mock_chain = MagicMock()
        mock_chain.select_related.return_value.first.return_value = mock_secret_obj

        with patch("crm_records.models.ApiSecretKey") as MockApiSecretKey:
            MockApiSecretKey.objects.filter.return_value = mock_chain
            result = perm.has_permission(request, None)

        self.assertTrue(result)
        MockApiSecretKey.objects.filter.assert_called_once_with(
            secret=secret, is_active=True
        )
        self.assertFalse(getattr(request, "is_default_secret", True))
        self.assertEqual(getattr(request, "api_secret_key", ""), secret)
        self.assertIs(getattr(request, "api_secret_obj", None), mock_secret_obj)
        mock_secret_obj.save.assert_called_once_with(update_fields=["last_used_at"])

    def test_invalid_secret_returns_false(self):
        secret = "invalid-secret"
        request = _make_request(secret)
        perm = HasAPISecret()

        mock_chain = MagicMock()
        mock_chain.select_related.return_value.first.return_value = None

        with patch("crm_records.models.ApiSecretKey") as MockApiSecretKey:
            MockApiSecretKey.objects.filter.return_value = mock_chain
            result = perm.has_permission(request, None)

        self.assertFalse(result)
        MockApiSecretKey.objects.filter.assert_called_once_with(
            secret=secret, is_active=True
        )

    def test_header_case_insensitive(self):
        request = MagicMock()
        request.headers = {"x-secret-pyro": "lowercase-secret"}
        request.META = {}
        perm = HasAPISecret()

        mock_secret_obj = MagicMock()
        mock_chain = MagicMock()
        mock_chain.select_related.return_value.first.return_value = mock_secret_obj

        with patch("crm_records.models.ApiSecretKey") as MockApiSecretKey:
            MockApiSecretKey.objects.filter.return_value = mock_chain
            result = perm.has_permission(request, None)

        self.assertTrue(result)
        MockApiSecretKey.objects.filter.assert_called_once_with(
            secret="lowercase-secret", is_active=True
        )
