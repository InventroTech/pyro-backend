"""
Simple tests for HasAPISecret permission (crm_records/permissions.py).
Plain secret matching: PYRO_SECRET or ApiSecretKey.secret. Run with: manage.py test
"""
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, override_settings

from crm_records.permissions import HasAPISecret


def _make_request(secret_header):
    request = MagicMock()
    request.headers = {"X-Secret-Pyro": secret_header, "x-secret-pyro": secret_header}
    request.META = {"HTTP_X_SECRET_PYRO": secret_header}
    return request


@override_settings(
    PYRO_SECRET=None,
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
)
class HasAPISecretTests(SimpleTestCase):
    def test_missing_header_returns_false(self):
        request = _make_request("")
        request.headers = {}
        request.META = {}
        perm = HasAPISecret()
        self.assertFalse(perm.has_permission(request, None))

    def test_pyro_secret_from_settings_returns_true(self):
        request = _make_request("env-secret-123")
        perm = HasAPISecret()
        with override_settings(PYRO_SECRET="env-secret-123"):
            result = perm.has_permission(request, None)
        self.assertTrue(result)
        self.assertTrue(getattr(request, "is_default_secret", False))

    def test_valid_db_secret_returns_true(self):
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
        MockApiSecretKey.objects.filter.assert_called_once()
        call_kwargs = MockApiSecretKey.objects.filter.call_args[1]
        self.assertEqual(call_kwargs["secret"], secret)
        self.assertTrue(call_kwargs["is_active"])

    def test_invalid_secret_returns_false(self):
        request = _make_request("invalid-secret")
        perm = HasAPISecret()
        mock_chain = MagicMock()
        mock_chain.select_related.return_value.first.return_value = None
        with patch("crm_records.models.ApiSecretKey") as MockApiSecretKey:
            MockApiSecretKey.objects.filter.return_value = mock_chain
            result = perm.has_permission(request, None)
        self.assertFalse(result)

    def test_header_case_insensitive(self):
        request = MagicMock()
        request.headers = {"x-secret-pyro": "lowercase-secret"}
        request.META = {}
        perm = HasAPISecret()
        mock_secret_obj = MagicMock()
        mock_secret_obj.id = 1
        mock_secret_obj.tenant_id = "tid"
        mock_secret_obj.tenant.slug = "t"
        mock_chain = MagicMock()
        mock_chain.select_related.return_value.first.return_value = mock_secret_obj
        with patch("crm_records.models.ApiSecretKey") as MockApiSecretKey:
            MockApiSecretKey.objects.filter.return_value = mock_chain
            result = perm.has_permission(request, None)
        self.assertTrue(result)
        MockApiSecretKey.objects.filter.assert_called_once()
        self.assertEqual(MockApiSecretKey.objects.filter.call_args[1]["secret"], "lowercase-secret")
