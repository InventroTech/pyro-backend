"""
Unit tests for signup flow: SetupNewTenantView (setup-new-tenant endpoint).
Uses mocked DB (no migrations). Run: python manage.py test tests.rest.accounts.test_signup_setup_new_tenant
"""
from unittest.mock import MagicMock, patch
from django.test import SimpleTestCase
from rest_framework import status

from accounts.views import SetupNewTenantView


def _make_request(tenant_slug="my-org", tenant_name="", user_email="user@example.com", user_uid="uid-123"):
    """Build a minimal request object with .data and .user for the view."""
    request = MagicMock()
    request.data = {"tenant_slug": tenant_slug, "tenant_name": tenant_name}
    request.user = MagicMock(email=user_email, supabase_uid=user_uid)
    return request


class SignupSetupNewTenantValidationTests(SimpleTestCase):
    """Validation and error responses (400)."""

    def test_missing_tenant_slug_returns_400(self):
        request = _make_request(tenant_slug="")
        response = SetupNewTenantView().post(request)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("tenant_slug is required", response.data.get("error", ""))

    def test_invalid_slug_after_slugify_returns_400(self):
        request = _make_request(tenant_slug="---")
        response = SetupNewTenantView().post(request)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("letter or number", response.data.get("error", ""))

    def test_missing_user_email_returns_400(self):
        request = _make_request()
        request.user.email = None
        response = SetupNewTenantView().post(request)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("email", response.data.get("error", "").lower())

    def test_missing_supabase_uid_returns_400(self):
        request = _make_request()
        request.user.supabase_uid = None
        response = SetupNewTenantView().post(request)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("supabase_uid", response.data.get("error", "").lower())


class SignupSetupNewTenantAlreadySetUpTests(SimpleTestCase):
    """User already has an active membership → 200."""

    @patch("accounts.views.TenantMembership")
    def test_already_has_membership_returns_200(self, mock_tm):
        existing = MagicMock()
        existing.tenant.id = "tenant-uuid"
        existing.tenant.slug = "my-org"
        existing.role.id = "role-uuid"
        existing.role.key = "pyro_admin"
        mock_tm.objects.filter.return_value.select_related.return_value.first.return_value = existing

        request = _make_request()
        response = SetupNewTenantView().post(request)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data.get("success"))
        self.assertEqual(response.data.get("tenant_slug"), "my-org")
        self.assertEqual(response.data.get("message"), "Already set up")


class SignupSetupNewTenantCreateTests(SimpleTestCase):
    """New tenant creation → 201."""

    @patch("accounts.views.TenantMembership")
    @patch("accounts.views.Role")
    @patch("accounts.views.Tenant")
    @patch("accounts.views.transaction")
    def test_new_tenant_returns_201(self, mock_transaction, mock_tenant, mock_role, mock_tm):
        mock_transaction.atomic.return_value.__enter__ = lambda s: None
        mock_transaction.atomic.return_value.__exit__ = lambda s, *a: None
        mock_tm.objects.filter.return_value.select_related.return_value.first.return_value = None

        tenant = MagicMock()
        tenant.id = "new-tenant-uuid"
        tenant.slug = "my-org"
        mock_tenant.objects.get_or_create.return_value = (tenant, True)

        role = MagicMock()
        role.id = "role-uuid"
        role.key = "pyro_admin"
        mock_role.objects.get_or_create.return_value = (role, True)

        membership = MagicMock()
        membership.user_id = "uid-123"
        mock_tm.objects.get_or_create.return_value = (membership, True)

        request = _make_request()
        response = SetupNewTenantView().post(request)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertTrue(response.data.get("success"))
        self.assertEqual(response.data.get("tenant_slug"), "my-org")
        self.assertEqual(response.data.get("role_key"), "pyro_admin")


class SignupSetupNewTenantConflictTests(SimpleTestCase):
    """Slug already taken → 409."""

    @patch("accounts.views.TenantMembership")
    @patch("accounts.views.Tenant")
    @patch("accounts.views.transaction")
    def test_slug_already_taken_returns_409(self, mock_transaction, mock_tenant, mock_tm):
        mock_transaction.atomic.return_value.__enter__ = lambda s: None
        mock_transaction.atomic.return_value.__exit__ = lambda s, *a: None
        mock_tm.objects.filter.return_value.select_related.return_value.first.return_value = None

        tenant = MagicMock()
        tenant.slug = "my-org"
        mock_tenant.objects.get_or_create.return_value = (tenant, False)

        request = _make_request(tenant_slug="my-org")
        response = SetupNewTenantView().post(request)

        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertIn("already taken", response.data.get("error", ""))
