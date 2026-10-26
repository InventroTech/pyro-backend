"""
The settings page stores an inactive-email hour threshold (default 72) and shows
auth.users.last_sign_in_at. Memberships are not turned off.
"""
from datetime import timedelta

from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from accounts.models import SupabaseAuthUser
from authentication.models import User
from core.models import TenantSettings
from tests.base.test_setup import BaseAPITestCase
from tests.factories import TenantFactory


class InactiveEmailPolicyAPITests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.client.force_authenticate(user=self.user)
        self.url = reverse("inactive-email-policy")

    def test_get_returns_72_hour_default_without_creating_a_row(self):
        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["enabled"], True)
        self.assertEqual(response.data["inactive_after_hours"], 72)
        self.assertFalse(TenantSettings.objects.filter(tenant=self.tenant).exists())

    def test_patch_updates_the_rule_the_settings_page_saves(self):
        response = self.client.patch(
            self.url,
            {"enabled": False, "inactive_after_hours": 30},
            format="json",
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["enabled"], False)
        self.assertEqual(response.data["inactive_after_hours"], 30)
        row = TenantSettings.objects.get(tenant=self.tenant)
        self.assertFalse(row.inactive_emails_enabled)
        self.assertEqual(row.inactive_login_hours, 30)

    def test_patch_hours_leaves_enabled_unchanged(self):
        TenantSettings.objects.create(
            tenant=self.tenant,
            inactive_emails_enabled=False,
            inactive_login_hours=72,
        )

        response = self.client.patch(
            self.url,
            {"inactive_after_hours": 10},
            format="json",
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["enabled"], False)
        self.assertEqual(response.data["inactive_after_hours"], 10)

    def test_patch_rejects_a_non_positive_day_count(self):
        response = self.client.patch(
            self.url,
            {"inactive_after_hours": 0},
            format="json",
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, 400, response.data)

    def test_policy_is_scoped_to_the_tenant(self):
        tenant_b = TenantFactory()
        user_b, headers_b = self.make_authenticated_user(
            email="other-tenant@example.com",
            tenant=tenant_b,
        )
        self.client.patch(
            self.url,
            {"inactive_after_hours": 15},
            format="json",
            **self.auth_headers,
        )

        self.client.force_authenticate(user=user_b)
        response = self.client.get(self.url, **headers_b)

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["inactive_after_hours"], 72)

    def test_settings_user_list_shows_auth_users_last_sign_in_at(self):
        signed_in = timezone.now() - timedelta(days=2)
        SupabaseAuthUser.objects.filter(id=self.supabase_uid).update(last_sign_in_at=signed_in)

        response = self.client.get("/membership/users/", **self.auth_headers)

        self.assertEqual(response.status_code, 200, response.data)
        row = next(item for item in response.data["results"] if item["email"] == self.email)
        self.assertIsNotNone(row["last_sign_in_at"])
        self.assertAlmostEqual(row["last_sign_in_at"].timestamp(), signed_in.timestamp(), delta=1)

    @override_settings(AUTH_PROVIDER="django")
    def test_settings_user_list_shows_django_last_login(self):
        signed_in = timezone.now() - timedelta(days=4)
        User.objects.filter(pk=self.user.pk).update(last_login=signed_in)
        SupabaseAuthUser.objects.filter(id=self.supabase_uid).update(
            last_sign_in_at=timezone.now() - timedelta(days=40)
        )

        response = self.client.get("/membership/users/", **self.auth_headers)

        self.assertEqual(response.status_code, 200, response.data)
        row = next(item for item in response.data["results"] if item["email"] == self.email)
        self.assertAlmostEqual(row["last_sign_in_at"].timestamp(), signed_in.timestamp(), delta=1)
