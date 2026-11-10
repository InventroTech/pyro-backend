"""
The settings page stores an inactive-email hour threshold (default 72).
Memberships are not turned off. Last sign-in tests live in test_last_sign_in.py.
"""
from django.test import TestCase
from django.urls import reverse

from core.models import TenantSettings
from tests.base.test_setup import BaseAPITestCase
from tests.factories import TenantFactory
from user_settings.inactivity import (
    DEFAULT_INACTIVE_LOGIN_HOURS,
    MAX_INACTIVE_LOGIN_HOURS,
    inactive_email_policy,
    save_inactive_email_policy,
)

DEFAULT_POLICY = {"enabled": True, "inactive_after_hours": DEFAULT_INACTIVE_LOGIN_HOURS}


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

    # --- more PATCH cases -------------------------------------------------

    def _patch(self, payload):
        return self.client.patch(self.url, payload, format="json", **self.auth_headers)

    def _assert_rejected_without_saving(self, payload):
        response = self._patch(payload)

        self.assertEqual(response.status_code, 400, response.data)
        self.assertFalse(TenantSettings.objects.filter(tenant=self.tenant).exists())

    def test_patch_rejects_more_than_a_year_of_hours(self):
        self._assert_rejected_without_saving({"inactive_after_hours": MAX_INACTIVE_LOGIN_HOURS + 1})

    def test_patch_rejects_negative_hours(self):
        self._assert_rejected_without_saving({"inactive_after_hours": -5})

    def test_patch_rejects_non_numeric_hours(self):
        self._assert_rejected_without_saving({"inactive_after_hours": "abc"})

    def test_patch_rejects_fractional_hours(self):
        self._assert_rejected_without_saving({"inactive_after_hours": 12.5})

    def test_patch_rejects_null_hours(self):
        self._assert_rejected_without_saving({"inactive_after_hours": None})

    def test_patch_rejects_a_non_boolean_enabled(self):
        self._assert_rejected_without_saving({"enabled": "maybe"})

    def test_bad_hours_do_not_save_a_valid_enabled_in_the_same_request(self):
        self._assert_rejected_without_saving({"enabled": False, "inactive_after_hours": 0})

    def test_patch_accepts_the_minimum_of_one_hour(self):
        response = self._patch({"inactive_after_hours": 1})

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(TenantSettings.objects.get(tenant=self.tenant).inactive_login_hours, 1)

    def test_patch_accepts_the_maximum_of_one_year(self):
        response = self._patch({"inactive_after_hours": MAX_INACTIVE_LOGIN_HOURS})

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(
            TenantSettings.objects.get(tenant=self.tenant).inactive_login_hours,
            MAX_INACTIVE_LOGIN_HOURS,
        )

    def test_patch_accepts_hours_sent_as_a_numeric_string(self):
        response = self._patch({"inactive_after_hours": "48"})

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["inactive_after_hours"], 48)

    def test_patch_enabled_only_leaves_hours_unchanged(self):
        TenantSettings.objects.create(tenant=self.tenant, inactive_emails_enabled=True, inactive_login_hours=24)

        response = self._patch({"enabled": False})

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data, {"enabled": False, "inactive_after_hours": 24})

    def test_empty_patch_returns_the_policy_without_creating_a_row(self):
        response = self._patch({})

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data, DEFAULT_POLICY)
        self.assertFalse(TenantSettings.objects.filter(tenant=self.tenant).exists())

    def test_unknown_fields_are_ignored_and_nothing_is_saved(self):
        response = self._patch({"something_else": 5})

        self.assertEqual(response.status_code, 200, response.data)
        self.assertFalse(TenantSettings.objects.filter(tenant=self.tenant).exists())

    def test_get_after_patch_returns_what_was_saved(self):
        self._patch({"enabled": False, "inactive_after_hours": 100})

        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.data, {"enabled": False, "inactive_after_hours": 100})

    def test_patching_twice_keeps_a_single_row(self):
        self._patch({"inactive_after_hours": 10})
        self._patch({"inactive_after_hours": 20})

        self.assertEqual(TenantSettings.objects.filter(tenant=self.tenant).count(), 1)
        self.assertEqual(TenantSettings.objects.get(tenant=self.tenant).inactive_login_hours, 20)

    def test_patch_does_not_change_another_tenants_row(self):
        tenant_b = TenantFactory()
        TenantSettings.objects.create(tenant=tenant_b, inactive_emails_enabled=True, inactive_login_hours=5)

        self._patch({"enabled": False, "inactive_after_hours": 99})

        row_b = TenantSettings.objects.get(tenant=tenant_b)
        self.assertTrue(row_b.inactive_emails_enabled)
        self.assertEqual(row_b.inactive_login_hours, 5)

    # --- access -----------------------------------------------------------

    def test_get_without_login_is_refused(self):
        self.client.force_authenticate(user=None)

        response = self.client.get(self.url, **self.make_unauthenticated_headers())

        self.assertIn(response.status_code, (401, 403))

    def test_patch_without_login_is_refused_and_saves_nothing(self):
        self.client.force_authenticate(user=None)

        response = self.client.patch(
            self.url,
            {"inactive_after_hours": 10},
            format="json",
            **self.make_unauthenticated_headers(),
        )

        self.assertIn(response.status_code, (401, 403))
        self.assertFalse(TenantSettings.objects.filter(tenant=self.tenant).exists())


# ---------------------------------------------------------------------------
# user_settings.inactivity.inactive_email_policy
# ---------------------------------------------------------------------------


class InactiveEmailPolicyFunctionTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()

    def _stored(self, **fields):
        TenantSettings.objects.update_or_create(tenant=self.tenant, defaults=fields)

    def test_no_tenant_returns_the_default(self):
        self.assertEqual(inactive_email_policy(None), DEFAULT_POLICY)

    def test_object_without_a_pk_returns_the_default(self):
        class NoPk:
            pk = None

        self.assertEqual(inactive_email_policy(NoPk()), DEFAULT_POLICY)

    def test_tenant_without_a_row_returns_the_default_and_creates_nothing(self):
        self.assertEqual(inactive_email_policy(self.tenant), DEFAULT_POLICY)
        self.assertFalse(TenantSettings.objects.filter(tenant=self.tenant).exists())

    def test_returns_the_stored_values(self):
        self._stored(inactive_emails_enabled=False, inactive_login_hours=36)

        self.assertEqual(
            inactive_email_policy(self.tenant), {"enabled": False, "inactive_after_hours": 36}
        )

    def test_stored_zero_hours_falls_back_to_the_default(self):
        self._stored(inactive_emails_enabled=True, inactive_login_hours=72)
        TenantSettings.objects.filter(tenant=self.tenant).update(inactive_login_hours=0)

        self.assertEqual(
            inactive_email_policy(self.tenant)["inactive_after_hours"], DEFAULT_INACTIVE_LOGIN_HOURS
        )

    def test_stored_hours_above_the_maximum_are_capped(self):
        self._stored(inactive_emails_enabled=True, inactive_login_hours=72)
        TenantSettings.objects.filter(tenant=self.tenant).update(inactive_login_hours=99999)

        self.assertEqual(
            inactive_email_policy(self.tenant)["inactive_after_hours"], MAX_INACTIVE_LOGIN_HOURS
        )

    def test_boundaries_are_kept_as_is(self):
        for hours in (1, MAX_INACTIVE_LOGIN_HOURS):
            with self.subTest(hours=hours):
                self._stored(inactive_emails_enabled=True, inactive_login_hours=hours)

                self.assertEqual(inactive_email_policy(self.tenant)["inactive_after_hours"], hours)

    def test_enabled_is_always_a_real_boolean(self):
        self._stored(inactive_emails_enabled=False, inactive_login_hours=72)

        self.assertIs(inactive_email_policy(self.tenant)["enabled"], False)

    def test_another_tenants_row_is_not_used(self):
        TenantSettings.objects.create(
            tenant=TenantFactory(), inactive_emails_enabled=False, inactive_login_hours=3
        )

        self.assertEqual(inactive_email_policy(self.tenant), DEFAULT_POLICY)


# ---------------------------------------------------------------------------
# user_settings.inactivity.save_inactive_email_policy
# ---------------------------------------------------------------------------


class SaveInactiveEmailPolicyFunctionTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()

    def _row(self):
        return TenantSettings.objects.get(tenant=self.tenant)

    def test_creates_the_row_when_missing(self):
        result = save_inactive_email_policy(self.tenant, enabled=False, inactive_after_hours=12)

        self.assertEqual(result, {"enabled": False, "inactive_after_hours": 12})
        self.assertFalse(self._row().inactive_emails_enabled)
        self.assertEqual(self._row().inactive_login_hours, 12)

    def test_only_enabled_leaves_hours_alone(self):
        TenantSettings.objects.create(tenant=self.tenant, inactive_emails_enabled=True, inactive_login_hours=40)

        save_inactive_email_policy(self.tenant, enabled=False)

        self.assertFalse(self._row().inactive_emails_enabled)
        self.assertEqual(self._row().inactive_login_hours, 40)

    def test_only_hours_leaves_enabled_alone(self):
        TenantSettings.objects.create(tenant=self.tenant, inactive_emails_enabled=False, inactive_login_hours=40)

        save_inactive_email_policy(self.tenant, inactive_after_hours=8)

        self.assertFalse(self._row().inactive_emails_enabled)
        self.assertEqual(self._row().inactive_login_hours, 8)

    def test_enabled_false_is_saved_not_treated_as_missing(self):
        TenantSettings.objects.create(tenant=self.tenant, inactive_emails_enabled=True, inactive_login_hours=72)

        save_inactive_email_policy(self.tenant, enabled=False)

        self.assertFalse(self._row().inactive_emails_enabled)

    def test_truthy_enabled_is_stored_as_true(self):
        save_inactive_email_policy(self.tenant, enabled=1)

        self.assertIs(self._row().inactive_emails_enabled, True)

    def test_hours_given_as_a_string_are_stored_as_an_int(self):
        result = save_inactive_email_policy(self.tenant, inactive_after_hours="24")

        self.assertEqual(self._row().inactive_login_hours, 24)
        self.assertEqual(result["inactive_after_hours"], 24)

    def test_nothing_to_change_keeps_existing_values(self):
        TenantSettings.objects.create(tenant=self.tenant, inactive_emails_enabled=False, inactive_login_hours=9)

        result = save_inactive_email_policy(self.tenant)

        self.assertEqual(result, {"enabled": False, "inactive_after_hours": 9})

    def test_nothing_to_change_on_a_new_tenant_gives_the_defaults(self):
        result = save_inactive_email_policy(self.tenant)

        self.assertEqual(result, DEFAULT_POLICY)

    def test_returns_the_same_shape_as_the_read_function(self):
        result = save_inactive_email_policy(self.tenant, enabled=True, inactive_after_hours=30)

        self.assertEqual(result, inactive_email_policy(self.tenant))

    def test_other_settings_on_the_row_are_not_touched(self):
        TenantSettings.objects.create(
            tenant=self.tenant,
            persistent_object_history=True,
            inactive_emails_enabled=True,
            inactive_login_hours=72,
        )

        save_inactive_email_policy(self.tenant, enabled=False, inactive_after_hours=5)

        self.assertTrue(self._row().persistent_object_history)
