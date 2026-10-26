"""
Emails with no sign-in for the configured number of consecutive days (default 72)
are marked inactive. Sign-in time is auth.users.last_sign_in_at, shown on the
settings user list.
"""
import uuid
from datetime import timedelta

from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from accounts.models import SupabaseAuthUser
from authentication.models import User
from authz.models import TenantMembership
from authz.service import get_effective_permissions
from background_jobs.job_handlers import DeactivateInactiveEmailsJobHandler, get_handler_registry
from background_jobs.models import JobType
from core.models import TenantSettings
from tests.base.test_setup import BaseAPITestCase
from tests.factories import (
    BackgroundJobFactory,
    SupabaseAuthUserFactory,
    TenantFactory,
    TenantMembershipFactory,
    UserFactory,
)
from user_settings.inactivity import deactivate_inactive_emails


def _auth_user(membership, *, sign_in_days=None, created_days=None, clear_sign_in=False):
    uid = membership.user_id or uuid.uuid4()
    SupabaseAuthUserFactory(id=uid, email=membership.email)
    fields = {}
    if sign_in_days is not None:
        fields["last_sign_in_at"] = timezone.now() - timedelta(days=sign_in_days)
    if clear_sign_in:
        fields["last_sign_in_at"] = None
    if created_days is not None:
        fields["created_at"] = timezone.now() - timedelta(days=created_days)
    if fields:
        SupabaseAuthUser.objects.filter(id=uid).update(**fields)
    return uid


class InactiveEmailPolicyAPITests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.client.force_authenticate(user=self.user)
        self.url = reverse("inactive-email-policy")

    def test_get_returns_72_day_default_without_creating_a_row(self):
        response = self.client.get(self.url, **self.auth_headers)

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["enabled"], True)
        self.assertEqual(response.data["inactive_after_days"], 72)
        self.assertFalse(TenantSettings.objects.filter(tenant=self.tenant).exists())

    def test_patch_updates_the_rule_the_settings_page_saves(self):
        response = self.client.patch(
            self.url,
            {"enabled": False, "inactive_after_days": 30},
            format="json",
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["enabled"], False)
        self.assertEqual(response.data["inactive_after_days"], 30)
        row = TenantSettings.objects.get(tenant=self.tenant)
        self.assertFalse(row.inactive_emails_enabled)
        self.assertEqual(row.inactive_login_days, 30)

    def test_patch_days_leaves_enabled_unchanged(self):
        TenantSettings.objects.create(
            tenant=self.tenant,
            inactive_emails_enabled=False,
            inactive_login_days=72,
        )

        response = self.client.patch(
            self.url,
            {"inactive_after_days": 10},
            format="json",
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["enabled"], False)
        self.assertEqual(response.data["inactive_after_days"], 10)

    def test_patch_rejects_a_non_positive_day_count(self):
        response = self.client.patch(
            self.url,
            {"inactive_after_days": 0},
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
            {"inactive_after_days": 15},
            format="json",
            **self.auth_headers,
        )

        self.client.force_authenticate(user=user_b)
        response = self.client.get(self.url, **headers_b)

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["inactive_after_days"], 72)

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


class DeactivateInactiveEmailsTests(TestCase):
    def setUp(self):
        super().setUp()
        self.tenant = TenantFactory()
        self.handler = DeactivateInactiveEmailsJobHandler()

    def _job(self):
        return BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.DEACTIVATE_INACTIVE_EMAILS,
            payload={},
        )

    def test_handler_is_registered(self):
        self.assertTrue(get_handler_registry().has_handler(JobType.DEACTIVATE_INACTIVE_EMAILS))

    def test_72_days_without_a_sign_in_marks_the_email_inactive(self):
        stale = TenantMembershipFactory(tenant=self.tenant, is_active=True, email="stale@example.com")
        fresh = TenantMembershipFactory(tenant=self.tenant, is_active=True, email="fresh@example.com")
        _auth_user(stale, sign_in_days=72)
        _auth_user(fresh, sign_in_days=71)

        count = deactivate_inactive_emails(tenant=self.tenant)

        stale.refresh_from_db()
        fresh.refresh_from_db()
        self.assertEqual(count, 1)
        self.assertFalse(stale.is_active)
        self.assertTrue(fresh.is_active)

    def test_email_match_when_membership_has_no_user_id(self):
        membership = TenantMembershipFactory(
            tenant=self.tenant,
            is_active=True,
            email="person@example.com",
            user_id=None,
        )
        _auth_user(membership, sign_in_days=80)

        deactivate_inactive_emails(tenant=self.tenant)

        membership.refresh_from_db()
        self.assertFalse(membership.is_active)

    def test_never_signed_in_email_uses_auth_user_created_at(self):
        aged = TenantMembershipFactory(tenant=self.tenant, is_active=True, email="new-old@example.com")
        recent = TenantMembershipFactory(tenant=self.tenant, is_active=True, email="brand-new@example.com")
        _auth_user(aged, clear_sign_in=True, created_days=72)
        _auth_user(recent, clear_sign_in=True, created_days=1)

        deactivate_inactive_emails(tenant=self.tenant)

        aged.refresh_from_db()
        recent.refresh_from_db()
        self.assertFalse(aged.is_active)
        self.assertTrue(recent.is_active)

    def test_membership_without_an_auth_user_uses_created_at(self):
        aged = TenantMembershipFactory(tenant=self.tenant, is_active=True, email="no-auth@example.com")
        TenantMembership.objects.filter(pk=aged.pk).update(
            created_at=timezone.now() - timedelta(days=72)
        )

        deactivate_inactive_emails(tenant=self.tenant)

        aged.refresh_from_db()
        self.assertFalse(aged.is_active)

    def test_settings_page_day_count_is_what_the_job_uses(self):
        TenantSettings.objects.create(
            tenant=self.tenant,
            inactive_emails_enabled=True,
            inactive_login_days=10,
        )
        membership = TenantMembershipFactory(tenant=self.tenant, is_active=True)
        _auth_user(membership, sign_in_days=10)
        job = self._job()

        self.assertTrue(self.handler.process(job))

        membership.refresh_from_db()
        self.assertFalse(membership.is_active)
        self.assertEqual(job.result, {"deactivated": 1})

        kept = TenantMembershipFactory(tenant=self.tenant, is_active=True, email="kept@example.com")
        _auth_user(kept, sign_in_days=9)
        self.handler.process(self._job())
        kept.refresh_from_db()
        self.assertTrue(kept.is_active)

    def test_disabled_policy_leaves_stale_emails_active(self):
        TenantSettings.objects.create(
            tenant=self.tenant,
            inactive_emails_enabled=False,
            inactive_login_days=72,
        )
        membership = TenantMembershipFactory(tenant=self.tenant, is_active=True)
        _auth_user(membership, sign_in_days=200)

        count = deactivate_inactive_emails(tenant=self.tenant)

        membership.refresh_from_db()
        self.assertEqual(count, 0)
        self.assertTrue(membership.is_active)

    def test_other_tenants_are_left_alone(self):
        other = TenantFactory()
        membership = TenantMembershipFactory(tenant=other, is_active=True)
        _auth_user(membership, sign_in_days=200)

        deactivate_inactive_emails(tenant=self.tenant)

        membership.refresh_from_db()
        self.assertTrue(membership.is_active)

    def test_deactivation_drops_the_permissions_cache(self):
        membership = TenantMembershipFactory(tenant=self.tenant, is_active=True)
        _auth_user(membership, sign_in_days=80)
        self.assertIsNotNone(get_effective_permissions(str(membership.user_id), self.tenant)["role_key"])

        deactivate_inactive_emails(tenant=self.tenant)

        self.assertIsNone(get_effective_permissions(str(membership.user_id), self.tenant)["role_key"])

    def test_turning_an_email_back_on_does_not_change_last_sign_in_at(self):
        membership = TenantMembershipFactory(tenant=self.tenant, is_active=True)
        uid = _auth_user(membership, sign_in_days=100)
        signed_in = SupabaseAuthUser.objects.get(id=uid).last_sign_in_at

        deactivate_inactive_emails(tenant=self.tenant)
        membership.refresh_from_db()
        membership.is_active = True
        membership.save(update_fields=["is_active"])

        self.assertEqual(SupabaseAuthUser.objects.get(id=uid).last_sign_in_at, signed_in)
        self.assertEqual(deactivate_inactive_emails(tenant=self.tenant), 1)
        membership.refresh_from_db()
        self.assertFalse(membership.is_active)


@override_settings(AUTH_PROVIDER="django")
class DjangoAuthInactiveEmailTests(TestCase):
    """Testing and preprod record login on authentication.User.last_login."""

    def setUp(self):
        super().setUp()
        self.tenant = TenantFactory()

    def _login_user(self, membership, *, days):
        user = UserFactory(supabase_uid=str(membership.user_id), email=membership.email)
        User.objects.filter(pk=user.pk).update(last_login=timezone.now() - timedelta(days=days))
        return user

    def test_stale_last_login_marks_the_email_inactive(self):
        stale = TenantMembershipFactory(tenant=self.tenant, is_active=True, email="stale-django@example.com")
        fresh = TenantMembershipFactory(tenant=self.tenant, is_active=True, email="fresh-django@example.com")
        self._login_user(stale, days=72)
        self._login_user(fresh, days=2)
        # A fresh Supabase stamp must not keep the stale Django login active.
        SupabaseAuthUserFactory(id=stale.user_id, email=stale.email)
        SupabaseAuthUser.objects.filter(id=stale.user_id).update(
            last_sign_in_at=timezone.now() - timedelta(days=1)
        )

        deactivate_inactive_emails(tenant=self.tenant)

        stale.refresh_from_db()
        fresh.refresh_from_db()
        self.assertFalse(stale.is_active)
        self.assertTrue(fresh.is_active)

    def test_recent_last_login_ignores_a_stale_supabase_stamp(self):
        membership = TenantMembershipFactory(tenant=self.tenant, is_active=True, email="recent-django@example.com")
        self._login_user(membership, days=3)
        SupabaseAuthUserFactory(id=membership.user_id, email=membership.email)
        SupabaseAuthUser.objects.filter(id=membership.user_id).update(
            last_sign_in_at=timezone.now() - timedelta(days=200)
        )

        count = deactivate_inactive_emails(tenant=self.tenant)

        membership.refresh_from_db()
        self.assertEqual(count, 0)
        self.assertTrue(membership.is_active)
