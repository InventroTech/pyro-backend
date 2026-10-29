"""One account per email address (case-insensitive), including under concurrent sign-ups."""
import importlib
import uuid
from io import StringIO
from unittest.mock import patch

from django.apps import apps as django_apps
from django.core.cache import cache
from django.core.management import call_command
from django.db import IntegrityError, connection, transaction
from django.db.models import QuerySet
from django.test import TestCase, override_settings
from rest_framework.test import APIClient

from authentication.models import OAuthIdentity, User
from authentication.oauth import Profile, resolve_user
from authentication.supabase_import import import_rows
from config.supabase_auth import _get_or_create_profile

EMAIL = "shared@example.com"


def _lose_race_to(rival_email):
    """
    Another request saves an account with this email right after our existence check:
    the rival row exists, but the first User lookup does not see it.
    """
    User.objects.create(supabase_uid=str(uuid.uuid4()), email=rival_email)
    real_first = QuerySet.first
    missed = []

    def first(self):
        if self.model is User and not missed:
            missed.append(True)
            return None
        return real_first(self)

    return patch.object(QuerySet, "first", first)


class EmailConstraintTests(TestCase):
    def test_same_email_in_any_case_is_rejected(self):
        User.objects.create_user(supabase_uid=str(uuid.uuid4()), email=EMAIL)
        with self.assertRaises(IntegrityError), transaction.atomic():
            User.objects.create(supabase_uid=str(uuid.uuid4()), email="Shared@Example.com")

    def test_users_without_email_are_allowed(self):
        User.objects.create(supabase_uid=str(uuid.uuid4()), email=None)
        User.objects.create(supabase_uid=str(uuid.uuid4()), email=None)
        User.objects.create(supabase_uid=str(uuid.uuid4()), email="")
        User.objects.create(supabase_uid=str(uuid.uuid4()), email="")
        self.assertEqual(User.objects.count(), 4)

    def test_duplicate_report_command_is_clean(self):
        User.objects.create_user(supabase_uid=str(uuid.uuid4()), email=EMAIL)
        out = StringIO()
        call_command("find_duplicate_user_emails", stdout=out)
        self.assertIn("No duplicate emails", out.getvalue())

    def test_existing_duplicates_stop_the_migration_and_are_reported(self):
        constraint = next(c for c in User._meta.constraints if c.name == "auth_user_email_ci_unique")
        with connection.schema_editor() as editor:
            editor.remove_constraint(User, constraint)
        first = User.objects.create(supabase_uid=str(uuid.uuid4()), email=EMAIL)
        User.objects.create(supabase_uid=str(uuid.uuid4()), email=EMAIL.upper())

        migration = importlib.import_module("authentication.migrations.0007_user_email_unique")
        with self.assertRaisesMessage(RuntimeError, f"{EMAIL} (2 accounts)"):
            migration.refuse_duplicate_emails(django_apps, None)

        out = StringIO()
        call_command("find_duplicate_user_emails", stdout=out)
        self.assertIn(EMAIL, out.getvalue())
        self.assertIn(f"uid={first.supabase_uid}", out.getvalue())


@override_settings(AUTH_PROVIDER="django", AUTH_JWT_SECRET="test-secret")
class ConcurrentAccountCreationTests(TestCase):
    def setUp(self):
        cache.clear()

    @patch("authentication.account_views.send_confirmation_email", return_value=True)
    def test_concurrent_signup_keeps_one_account(self, _):
        with _lose_race_to(EMAIL.upper()):
            response = APIClient().post(
                "/auth/signup/", {"email": EMAIL, "password": "Pass-Word-77"}, format="json"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(User.objects.filter(email__iexact=EMAIL).count(), 1)

    def test_concurrent_oauth_login_links_the_existing_account(self):
        profile = Profile(subject="google-sub-1", email=EMAIL, email_verified=True, metadata={})
        with _lose_race_to(EMAIL.upper()):
            user = resolve_user("google", profile)

        self.assertEqual(User.objects.filter(email__iexact=EMAIL).count(), 1)
        self.assertEqual(OAuthIdentity.objects.get(subject="google-sub-1").user, user)


class EmailOwnedByAnotherUserTests(TestCase):
    def setUp(self):
        self.owner = User.objects.create_user(supabase_uid=str(uuid.uuid4()), email=EMAIL)

    def test_import_skips_supabase_user_whose_email_is_taken(self):
        other_uid = str(uuid.uuid4())
        report = import_rows(
            [
                {
                    "id": other_uid,
                    "email": EMAIL,
                    "encrypted_password": None,
                    "email_confirmed_at": None,
                    "user_metadata": {},
                }
            ],
            [],
        )

        self.assertFalse(User.objects.filter(supabase_uid=other_uid).exists())
        self.assertEqual(report.created, 0)
        self.assertIn(EMAIL, report.skipped[0])

    @override_settings(AUTH_PROVIDER="supabase")
    def test_supabase_mode_mirror_does_not_copy_a_taken_email(self):
        other_uid = str(uuid.uuid4())
        user = _get_or_create_profile({"sub": other_uid, "email": EMAIL, "iss": "supabase"})

        self.assertEqual(user.supabase_uid, other_uid)
        self.assertIsNone(user.email)
        self.owner.refresh_from_db()
        self.assertEqual(self.owner.email, EMAIL)
