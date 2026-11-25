import uuid
from datetime import datetime, timedelta, timezone as dt_timezone
from unittest.mock import MagicMock

import bcrypt
from django.test import TestCase

from authentication.models import OAuthIdentity, User
from authentication.supabase_import import fetch_supabase_rows, import_rows

PASSWORD = "Supabase-Pass-1"
SUPABASE_HASH = bcrypt.hashpw(PASSWORD.encode(), bcrypt.gensalt(rounds=4, prefix=b"2a")).decode()
CONFIRMED = datetime(2026, 1, 2, 3, 4, 5)


def _user_row(uid=None, email="person@example.com", encrypted_password=SUPABASE_HASH, **extra):
    return {
        "id": uid or str(uuid.uuid4()),
        "email": email,
        "encrypted_password": encrypted_password,
        "email_confirmed_at": CONFIRMED,
        "user_metadata": {"full_name": "Pat"},
        **extra,
    }


class SupabaseImportTests(TestCase):
    def test_new_user_keeps_id_password_and_confirmation(self):
        row = _user_row()

        report = import_rows([row], [])

        self.assertEqual(report.created, 1)
        user = User.objects.get(supabase_uid=row["id"])
        self.assertEqual(user.email, "person@example.com")
        self.assertEqual(user.email_verified_at, CONFIRMED)
        self.assertEqual(user.user_metadata, {"full_name": "Pat"})
        self.assertTrue(user.check_password(PASSWORD))

    def test_imported_user_can_log_in(self):
        import_rows([_user_row()], [])

        with self.settings(AUTH_JWT_SECRET="test-auth-jwt-secret"):
            response = self.client.post(
                "/auth/login/", {"email": "person@example.com", "password": PASSWORD}, content_type="application/json"
            )

        self.assertEqual(response.status_code, 200)

    def test_existing_mirrored_user_is_filled_in(self):
        uid = str(uuid.uuid4())
        User.objects.create_user(supabase_uid=uid, email="person@example.com")

        report = import_rows([_user_row(uid=uid)], [])

        self.assertEqual((report.created, report.updated, report.passwords_set), (0, 1, 1))
        self.assertTrue(User.objects.get(supabase_uid=uid).check_password(PASSWORD))

    def test_rerun_is_a_no_op(self):
        rows = [_user_row()]
        import_rows(rows, [])

        report = import_rows(rows, [])

        self.assertEqual((report.created, report.updated, report.unchanged), (0, 0, 1))

    def test_django_password_kept_unless_overwrite(self):
        uid = str(uuid.uuid4())
        User.objects.create_user(supabase_uid=uid, email="person@example.com", password="Django-Pass-2")

        import_rows([_user_row(uid=uid)], [])
        self.assertTrue(User.objects.get(supabase_uid=uid).check_password("Django-Pass-2"))

        import_rows([_user_row(uid=uid)], [], overwrite_passwords=True)
        self.assertTrue(User.objects.get(supabase_uid=uid).check_password(PASSWORD))

    def test_oauth_only_user_has_no_password(self):
        row = _user_row(encrypted_password="")

        import_rows([row], [])

        self.assertFalse(User.objects.get(supabase_uid=row["id"]).has_usable_password())

    def test_unconfirmed_user_stays_unconfirmed(self):
        row = _user_row(email_confirmed_at=None)
        import_rows([row], [])
        self.assertIsNone(User.objects.get(supabase_uid=row["id"]).email_verified_at)

    def test_identities_are_linked_once(self):
        row = _user_row()
        identities = [
            {"user_id": row["id"], "provider": "google", "provider_id": "g-1", "email": "person@example.com"},
            {"user_id": row["id"], "provider": "custom:zoho", "provider_id": "z-1", "email": "person@example.com"},
            {"user_id": row["id"], "provider": "github", "provider_id": "gh-1", "email": ""},
        ]

        report = import_rows([row], identities)
        again = import_rows([row], identities)

        self.assertEqual(report.identities_linked, 2)
        self.assertEqual(again.identities_linked, 0)
        self.assertEqual(
            set(OAuthIdentity.objects.values_list("provider", "subject")), {("google", "g-1"), ("zoho", "z-1")}
        )
        self.assertEqual(len(report.skipped), 1)

    def test_fetch_converts_timestamps_to_naive_utc(self):
        aware = datetime(2026, 1, 2, 8, 34, 5, tzinfo=dt_timezone(timedelta(hours=5, minutes=30)))
        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [("uid-1", "a@example.com", SUPABASE_HASH, aware, {})],
            [("uid-1", "google", "g-1", "a@example.com")],
        ]

        users, identities = fetch_supabase_rows(cursor)

        self.assertEqual(users[0]["email_confirmed_at"], CONFIRMED)
        self.assertEqual(identities[0]["provider_id"], "g-1")
