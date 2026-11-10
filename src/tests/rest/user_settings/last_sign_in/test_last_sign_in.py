"""
Last sign-in on the settings user list comes from authentication.User.last_login.

The token check (config.supabase_auth) copies the Supabase sign-in time from the
token's amr claim onto last_login; user_settings.inactivity reads it back for
/membership/users/. auth.users.last_sign_in_at is no longer used.
"""
import time
import uuid
from datetime import datetime, timedelta, timezone as dt_timezone
from unittest.mock import patch

import jwt
from django.conf import settings
from django.db import DatabaseError, connection
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from rest_framework import exceptions

from accounts.models import SupabaseAuthUser
from authentication.models import User
from config.supabase_auth import (
    SIGN_IN_MAX_CLOCK_SKEW_SECONDS,
    SupabaseJWTAuthentication,
    _get_or_create_profile,
    _record_sign_in,
    _signed_in_at,
)
from tests.base.test_setup import BaseAPITestCase
from tests.factories import TenantFactory, TenantMembershipFactory, UserFactory
from user_settings.inactivity import (
    _django_users_for_memberships,
    last_sign_in_at_by_membership_id,
)

TEST_SECRET = "last-sign-in-test-secret"


def _ts(dt):
    return int(dt.timestamp())


def _ago(**kwargs):
    return timezone.now() - timedelta(**kwargs)


def _django_dt(aware):
    """Shape an aware UTC datetime the way Django stores it (naive when USE_TZ=False)."""
    return aware if settings.USE_TZ else timezone.make_naive(aware)


def _amr(*timestamps, method="password"):
    return [{"method": method, "timestamp": ts} for ts in timestamps]


def _token(claims, secret=TEST_SECRET):
    token = jwt.encode({"aud": "authenticated", **claims}, secret, algorithm="HS256")
    return token.decode("utf-8") if isinstance(token, bytes) else token


def _separate_user(email, last_login=None):
    """UserFactory reuses users by email; duplicate-email cases need a real second row."""
    user = User.objects.create(supabase_uid=str(uuid.uuid4()), email=email)
    if last_login is not None:
        User.objects.filter(pk=user.pk).update(last_login=last_login)
    return user


def _last_login_updates(queries):
    return [
        q["sql"]
        for q in queries
        if q["sql"].lstrip().upper().startswith("UPDATE") and "last_login" in q["sql"]
    ]


# ---------------------------------------------------------------------------
# config.supabase_auth._signed_in_at
# ---------------------------------------------------------------------------


class SignedInAtTests(SimpleTestCase):
    # --- happy path -------------------------------------------------------

    def test_reads_the_amr_timestamp_as_utc(self):
        result = _signed_in_at({"amr": _amr(1762758701)})

        self.assertEqual(result, _django_dt(datetime(2025, 11, 10, 7, 11, 41, tzinfo=dt_timezone.utc)))

    def test_result_matches_the_use_tz_setting_so_it_compares_with_db_values(self):
        result = _signed_in_at({"amr": _amr(1762758701)})

        self.assertEqual(timezone.is_aware(result), settings.USE_TZ)

    @override_settings(USE_TZ=False, TIME_ZONE="UTC")
    def test_naive_utc_when_use_tz_is_off(self):
        result = _signed_in_at({"amr": _amr(1762758701)})

        self.assertTrue(timezone.is_naive(result))
        self.assertEqual(result, datetime(2025, 11, 10, 7, 11, 41))

    @override_settings(USE_TZ=True)
    def test_aware_utc_when_use_tz_is_on(self):
        result = _signed_in_at({"amr": _amr(1762758701)})

        self.assertEqual(result, datetime(2025, 11, 10, 7, 11, 41, tzinfo=dt_timezone.utc))

    def test_result_can_be_compared_with_timezone_now(self):
        result = _signed_in_at({"amr": _amr(int(time.time()) - 60)})

        self.assertLess(result, timezone.now())

    def test_latest_of_several_amr_entries_wins(self):
        result = _signed_in_at({"amr": _amr(1000, 3000, 2000)})

        self.assertEqual(_ts(result), 3000)

    def test_accepts_a_float_timestamp(self):
        result = _signed_in_at({"amr": _amr(1762758701.9)})

        self.assertEqual(_ts(result), 1762758701)

    def test_accepts_amr_as_a_tuple(self):
        result = _signed_in_at({"amr": tuple(_amr(1762758701))})

        self.assertEqual(_ts(result), 1762758701)

    def test_does_not_change_the_claims(self):
        claims = {"amr": _amr(2000, 1000)}

        _signed_in_at(claims)

        self.assertEqual(claims, {"amr": _amr(2000, 1000)})

    # --- missing / empty --------------------------------------------------

    def test_no_amr_claim_returns_none(self):
        self.assertIsNone(_signed_in_at({"sub": "x"}))

    def test_null_or_empty_amr_returns_none(self):
        self.assertIsNone(_signed_in_at({"amr": None}))
        self.assertIsNone(_signed_in_at({"amr": []}))

    # --- wrong shape (must not crash the token check) ---------------------

    def test_amr_as_a_number_returns_none(self):
        self.assertIsNone(_signed_in_at({"amr": 1762758701}))

    def test_amr_as_a_string_returns_none(self):
        self.assertIsNone(_signed_in_at({"amr": "password"}))

    def test_amr_as_a_dict_returns_none(self):
        self.assertIsNone(_signed_in_at({"amr": {"method": "password", "timestamp": 1762758701}}))

    def test_plain_string_amr_entries_are_ignored(self):
        self.assertIsNone(_signed_in_at({"amr": ["pwd", "otp"]}))

    def test_entries_without_a_numeric_timestamp_are_ignored(self):
        claims = {
            "amr": [
                {"method": "otp"},
                {"method": "x", "timestamp": "1762758701"},
                {"method": "y", "timestamp": None},
                {"method": "z", "timestamp": True},
                {"method": "w", "timestamp": [1762758701]},
                None,
                42,
            ]
        }

        self.assertIsNone(_signed_in_at(claims))

    def test_good_entry_is_used_when_mixed_with_bad_ones(self):
        claims = {"amr": ["pwd", {"method": "otp"}, {"method": "password", "timestamp": 5000}]}

        self.assertEqual(_ts(_signed_in_at(claims)), 5000)

    # --- out-of-range values ----------------------------------------------

    def test_zero_and_negative_timestamps_are_ignored(self):
        self.assertIsNone(_signed_in_at({"amr": _amr(0)}))
        self.assertIsNone(_signed_in_at({"amr": _amr(-1762758701)}))

    def test_far_future_timestamp_is_ignored(self):
        tomorrow = time.time() + 86400

        self.assertIsNone(_signed_in_at({"amr": _amr(tomorrow)}))

    def test_small_clock_skew_is_accepted(self):
        slightly_ahead = int(time.time()) + 60

        self.assertEqual(_ts(_signed_in_at({"amr": _amr(slightly_ahead)})), slightly_ahead)

    def test_just_past_the_clock_skew_limit_is_ignored(self):
        too_far = time.time() + SIGN_IN_MAX_CLOCK_SKEW_SECONDS + 60

        self.assertIsNone(_signed_in_at({"amr": _amr(too_far)}))

    def test_millisecond_timestamp_is_ignored_not_a_crash(self):
        self.assertIsNone(_signed_in_at({"amr": _amr(1762758701000)}))

    def test_millisecond_entry_does_not_hide_a_valid_one(self):
        result = _signed_in_at({"amr": _amr(1762758701000, 1762758701)})

        self.assertEqual(_ts(result), 1762758701)

    def test_huge_infinite_and_nan_timestamps_do_not_crash(self):
        for value in (10**20, float("inf"), float("-inf"), float("nan")):
            with self.subTest(value=value):
                self.assertIsNone(_signed_in_at({"amr": _amr(value)}))


# ---------------------------------------------------------------------------
# config.supabase_auth._record_sign_in
# ---------------------------------------------------------------------------


class RecordSignInTests(TestCase):
    def setUp(self):
        self.user = UserFactory(supabase_uid=str(uuid.uuid4()), email="record@example.com")

    def _db_last_login(self):
        return User.objects.get(pk=self.user.pk).last_login

    def test_sets_last_login_when_empty(self):
        signed_in = _ago(hours=1)

        _record_sign_in(self.user, signed_in)

        self.assertEqual(self.user.last_login, signed_in)
        self.assertEqual(self._db_last_login(), signed_in)

    def test_moves_last_login_forward(self):
        _record_sign_in(self.user, _ago(days=2))
        newer = _ago(minutes=1)

        _record_sign_in(self.user, newer)

        self.assertEqual(self._db_last_login(), newer)

    def test_never_moves_last_login_back(self):
        newer = _ago(minutes=1)
        _record_sign_in(self.user, newer)

        _record_sign_in(self.user, _ago(days=2))

        self.assertEqual(self.user.last_login, newer)
        self.assertEqual(self._db_last_login(), newer)

    def test_no_query_when_already_up_to_date(self):
        signed_in = _ago(minutes=1)
        _record_sign_in(self.user, signed_in)

        with self.assertNumQueries(0):
            _record_sign_in(self.user, signed_in)

    def test_stale_copy_does_not_overwrite_a_newer_value_in_the_db(self):
        newer = _ago(minutes=1)
        User.objects.filter(pk=self.user.pk).update(last_login=newer)
        stale = User.objects.get(pk=self.user.pk)
        stale.last_login = None

        _record_sign_in(stale, _ago(days=3))

        self.assertEqual(self._db_last_login(), newer)
        self.assertIsNone(stale.last_login)

    def test_only_touches_this_user(self):
        other = UserFactory(supabase_uid=str(uuid.uuid4()), email="bystander@example.com")

        _record_sign_in(self.user, _ago(minutes=1))

        other.refresh_from_db()
        self.assertIsNone(other.last_login)

    def test_database_error_is_logged_not_raised(self):
        with patch.object(User.objects, "filter", side_effect=DatabaseError("db down")):
            with self.assertLogs("config.supabase_auth", level="WARNING") as logs:
                _record_sign_in(self.user, _ago(minutes=1))

        self.assertIsNone(self.user.last_login)
        self.assertIn("Could not record sign-in", logs.output[0])

    def test_database_error_leaves_the_db_unchanged(self):
        with patch.object(User.objects, "filter", side_effect=DatabaseError("db down")):
            with self.assertLogs("config.supabase_auth", level="WARNING"):
                _record_sign_in(self.user, _ago(minutes=1))

        self.assertIsNone(self._db_last_login())


# ---------------------------------------------------------------------------
# config.supabase_auth._get_or_create_profile
# ---------------------------------------------------------------------------


class GetOrCreateProfileTests(TestCase):
    def setUp(self):
        self.uid = str(uuid.uuid4())
        self.email = "signin@example.com"

    def _claims(self, *amr_timestamps, **extra):
        claims = {"sub": self.uid, "email": self.email, **extra}
        if amr_timestamps:
            claims["amr"] = _amr(*amr_timestamps)
        return claims

    def _last_login(self):
        return User.objects.get(supabase_uid=self.uid).last_login

    def test_new_user_gets_last_login_from_the_token(self):
        signed_in = _ts(_ago(hours=1))

        user = _get_or_create_profile(self._claims(signed_in))

        self.assertEqual(_ts(user.last_login), signed_in)
        self.assertEqual(_ts(self._last_login()), signed_in)

    def test_new_user_without_amr_has_no_last_login(self):
        user = _get_or_create_profile(self._claims())

        self.assertIsNone(user.last_login)
        self.assertIsNone(self._last_login())

    def test_newer_sign_in_moves_last_login_forward(self):
        older = _ts(_ago(days=3))
        newer = _ts(_ago(minutes=5))
        _get_or_create_profile(self._claims(older))

        user = _get_or_create_profile(self._claims(newer))

        self.assertEqual(_ts(user.last_login), newer)
        self.assertEqual(_ts(self._last_login()), newer)

    def test_older_token_does_not_move_last_login_back(self):
        newer = _ts(_ago(minutes=5))
        older = _ts(_ago(days=3))
        _get_or_create_profile(self._claims(newer))

        _get_or_create_profile(self._claims(older))

        self.assertEqual(_ts(self._last_login()), newer)

    def test_stored_last_login_read_from_the_db_compares_without_error(self):
        UserFactory(supabase_uid=self.uid, email=self.email)
        User.objects.filter(supabase_uid=self.uid).update(last_login=_ago(days=1))
        newer = _ts(_ago(minutes=1))

        user = _get_or_create_profile(self._claims(newer))

        self.assertEqual(_ts(user.last_login), newer)
        self.assertEqual(_ts(self._last_login()), newer)

    def test_many_requests_in_a_row_keep_working(self):
        signed_in = _ts(_ago(minutes=2))

        for _ in range(3):
            _get_or_create_profile(self._claims(signed_in))

        self.assertEqual(_ts(self._last_login()), signed_in)

    def test_same_sign_in_does_not_write_again(self):
        signed_in = _ts(_ago(minutes=5))
        _get_or_create_profile(self._claims(signed_in))

        with CaptureQueriesContext(connection) as ctx:
            _get_or_create_profile(self._claims(signed_in))

        self.assertEqual(_last_login_updates(ctx.captured_queries), [])

    def test_token_without_amr_keeps_existing_last_login(self):
        signed_in = _ago(days=2)
        UserFactory(supabase_uid=self.uid, email=self.email)
        User.objects.filter(supabase_uid=self.uid).update(last_login=signed_in)

        with CaptureQueriesContext(connection) as ctx:
            _get_or_create_profile(self._claims())

        self.assertEqual(_last_login_updates(ctx.captured_queries), [])
        self.assertAlmostEqual(self._last_login().timestamp(), signed_in.timestamp(), delta=1)

    def test_malformed_amr_still_returns_the_user(self):
        user = _get_or_create_profile({**self._claims(), "amr": 12345})

        self.assertEqual(user.supabase_uid, self.uid)
        self.assertIsNone(self._last_login())

    def test_far_future_sign_in_is_not_saved(self):
        tomorrow = int(time.time()) + 86400

        _get_or_create_profile(self._claims(tomorrow))

        self.assertIsNone(self._last_login())

    def test_far_future_token_does_not_block_later_real_sign_ins(self):
        _get_or_create_profile(self._claims(int(time.time()) + 86400))
        real = _ts(_ago(minutes=1))

        _get_or_create_profile(self._claims(real))

        self.assertEqual(_ts(self._last_login()), real)

    def test_email_change_and_sign_in_are_both_saved(self):
        UserFactory(supabase_uid=self.uid, email="old@example.com")
        signed_in = _ts(_ago(minutes=1))

        _get_or_create_profile(self._claims(signed_in))

        row = User.objects.get(supabase_uid=self.uid)
        self.assertEqual(row.email, self.email)
        self.assertEqual(_ts(row.last_login), signed_in)

    def test_uppercase_email_claim_is_lowercased_and_sign_in_saved(self):
        signed_in = _ts(_ago(minutes=1))

        _get_or_create_profile({**self._claims(signed_in), "email": "SignIn@Example.COM"})

        row = User.objects.get(supabase_uid=self.uid)
        self.assertEqual(row.email, "signin@example.com")
        self.assertEqual(_ts(row.last_login), signed_in)

    def test_user_id_claim_is_used_when_sub_is_missing(self):
        signed_in = _ts(_ago(minutes=1))
        claims = {"user_id": self.uid, "email": self.email, "amr": _amr(signed_in)}

        user = _get_or_create_profile(claims)

        self.assertEqual(user.supabase_uid, self.uid)
        self.assertEqual(_ts(self._last_login()), signed_in)

    def test_only_the_signed_in_user_is_updated(self):
        other = UserFactory(supabase_uid=str(uuid.uuid4()), email="other@example.com")

        _get_or_create_profile(self._claims(_ts(timezone.now())))

        other.refresh_from_db()
        self.assertIsNone(other.last_login)

    def test_database_error_while_recording_still_returns_the_user(self):
        UserFactory(supabase_uid=self.uid, email=self.email)

        with patch.object(User.objects, "filter", side_effect=DatabaseError("db down")):
            with self.assertLogs("config.supabase_auth", level="WARNING"):
                user = _get_or_create_profile(self._claims(_ts(_ago(minutes=1))))

        self.assertEqual(user.supabase_uid, self.uid)
        self.assertIsNone(self._last_login())

    def test_token_without_sub_is_rejected(self):
        with self.assertRaises(exceptions.AuthenticationFailed):
            _get_or_create_profile({"email": self.email, "amr": _amr(_ts(timezone.now()))})

        self.assertFalse(User.objects.filter(email=self.email).exists())


# ---------------------------------------------------------------------------
# config.supabase_auth.SupabaseJWTAuthentication.authenticate
# ---------------------------------------------------------------------------


class SupabaseJWTAuthenticationTests(TestCase):
    def setUp(self):
        patcher = patch("config.supabase_auth.SUPABASE_JWT_SECRET", TEST_SECRET)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.uid = str(uuid.uuid4())
        self.email = "jwt-signin@example.com"
        self.auth = SupabaseJWTAuthentication()

    def _request(self, token=None, scheme="Bearer"):
        headers = {"HTTP_AUTHORIZATION": f"{scheme} {token}"} if token else {}
        return RequestFactory().get("/", **headers)

    def _claims(self, *amr_timestamps, **extra):
        claims = {"sub": self.uid, "email": self.email, **extra}
        if amr_timestamps:
            claims["amr"] = _amr(*amr_timestamps)
        return claims

    def _last_login(self):
        return User.objects.get(supabase_uid=self.uid).last_login

    def test_real_token_records_the_sign_in(self):
        signed_in = _ts(_ago(minutes=30))

        user, _ = self.auth.authenticate(self._request(_token(self._claims(signed_in))))

        self.assertEqual(user.supabase_uid, self.uid)
        self.assertEqual(_ts(self._last_login()), signed_in)

    def test_sets_claims_and_token_on_the_request(self):
        token = _token(self._claims(_ts(_ago(minutes=1))))
        request = self._request(token)

        self.auth.authenticate(request)

        self.assertEqual(request.jwt_claims["sub"], self.uid)
        self.assertEqual(request.token, token)

    def test_lowercase_bearer_scheme_works(self):
        signed_in = _ts(_ago(minutes=3))

        self.auth.authenticate(self._request(_token(self._claims(signed_in)), scheme="bearer"))

        self.assertEqual(_ts(self._last_login()), signed_in)

    def test_refreshed_token_with_same_sign_in_does_not_write_again(self):
        signed_in = _ts(_ago(hours=2))
        self.auth.authenticate(self._request(_token(self._claims(signed_in, iat=signed_in))))
        refreshed = _token(self._claims(signed_in, iat=_ts(timezone.now())))

        with CaptureQueriesContext(connection) as ctx:
            self.auth.authenticate(self._request(refreshed))

        self.assertEqual(_last_login_updates(ctx.captured_queries), [])
        self.assertEqual(_ts(self._last_login()), signed_in)

    def test_new_sign_in_after_logout_updates_last_login(self):
        self.auth.authenticate(self._request(_token(self._claims(_ts(_ago(days=2))))))
        again = _ts(_ago(minutes=1))

        self.auth.authenticate(self._request(_token(self._claims(again))))

        self.assertEqual(_ts(self._last_login()), again)

    def test_spoof_style_token_without_amr_does_not_record_a_sign_in(self):
        token = _token(self._claims(role="authenticated"))

        user, _ = self.auth.authenticate(self._request(token))

        self.assertIsNone(User.objects.get(pk=user.pk).last_login)

    def test_token_with_malformed_amr_still_authenticates(self):
        user, _ = self.auth.authenticate(self._request(_token(self._claims(amr="password"))))

        self.assertEqual(user.supabase_uid, self.uid)
        self.assertIsNone(self._last_login())

    def test_no_authorization_header_returns_none(self):
        self.assertIsNone(self.auth.authenticate(self._request()))
        self.assertFalse(User.objects.filter(supabase_uid=self.uid).exists())

    def test_non_bearer_scheme_returns_none(self):
        token = _token(self._claims(_ts(_ago(minutes=1))))

        self.assertIsNone(self.auth.authenticate(self._request(token, scheme="Basic")))
        self.assertFalse(User.objects.filter(supabase_uid=self.uid).exists())

    def test_wrong_secret_is_rejected_and_records_nothing(self):
        UserFactory(supabase_uid=self.uid, email=self.email)
        bad = _token(self._claims(_ts(timezone.now())), secret="wrong-secret")

        with self.assertRaises(exceptions.AuthenticationFailed):
            self.auth.authenticate(self._request(bad))

        self.assertIsNone(self._last_login())

    def test_expired_token_is_rejected_and_records_nothing(self):
        UserFactory(supabase_uid=self.uid, email=self.email)
        expired = _token(self._claims(_ts(_ago(hours=2)), exp=_ts(_ago(hours=1))))

        with self.assertRaises(exceptions.AuthenticationFailed):
            self.auth.authenticate(self._request(expired))

        self.assertIsNone(self._last_login())

    def test_garbage_token_is_rejected(self):
        with self.assertRaises(exceptions.AuthenticationFailed):
            self.auth.authenticate(self._request("not-a-jwt"))

        self.assertFalse(User.objects.filter(supabase_uid=self.uid).exists())


# ---------------------------------------------------------------------------
# user_settings.inactivity._django_users_for_memberships
# ---------------------------------------------------------------------------


class DjangoUsersForMembershipsTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()

    def _membership(self, **kwargs):
        return TenantMembershipFactory(tenant=self.tenant, **kwargs)

    def _user(self, email, last_login=None, uid=None):
        user = UserFactory(supabase_uid=uid or str(uuid.uuid4()), email=email)
        if last_login is not None:
            User.objects.filter(pk=user.pk).update(last_login=last_login)
        return user

    def test_empty_list_returns_empty_without_queries(self):
        with self.assertNumQueries(0):
            self.assertEqual(_django_users_for_memberships([]), {})

    def test_matches_by_supabase_uid(self):
        uid = str(uuid.uuid4())
        user = self._user("uid-match@example.com", uid=uid)
        membership = self._membership(user_id=uid, email="different@example.com")

        matched = _django_users_for_memberships([membership])

        self.assertEqual(matched[membership.id].pk, user.pk)

    def test_falls_back_to_email_ignoring_case(self):
        user = self._user("Sam.Case@Example.COM")
        membership = self._membership(user_id=str(uuid.uuid4()), email="sam.case@example.com")

        matched = _django_users_for_memberships([membership])

        self.assertEqual(matched[membership.id].pk, user.pk)

    def test_membership_without_user_id_matches_by_email(self):
        user = self._user("invited@example.com")
        membership = self._membership(user_id=None, email="invited@example.com")

        matched = _django_users_for_memberships([membership])

        self.assertEqual(matched[membership.id].pk, user.pk)

    def test_uid_match_wins_over_email_match(self):
        uid = str(uuid.uuid4())
        by_uid = self._user("uid-owner@example.com", uid=uid)
        self._user("shared@example.com", last_login=timezone.now())
        membership = self._membership(user_id=uid, email="shared@example.com")

        matched = _django_users_for_memberships([membership])

        self.assertEqual(matched[membership.id].pk, by_uid.pk)

    def test_duplicate_email_accounts_pick_the_most_recent_sign_in(self):
        _separate_user("dupe@example.com", last_login=_ago(days=10))
        recent = _separate_user("DUPE@example.com", last_login=_ago(minutes=5))
        _separate_user("Dupe@Example.com", last_login=_ago(days=1))
        membership = self._membership(user_id=None, email="dupe@example.com")

        matched = _django_users_for_memberships([membership])

        self.assertEqual(matched[membership.id].pk, recent.pk)

    def test_duplicate_email_prefers_an_account_that_has_signed_in(self):
        _separate_user("half@example.com")
        signed_in = _separate_user("half@example.com", last_login=_ago(days=30))
        _separate_user("half@example.com")
        membership = self._membership(user_id=None, email="half@example.com")

        matched = _django_users_for_memberships([membership])

        self.assertEqual(matched[membership.id].pk, signed_in.pk)

    def test_user_with_no_email_still_matches_by_uid(self):
        uid = str(uuid.uuid4())
        user = self._user("placeholder@example.com", uid=uid)
        User.objects.filter(pk=user.pk).update(email=None)
        membership = self._membership(user_id=uid, email="no-email-user@example.com")

        matched = _django_users_for_memberships([membership])

        self.assertEqual(matched[membership.id].pk, user.pk)

    def test_no_matching_user_returns_none(self):
        membership = self._membership(user_id=str(uuid.uuid4()), email="nobody@example.com")

        matched = _django_users_for_memberships([membership])

        self.assertIsNone(matched[membership.id])

    def test_every_membership_has_a_key_even_without_a_match(self):
        hit = self._user("hit@example.com")
        memberships = [
            self._membership(user_id=hit.supabase_uid, email="hit@example.com"),
            self._membership(user_id=str(uuid.uuid4()), email="miss@example.com"),
        ]

        matched = _django_users_for_memberships(memberships)

        self.assertEqual(set(matched), {m.id for m in memberships})

    def test_each_membership_gets_its_own_user(self):
        users = [self._user(f"many{i}@example.com") for i in range(3)]
        memberships = [self._membership(user_id=u.supabase_uid, email=u.email) for u in users]

        matched = _django_users_for_memberships(memberships)

        self.assertEqual(
            {m.id: matched[m.id].pk for m in memberships},
            {m.id: u.pk for m, u in zip(memberships, users)},
        )

    def test_uses_one_query_for_many_memberships(self):
        users = [self._user(f"q{i}@example.com") for i in range(5)]
        memberships = [self._membership(user_id=u.supabase_uid, email=u.email) for u in users]

        with self.assertNumQueries(1):
            _django_users_for_memberships(memberships)


# ---------------------------------------------------------------------------
# user_settings.inactivity.last_sign_in_at_by_membership_id
# ---------------------------------------------------------------------------


class LastSignInAtByMembershipIdTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()

    def _member_with_login(self, email, last_login, tenant=None):
        uid = str(uuid.uuid4())
        user = UserFactory(supabase_uid=uid, email=email)
        User.objects.filter(pk=user.pk).update(last_login=last_login)
        return TenantMembershipFactory(tenant=tenant or self.tenant, user_id=uid, email=email)

    def test_empty_list_returns_empty(self):
        self.assertEqual(last_sign_in_at_by_membership_id([]), {})

    def test_returns_last_login_for_each_membership(self):
        a_time = _ago(days=1)
        b_time = _ago(hours=2)
        a = self._member_with_login("a@example.com", a_time)
        b = self._member_with_login("b@example.com", b_time)

        result = last_sign_in_at_by_membership_id([a, b])

        self.assertAlmostEqual(result[a.id].timestamp(), a_time.timestamp(), delta=1)
        self.assertAlmostEqual(result[b.id].timestamp(), b_time.timestamp(), delta=1)

    def test_values_match_the_use_tz_setting(self):
        membership = self._member_with_login("aware@example.com", _ago(hours=1))

        value = last_sign_in_at_by_membership_id([membership])[membership.id]

        self.assertEqual(timezone.is_aware(value), settings.USE_TZ)

    def test_user_who_never_signed_in_is_none(self):
        membership = self._member_with_login("never@example.com", None)

        self.assertIsNone(last_sign_in_at_by_membership_id([membership])[membership.id])

    def test_membership_without_a_django_user_is_none(self):
        membership = TenantMembershipFactory(
            tenant=self.tenant, user_id=str(uuid.uuid4()), email="ghost@example.com"
        )

        self.assertIsNone(last_sign_in_at_by_membership_id([membership])[membership.id])

    def test_ignores_auth_users_last_sign_in_at(self):
        membership = self._member_with_login("supabase-only@example.com", None)
        SupabaseAuthUser.objects.create(
            id=uuid.UUID(str(membership.user_id)),
            email="supabase-only@example.com",
            last_sign_in_at=timezone.now(),
        )

        self.assertIsNone(last_sign_in_at_by_membership_id([membership])[membership.id])

    def test_same_user_in_two_tenants_shows_the_same_time(self):
        signed_in = _ago(hours=6)
        first = self._member_with_login("two-tenants@example.com", signed_in)
        second = TenantMembershipFactory(
            tenant=TenantFactory(), user_id=first.user_id, email="two-tenants@example.com"
        )

        result = last_sign_in_at_by_membership_id([first, second])

        self.assertEqual(result[first.id], result[second.id])
        self.assertAlmostEqual(result[first.id].timestamp(), signed_in.timestamp(), delta=1)

    def test_duplicate_email_accounts_show_the_most_recent_sign_in(self):
        recent = _ago(minutes=10)
        _separate_user("twice@example.com")
        _separate_user("twice@example.com", last_login=_ago(days=20))
        _separate_user("twice@example.com", last_login=recent)
        membership = TenantMembershipFactory(tenant=self.tenant, user_id=None, email="twice@example.com")

        value = last_sign_in_at_by_membership_id([membership])[membership.id]

        self.assertAlmostEqual(value.timestamp(), recent.timestamp(), delta=1)

    def test_uses_one_query_for_many_memberships(self):
        memberships = [
            self._member_with_login(f"bulk{i}@example.com", timezone.now()) for i in range(5)
        ]

        with self.assertNumQueries(1):
            result = last_sign_in_at_by_membership_id(memberships)

        self.assertEqual(set(result), {m.id for m in memberships})


# ---------------------------------------------------------------------------
# GET /membership/users/ (settings user list)
# ---------------------------------------------------------------------------


class UserListLastSignInAPITests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.client.force_authenticate(user=self.user)

    def _response(self):
        response = self.client.get("/membership/users/", **self.auth_headers)
        self.assertEqual(response.status_code, 200, response.data)
        return response

    def _row(self, email):
        results = self._response().data["results"]
        return next(item for item in results if item["email"] == email)

    def test_shows_authentication_user_last_login(self):
        signed_in = _ago(days=4)
        User.objects.filter(pk=self.user.pk).update(last_login=signed_in)
        SupabaseAuthUser.objects.filter(id=self.supabase_uid).update(last_sign_in_at=_ago(days=40))

        row = self._row(self.email)

        self.assertAlmostEqual(row["last_sign_in_at"].timestamp(), signed_in.timestamp(), delta=1)

    def test_ignores_auth_users_last_sign_in_at(self):
        User.objects.filter(pk=self.user.pk).update(last_login=None)
        SupabaseAuthUser.objects.filter(id=self.supabase_uid).update(last_sign_in_at=_ago(days=2))

        row = self._row(self.email)

        self.assertIsNone(row["last_sign_in_at"])

    def test_each_member_shows_their_own_sign_in(self):
        mine = _ago(days=1)
        theirs = _ago(hours=3)
        other_user, _ = self.make_authenticated_user(email="teammate@example.com")
        User.objects.filter(pk=self.user.pk).update(last_login=mine)
        User.objects.filter(pk=other_user.pk).update(last_login=theirs)

        self.assertAlmostEqual(
            self._row(self.email)["last_sign_in_at"].timestamp(), mine.timestamp(), delta=1
        )
        self.assertAlmostEqual(
            self._row("teammate@example.com")["last_sign_in_at"].timestamp(),
            theirs.timestamp(),
            delta=1,
        )

    def test_member_who_never_signed_in_is_null_next_to_one_who_did(self):
        User.objects.filter(pk=self.user.pk).update(last_login=_ago(hours=1))
        self.make_authenticated_user(email="never-signed-in@example.com")

        self.assertIsNotNone(self._row(self.email)["last_sign_in_at"])
        self.assertIsNone(self._row("never-signed-in@example.com")["last_sign_in_at"])

    def test_json_value_is_an_iso_datetime_string(self):
        signed_in = _ago(hours=2).replace(microsecond=0)
        User.objects.filter(pk=self.user.pk).update(last_login=signed_in)

        rows = self._response().json()["results"]
        raw = next(item for item in rows if item["email"] == self.email)["last_sign_in_at"]

        self.assertIsInstance(raw, str)
        parsed = parse_datetime(raw)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed, signed_in)

    def test_sign_in_recorded_by_the_token_check_shows_on_the_list(self):
        signed_in = _ts(_ago(minutes=20))
        _get_or_create_profile(
            {"sub": self.supabase_uid, "email": self.email, "amr": _amr(signed_in)}
        )

        row = self._row(self.email)

        self.assertEqual(_ts(row["last_sign_in_at"]), signed_in)
