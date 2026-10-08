import uuid
from datetime import timedelta
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

from django.core.cache import cache
from django.test import TestCase, override_settings
from django.utils import timezone
from rest_framework.test import APIClient

from authentication.models import OAuthIdentity, OAuthLoginCode, RefreshToken, User
from authentication.tokens import verify_access_token

SITE = "http://localhost:8080"
CALLBACK = f"{SITE}/app/acme/auth/callback"


def _response(status_code, body):
    resp = MagicMock(status_code=status_code, content=b"x")
    resp.json.return_value = body
    return resp


@override_settings(
    AUTH_JWT_SECRET="test-auth-jwt-secret",
    AUTH_ALLOWED_REDIRECT_ORIGINS=[SITE],
    AUTH_SITE_URL=SITE,
    AUTH_PUBLIC_BASE_URL="https://api.example.com",
    GOOGLE_LOGIN_CLIENT_ID="google-client",
    GOOGLE_LOGIN_CLIENT_SECRET="google-secret",
    ZOHO_LOGIN_CLIENT_ID="zoho-client",
    ZOHO_LOGIN_CLIENT_SECRET="zoho-secret",
    ZOHO_LOGIN_ACCOUNTS_URL="https://accounts.zoho.in",
)
class OAuthLoginTests(TestCase):
    def setUp(self):
        cache.clear()
        self.client = APIClient()
        self.userinfo = {
            "sub": "google-123",
            "email": "Person@Example.com",
            "email_verified": True,
            "name": "Pat Person",
            "picture": "https://img.example.com/p.png",
        }
        post = patch("authentication.oauth.requests.post", side_effect=self._token_endpoint)
        get = patch("authentication.oauth.requests.get", side_effect=lambda *a, **k: _response(200, self.userinfo))
        self.post = post.start()
        self.get = get.start()
        self.addCleanup(post.stop)
        self.addCleanup(get.stop)

    def _token_endpoint(self, url, data=None, **kwargs):
        if data.get("code") == "bad-code":
            return _response(400, {"error": "invalid_grant"})
        return _response(200, {"access_token": "provider-access-token"})

    def _authorize(self, provider="google", redirect_to=CALLBACK):
        response = self.client.get(f"/auth/oauth/{provider}/authorize/", {"redirect_to": redirect_to})
        state = parse_qs(urlsplit(response["Location"]).query).get("state", [""])[0]
        return response, state

    def _callback(self, state, provider="google", code="provider-code", **extra):
        return self.client.get(f"/auth/oauth/{provider}/callback/", {"state": state, "code": code, **extra})

    def _frontend_params(self, response):
        self.assertEqual(response.status_code, 302)
        location = response["Location"]
        self.assertTrue(location.startswith(CALLBACK), location)
        return {k: v[0] for k, v in parse_qs(urlsplit(location).query).items()}

    def _full_login(self, provider="google", **callback_extra):
        _, state = self._authorize(provider)
        params = self._frontend_params(self._callback(state, provider, **callback_extra))
        return self.client.post("/auth/oauth/exchange/", {"code": params["code"]}, format="json")

    def test_authorize_redirects_to_google_with_state_cookie(self):
        response, state = self._authorize()

        self.assertEqual(response.status_code, 302)
        location = urlsplit(response["Location"])
        query = parse_qs(location.query)
        self.assertEqual(location.netloc, "accounts.google.com")
        self.assertEqual(query["client_id"], ["google-client"])
        self.assertEqual(query["redirect_uri"], ["https://api.example.com/auth/oauth/google/callback/"])
        self.assertTrue(state)
        cookie = response.cookies["pyro_oauth_nonce"]
        self.assertTrue(cookie["httponly"])
        self.assertTrue(cookie["secure"])

    def test_new_user_is_created_and_logged_in(self):
        response = self._full_login()

        self.assertEqual(response.status_code, 200)
        user = User.objects.get(email="person@example.com")
        self.assertIsNotNone(user.email_verified_at)
        self.assertFalse(user.has_usable_password())
        self.assertEqual(user.user_metadata["full_name"], "Pat Person")
        self.assertTrue(OAuthIdentity.objects.filter(user=user, provider="google", subject="google-123").exists())
        self.assertEqual(verify_access_token(response.json()["access_token"])["sub"], user.supabase_uid)

    def test_existing_user_is_linked_by_email(self):
        user = User.objects.create_user(
            supabase_uid=str(uuid.uuid4()), email="person@example.com", password="Correct-Horse-42",
            email_verified_at=timezone.now(),
        )

        response = self._full_login()

        self.assertEqual(response.json()["user"]["id"], user.supabase_uid)
        user.refresh_from_db()
        self.assertTrue(user.check_password("Correct-Horse-42"))
        self.assertEqual(User.objects.filter(email__iexact="person@example.com").count(), 1)

    def test_second_login_uses_identity_even_if_email_changed(self):
        self._full_login()
        self.userinfo["email"] = "renamed@example.com"

        response = self._full_login()

        self.assertEqual(response.json()["user"]["email"], "person@example.com")
        self.assertEqual(User.objects.count(), 1)

    def test_unconfirmed_password_account_loses_password_when_linked(self):
        squatter = User.objects.create_user(
            supabase_uid=str(uuid.uuid4()), email="person@example.com", password="Squatter-Pass-1"
        )
        RefreshToken.objects.create(
            user=squatter, token_hash="x" * 64, family_id=uuid.uuid4(),
            expires_at=timezone.now() + timedelta(days=1),
        )

        self._full_login()

        squatter.refresh_from_db()
        self.assertFalse(squatter.has_usable_password())
        self.assertIsNotNone(squatter.email_verified_at)
        self.assertFalse(RefreshToken.objects.filter(user=squatter, revoked_at__isnull=True, token_hash="x" * 64).exists())

    def test_unverified_provider_email_is_rejected(self):
        self.userinfo["email_verified"] = False
        _, state = self._authorize()

        params = self._frontend_params(self._callback(state))

        self.assertEqual(params["error"], "access_denied")
        self.assertIn("not verified", params["error_description"])
        self.assertFalse(User.objects.exists())

    def test_inactive_user_is_rejected(self):
        User.objects.create_user(
            supabase_uid=str(uuid.uuid4()), email="person@example.com", is_active=False,
            email_verified_at=timezone.now(),
        )
        _, state = self._authorize()

        params = self._frontend_params(self._callback(state))

        self.assertEqual(params["error"], "access_denied")

    def test_callback_without_browser_cookie_is_rejected(self):
        _, state = self._authorize()
        self.client.cookies.clear()

        params = self._frontend_params(self._callback(state))

        self.assertEqual(params["error"], "invalid_request")
        self.post.assert_not_called()

    def test_tampered_state_goes_to_site_url(self):
        response = self._callback("not-a-real-state")
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response["Location"].startswith(SITE + "?error=invalid_request"))

    def test_provider_error_is_passed_to_frontend(self):
        _, state = self._authorize()
        params = self._frontend_params(
            self.client.get("/auth/oauth/google/callback/", {"state": state, "error": "access_denied"})
        )
        self.assertEqual(params["error"], "access_denied")

    def test_failed_code_exchange(self):
        _, state = self._authorize()
        params = self._frontend_params(self._callback(state, code="bad-code"))
        self.assertIn("Unable to exchange external code", params["error_description"])

    def test_unlisted_redirect_falls_back_to_site_url(self):
        response, _ = self._authorize(redirect_to="https://evil.example.com/x")
        response = self._callback(parse_qs(urlsplit(response["Location"]).query)["state"][0])
        self.assertTrue(response["Location"].startswith(SITE + "?code="))

    def test_disabled_provider(self):
        with override_settings(GOOGLE_LOGIN_CLIENT_ID=""):
            response = self.client.get("/auth/oauth/google/authorize/", {"redirect_to": CALLBACK})
        self.assertIn("error=invalid_request", response["Location"])
        unknown = self.client.get("/auth/oauth/facebook/authorize/", {"redirect_to": CALLBACK})
        self.assertIn("error=invalid_request", unknown["Location"])

    def test_login_code_is_single_use_and_expires(self):
        _, state = self._authorize()
        code = self._frontend_params(self._callback(state))["code"]
        self.assertEqual(self.client.post("/auth/oauth/exchange/", {"code": code}, format="json").status_code, 200)

        again = self.client.post("/auth/oauth/exchange/", {"code": code}, format="json")
        self.assertEqual(again.json()["error"], "invalid_grant")

        _, state = self._authorize()
        code = self._frontend_params(self._callback(state))["code"]
        OAuthLoginCode.objects.update(expires_at=timezone.now() - timedelta(seconds=1))
        expired = self.client.post("/auth/oauth/exchange/", {"code": code}, format="json")
        self.assertEqual(expired.status_code, 400)

    def test_zoho_uses_india_accounts_server(self):
        response, _ = self._authorize("zoho")
        self.assertEqual(urlsplit(response["Location"]).netloc, "accounts.zoho.in")

        self.userinfo = {"sub": "zoho-9", "email": "z@example.com", "name": "Zed"}
        login = self._full_login("zoho")

        self.assertEqual(login.status_code, 200)
        self.assertEqual(self.post.call_args.args[0], "https://accounts.zoho.in/oauth/v2/token")
        self.assertTrue(OAuthIdentity.objects.filter(provider="zoho", subject="zoho-9").exists())

    def test_zoho_other_data_centre_only_if_trusted(self):
        self._full_login("zoho", **{"accounts-server": "https://accounts.zoho.eu"})
        self.assertEqual(self.post.call_args.args[0], "https://accounts.zoho.eu/oauth/v2/token")

        self._full_login("zoho", **{"accounts-server": "https://evil.example.com"})
        self.assertEqual(self.post.call_args.args[0], "https://accounts.zoho.in/oauth/v2/token")
