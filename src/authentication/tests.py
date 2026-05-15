import os
from unittest.mock import MagicMock, patch

from django.test import TestCase
from rest_framework.test import APIClient


class SupabasePasswordRecoverViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()

    @patch.dict(
        os.environ,
        {
            "SUPABASE_PROJECT_URL": "https://example.supabase.co",
            "SUPABASE_ANON_KEY": "test-anon-key",
        },
        clear=False,
    )
    @patch("authentication.views.requests.post")
    def test_returns_ok_when_supabase_returns_200(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {}
        mock_post.return_value = mock_resp

        response = self.client.post(
            "/auth/forgot-password/",
            {"email": " user@example.com ", "redirect_to": "https://app.example.com/auth/reset-password"},
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertEqual(args[0], "https://example.supabase.co/auth/v1/recover")
        self.assertEqual(kwargs["json"], {"email": "user@example.com"})
        self.assertEqual(
            kwargs["params"],
            {"redirect_to": "https://app.example.com/auth/reset-password"},
        )

    @patch.dict(
        os.environ,
        {
            "SUPABASE_PROJECT_URL": "https://example.supabase.co",
            "SUPABASE_ANON_KEY": "test-anon-key",
        },
        clear=False,
    )
    def test_requires_email(self):
        response = self.client.post("/auth/forgot-password/", {}, format="json")
        self.assertEqual(response.status_code, 400)

    @patch.dict(os.environ, {"SUPABASE_PROJECT_URL": "", "SUPABASE_ANON_KEY": ""}, clear=False)
    def test_missing_config_returns_503(self):
        response = self.client.post(
            "/auth/forgot-password/",
            {"email": "user@example.com"},
            format="json",
        )
        self.assertEqual(response.status_code, 503)
