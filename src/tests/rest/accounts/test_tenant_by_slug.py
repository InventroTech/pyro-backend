"""Tenant slug lookup reads the application database, not Supabase PostgREST."""
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase
from rest_framework import status

from accounts.views import TenantBySlugView


def _request(slug):
    request = MagicMock()
    request.query_params = {"slug": slug}
    return request


class TenantBySlugViewTests(SimpleTestCase):
    def test_missing_slug_returns_400(self):
        response = TenantBySlugView().get(_request("   "))
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    @patch("accounts.views.Tenant")
    def test_unknown_slug_returns_404(self, mock_tenant):
        mock_tenant.objects.filter.return_value.only.return_value.first.return_value = None
        response = TenantBySlugView().get(_request("missing-org"))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        mock_tenant.objects.filter.assert_called_once_with(slug="missing-org")

    @patch("accounts.views.Tenant")
    def test_existing_slug_returns_id(self, mock_tenant):
        row = MagicMock()
        row.id = "0d0ae3a6-4706-403c-89ae-149e596d17da"
        row.slug = "cdspace"
        row.name = "CD Space"
        mock_tenant.objects.filter.return_value.only.return_value.first.return_value = row

        response = TenantBySlugView().get(_request("CD Space"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], "0d0ae3a6-4706-403c-89ae-149e596d17da")
        self.assertEqual(response.data["slug"], "cdspace")
        mock_tenant.objects.filter.assert_called_once_with(slug="cdspace")
