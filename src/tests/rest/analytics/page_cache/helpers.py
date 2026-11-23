"""Shared helpers for page-cache tests."""
from unittest.mock import MagicMock

from rest_framework.test import APIRequestFactory, force_authenticate

LOC_MEM_CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "page-cache-tests-split",
    }
}


def api_request(path, query=None, user_id="user-1", tenant_id=42):
    factory = APIRequestFactory()
    request = factory.get(path, query or {})
    user = MagicMock()
    user.supabase_uid = user_id
    user.id = user_id
    user.is_authenticated = True
    force_authenticate(request, user=user)
    request.tenant = MagicMock()
    request.tenant.id = tenant_id
    return request
