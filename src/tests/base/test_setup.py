from django.test import TestCase
from rest_framework.test import APIClient
from django.conf import settings
from authentication.models import User
import jwt
import uuid

class BaseAPITestCase(TestCase):
    """
    Base class for API test cases.
    Sets up a test user, JWT authentication, and provides helpers for auth headers.
    """

    def setUp(self):
        self.client = APIClient()
        self.tenant_id = str(uuid.uuid4())
        self.supabase_uid = "test-uid-123"
        self.email = "testuser@example.com"
        self.role = "authenticated"

        self.user = self.create_test_user()
        self.token = self.generate_supabase_jwt()
        self.auth_headers = self.get_auth_headers()

    def create_test_user(self, **kwargs):
        defaults = {
            "supabase_uid": self.supabase_uid,
            "email": self.email,
            "tenant_id": self.tenant_id,
            "role": self.role,
        }
        defaults.update(kwargs)
        return User.objects.create_user(**defaults)

    def generate_supabase_jwt(self):
        payload = {
            "sub": self.supabase_uid,
            "email": self.email,
            "tenant_id": self.tenant_id,
            "role": self.role,
            "aud": "authenticated"
        }
        token = jwt.encode(payload, settings.SUPABASE_JWT_SECRET, algorithm="HS256")
        # PyJWT >=2 returns bytes, decode if needed
        if isinstance(token, bytes):
            token = token.decode("utf-8")
        return token

    def get_auth_headers(self):
        return {
            "HTTP_AUTHORIZATION": f"Bearer {self.token}",
            "HTTP_X_TENANT_ID": self.tenant_id
        }
