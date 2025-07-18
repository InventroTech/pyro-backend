import jwt
from jwt import InvalidTokenError
from django.contrib.auth.models import AnonymousUser
from django.conf import settings
from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions
import os

SUPABASE_JWT_SECRET = os.environ.get('SUPABASE_JWT_SECRET')

class SupabaseUser(AnonymousUser):
    def __init__(self, supabase_uid=None, supabase_email=None, supabase_role=None, supabase_tenant_id=None):
        self.supabase_uid = supabase_uid
        self.supabase_email = supabase_email
        self.supabase_role = supabase_role
        self.supabase_tenant_id = supabase_tenant_id

    @property
    def is_authenticated(self):
        return True

    def __str__(self):
        return f"SupabaseUser({self.supabase_email or self.supabase_uid})"
    
class SupabaseJWTAuthentication(BaseAuthentication):
    def authenticate(self, request):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return None
        token = auth.split(" ")[1]
        try:
            payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"], options={"verify_aud": False})
        except InvalidTokenError:
            raise exceptions.AuthenticationFailed("Invalid Supabase JWT")

        user = SupabaseUser(
            supabase_uid=payload.get("sub"),
            supabase_email=payload.get("email"),
            supabase_role=payload.get("role"),
            supabase_tenant_id=payload.get("tenant_id")
        )
        return (user, None)

