import jwt
from jwt import InvalidTokenError
from django.contrib.auth.models import AnonymousUser
from django.conf import settings
from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions
import os

SUPABASE_JWT_SECRET = os.environ.get('SUPABASE_JWT_SECRET')

class SupabaseUser(AnonymousUser):
    @property
    def is_authenticated(self):
        return True
    
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
        # Optionally: Get or create user in Django DB, or just pass user info (Will come on this in future)
        user = SupabaseUser()
        user.supabase_uid = payload.get("sub")
        user.supabase_email = payload.get("email")
        return (user, None)
