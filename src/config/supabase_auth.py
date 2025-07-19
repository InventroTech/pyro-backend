import jwt
from jwt import InvalidTokenError
from django.contrib.auth.models import AnonymousUser
from django.conf import settings
from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions
import os
from authentication.models import User

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
        
        user, created = User.objects.get_or_create(
            supabase_uid=payload.get("sub"),
            defaults={
                'email': payload.get("email"),
                'role': payload.get("role"),
                'tenant_id': payload.get("tenant_id"),
                # Add more fields if needed
            }
        )
        # supabase sync with local user
        update_fields = {}
        if user.email != payload.get("email"):
            update_fields['email'] = payload.get("email")
        if user.role != payload.get("role"):
            update_fields['role'] = payload.get("role")
        if user.tenant_id != payload.get("tenant_id"):
            update_fields['tenant_id'] = payload.get("tenant_id")
        if update_fields:
            for key, value in update_fields.items():
                setattr(user, key, value)
            user.save(update_fields=list(update_fields.keys()))
        return (user, None)

