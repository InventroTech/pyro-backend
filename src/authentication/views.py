import logging
import os
import requests
import jwt
from jwt import InvalidTokenError
import uuid
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from django.db import models
from authz.models import TenantMembership
from authz.models import Role as AuthzRole
from core.models import Tenant

# Create your views here.
# Logging and config
logger = logging.getLogger(__name__)
SUPABASE_PROJECT_URL = os.environ.get('SUPABASE_PROJECT_URL')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY')
SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET")


def enrich_token_with_user_details(token: str, user_id: str, email: str) -> str:
    """
    Enrich Supabase JWT token with tenant and role information.
    This ensures the bearer token contains all user details needed by the client.
    
    Args:
        token: Original Supabase JWT token
        user_id: Supabase user ID (UUID)
        email: User email address
        
    Returns:
        Enriched JWT token with tenant_id, role_key, and role_id claims
    """
    if not SUPABASE_JWT_SECRET:
        logger.warning("SUPABASE_JWT_SECRET not configured, returning original token")
        return token
    
    try:
        # Decode the original token to get base claims
        claims = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"], options={"verify_aud": False})
    except InvalidTokenError as e:
        logger.warning(f"Failed to decode token: {e}, returning original token")
        return token
    
    # Find active tenant memberships for this user
    # Try by user_id first, then fallback to email
    try:
        user_uuid = uuid.UUID(user_id) if user_id else None
    except (ValueError, TypeError):
        user_uuid = None
    
    membership_query = TenantMembership.objects.filter(is_active=True)
    if user_uuid:
        membership_query = membership_query.filter(models.Q(user_id=user_uuid) | models.Q(email__iexact=email))
    else:
        membership_query = membership_query.filter(email__iexact=email)
    
    memberships = membership_query.select_related('tenant', 'role').order_by('created_at')
    
    if not memberships.exists():
        logger.info(f"No active tenant memberships found for user {user_id} ({email})")
        return token
    
    # Get the first active tenant membership (primary tenant)
    # In the future, this could be configurable or include all tenants
    membership = memberships.first()
    tenant = membership.tenant
    role = membership.role
    
    # Enrich claims with tenant and role information
    enriched_claims = {
        **claims,  # Include all original claims
        "tenant_id": str(tenant.id),
        "user_id": user_id,
        "role_id": str(role.id),
        "role_key": role.key,
    }
    
    # Re-sign the token with enriched claims
    try:
        enriched_token = jwt.encode(enriched_claims, SUPABASE_JWT_SECRET, algorithm="HS256")
        if isinstance(enriched_token, bytes):
            enriched_token = enriched_token.decode("utf-8")
        logger.info(f"Successfully enriched token for user {user_id} with tenant {tenant.slug} and role {role.key}")
        return enriched_token
    except Exception as e:
        logger.error(f"Failed to re-sign enriched token: {e}, returning original token")
        return token


class SupabaseAuthCheckView(APIView):
    """
    Authenticates with Supabase using email & password.
    Returns user info if valid, else error.
    """
    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        email = request.data.get("email")
        password = request.data.get("password")
        if not email or not password:
            return Response({"error": "Email and password are required."}, status=status.HTTP_400_BAD_REQUEST)

        url = f"{SUPABASE_PROJECT_URL}/auth/v1/token?grant_type=password"
        headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Content-Type": "application/json",
        }
        data = {"email": email, "password": password}
        try:
            r = requests.post(url, json=data, headers=headers)
        except Exception as e:
            logger.exception("Failed to call Supabase: %s", e)
            return Response({"error": "Failed to connect to Supabase."}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

        if r.status_code == 200:
            out = r.json()
            user_id = out.get("user", {}).get("id")
            email = out.get("user", {}).get("email")
            original_token = out.get("access_token")
            
            # Enrich the token with tenant and role information
            enriched_token = enrich_token_with_user_details(original_token, user_id, email)
            
            return Response({
                "valid": True,
                "user_id": user_id,
                "access_token": enriched_token,
                "email": email
            })
        else:
            error_body = {}
            try:
                error_body = r.json()
            except Exception:
                pass
            return Response({
                "valid": False,
                "error": error_body.get("error", "Login failed"),
                "message": error_body.get("msg") or error_body.get("message")
            }, status=status.HTTP_401_UNAUTHORIZED)
