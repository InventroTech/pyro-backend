import logging
import os
import requests
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from django.contrib.auth import get_user_model

# Create your views here.
# Logging and config
logger = logging.getLogger(__name__)
SUPABASE_PROJECT_URL = os.environ.get('SUPABASE_PROJECT_URL')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY')
User = get_user_model()

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
            return Response({
                "valid": True,
                "user_id": out.get("user", {}).get("id"),
                "access_token": out.get("access_token"),
                "email": out.get("user", {}).get("email")
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


class SupabaseSignUpView(APIView):
    """
    Register new user with Supabase through Django backend.
    """
    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        email = request.data.get("email")
        password = request.data.get("password")
        if not email or not password:
            return Response({"error": "Email and password are required."}, status=status.HTTP_400_BAD_REQUEST)

        url = f"{SUPABASE_PROJECT_URL}/auth/v1/signup"
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
            return Response({
                "success": True,
                "user": out.get("user"),
                "session": out.get("session"),
                "access_token": out.get("access_token")
            })
        else:
            error_body = {}
            try:
                error_body = r.json()
            except Exception:
                pass
            return Response({
                "success": False,
                "error": error_body.get("error", "Signup failed"),
                "message": error_body.get("msg") or error_body.get("message")
            }, status=status.HTTP_400_BAD_REQUEST)


class SupabaseSessionView(APIView):
    """
    Get current session from Supabase through Django backend.
    """
    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        access_token = request.data.get("access_token")
        refresh_token = request.data.get("refresh_token")
        
        if not access_token:
            return Response({"error": "Access token is required."}, status=status.HTTP_400_BAD_REQUEST)

        url = f"{SUPABASE_PROJECT_URL}/auth/v1/user"
        headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        
        try:
            r = requests.get(url, headers=headers)
        except Exception as e:
            logger.exception("Failed to call Supabase: %s", e)
            return Response({"error": "Failed to connect to Supabase."}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

        if r.status_code == 200:
            user_data = r.json()
            return Response({
                "success": True,
                "user": user_data,
                "session": {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "user": user_data
                }
            })
        else:
            return Response({
                "success": False,
                "error": "Invalid token"
            }, status=status.HTTP_401_UNAUTHORIZED)


class SupabaseSignOutView(APIView):
    """
    Sign out user through Supabase via Django backend.
    """
    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        access_token = request.data.get("access_token")
        
        if not access_token:
            return Response({"error": "Access token is required."}, status=status.HTTP_400_BAD_REQUEST)

        url = f"{SUPABASE_PROJECT_URL}/auth/v1/logout"
        headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        
        try:
            r = requests.post(url, headers=headers)
        except Exception as e:
            logger.exception("Failed to call Supabase: %s", e)
            return Response({"error": "Failed to connect to Supabase."}, status=status.HTTP_503_SERVICE_UNAVAILABLE)

        return Response({
            "success": True,
            "message": "Successfully signed out"
        })


class SupabaseOAuthView(APIView):
    """
    Get OAuth URL for provider through Django backend.
    """
    authentication_classes = []
    permission_classes = [AllowAny]

    def post(self, request):
        provider = request.data.get("provider")
        redirect_to = request.data.get("redirect_to")
        
        if not provider:
            return Response({"error": "Provider is required."}, status=status.HTTP_400_BAD_REQUEST)

        # Construct OAuth URL
        oauth_url = f"{SUPABASE_PROJECT_URL}/auth/v1/authorize"
        params = {
            "provider": provider,
            "redirect_to": redirect_to or f"{request.META.get('HTTP_ORIGIN', '')}/auth/callback"
        }
        
        # Build query string
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        full_url = f"{oauth_url}?{query_string}"
        
        return Response({
            "success": True,
            "url": full_url,
            "provider": provider
        })


class InviteUserView(APIView):
    """Invite a user to join a tenant"""
    
    def post(self, request):
        email = request.data.get("email")
        tenant_id = request.data.get("tenantId")
        role = request.data.get("role", "app_user")
        
        if not email or not tenant_id:
            return Response({"error": "Email and tenantId are required."}, status=status.HTTP_400_BAD_REQUEST)

        # Here you would implement your user invitation logic
        # This could involve:
        # 1. Creating a user invitation record in your database
        # 2. Sending an email invitation
        # 3. Creating a temporary user account
        
        try:
            # For now, just return success
            # You'll need to implement the actual invitation logic based on your requirements
            return Response({
                "success": True,
                "message": f"Invitation sent to {email} for role {role}"
            })
        except Exception as e:
            logger.exception("Failed to invite user: %s", e)
            return Response({
                "success": False,
                "error": "Failed to send invitation"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class UpdateUserRoleView(APIView):
    """Update a user's role"""
    
    def put(self, request, user_id):
        role_id = request.data.get("role_id")
        
        if not role_id:
            return Response({"error": "role_id is required."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            # Here you would implement your user role update logic
            # This could involve:
            # 1. Finding the user by user_id
            # 2. Updating their role_id in the database
            # 3. Returning success response
            
            # For now, just return success
            # You'll need to implement the actual update logic based on your user model
            return Response({
                "success": True,
                "message": f"User {user_id} role updated to {role_id}"
            })
        except Exception as e:
            logger.exception("Failed to update user role: %s", e)
            return Response({
                "success": False,
                "error": "Failed to update user role"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
