from django.db.models import F, ExpressionWrapper, DurationField, Avg
from django.db.models.functions import TruncDate
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated, AllowAny
import logging
import os
import requests

logger = logging.getLogger(__name__) 
SUPABASE_PROJECT_URL = os.environ.get('SUPABASE_PROJECT_URL')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY')
# logger examples for Bibhab
# logger.info("This is an info log!")
# logger.warning("This is a warning!")
# logger.error("This is an error!")


class SupabaseAuthCheckView(APIView):
    authentication_classes = []  # Allow unauthenticated for this test
    permission_classes = []
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
        data = {
            "email": email,
            "password": password,
        }

        r = requests.post(url, json=data, headers=headers)
        if r.status_code == 200:
            out = r.json()
            return Response({
                "valid": True,
                "user_id": out.get("user", {}).get("id"),
                "access_token": out.get("access_token"),
                "email": out.get("user", {}).get("email")
            })
        else:
            return Response({
                "valid": False,
                "error": r.json().get("error", "Login failed"),
                "message": r.json().get("msg") or r.json().get("message")
            }, status=status.HTTP_401_UNAUTHORIZED)
