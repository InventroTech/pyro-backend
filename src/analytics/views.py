from django.db.models import F, ExpressionWrapper, DurationField, Avg
from django.db.models.functions import TruncDate
from django.db.models import Count, Q
from rest_framework.views import APIView
from .models import SupportTicket
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated, AllowAny
import logging
import os
import requests
from .models import SupportTicket

logger = logging.getLogger(__name__) 
SUPABASE_PROJECT_URL = os.environ.get('SUPABASE_PROJECT_URL')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY')
# logger examples for Bibhab
# logger.info("This is an info log!")
# logger.warning("This is a warning!")
# logger.error("This is an error!")

class DailyResolvedTicketsView(APIView):
    permission_classes = []  # Will add it in future with tenant id support

    def get(self, request):
        # Only show tickets for this tenant if required
        tenant_id = getattr(request.user, 'supabase_tenant_id', None)
        qs = SupportTicket.objects.all()
        if tenant_id:
            qs = qs.filter(tenant_id=tenant_id)

        # Consider ticket resolved if 'completed_at' is not null
        qs = qs.filter(completed_at__isnull=False)
        data = (
            qs
            .annotate(date=TruncDate('completed_at'))
            .values('date')
            .annotate(count=Count('id'))
            .order_by('date')
        )
        return Response(list(data))

class TicketClosureTimeAnalytics(APIView):
    permission_classes = [IsAuthenticated]  # Add IsAuthenticated 
    def get(self, request):
        # print(request.user.supabase_tenant_id)
        # print(request.user.supabase_email)
        # print(request.user.supabase_role) (adding access to supabase user's role etc in the request itself by auth)
        start_date = request.query_params.get('start')
        end_date = request.query_params.get('end')
        # tenant_id=tenant_id 
        qs = SupportTicket.objects.filter(
            completed_at__isnull=False,
        )
        if start_date:
            qs = qs.filter(completed_at__date__gte=start_date)
        if end_date:
            qs = qs.filter(completed_at__date__lte=end_date)

        # Duration in hours
        qs = qs.annotate(
            closure_time=ExpressionWrapper(
                F('completed_at') - F('created_at'),
                output_field=DurationField()
            ),
            day=TruncDate('completed_at')
        )

        results = (
            qs.values('day')
              .annotate(avg_closure_hours=Avg(ExpressionWrapper(
                  F('closure_time'),
                  output_field=DurationField()
              )))
              .order_by('day')
        )
        # Convert duration to float hours
        data = [
            {
                "date": r["day"],
                "avg_closure_hours": round(r["avg_closure_hours"].total_seconds() / 3600, 2)
            }
            for r in results if r["avg_closure_hours"] is not None
        ]
        return Response(data)


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
