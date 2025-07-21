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
import numpy as np

logger = logging.getLogger(__name__) 
SUPABASE_PROJECT_URL = os.environ.get('SUPABASE_PROJECT_URL')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY')
# logger examples for Bibhab
# logger.info("This is an info log!")
# logger.warning("This is a warning!")
# logger.error("This is an error!")


class StackedBarResolvedUnresolvedView(APIView):
    permission_classes = []  # Use [IsAuthenticated] in production

    def get(self, request):
        start = request.query_params.get('start')
        end = request.query_params.get('end')
        tenant_id = request.query_params.get('tenant_id')

        qs = SupportTicket.objects.all()
        qs = qs.filter(created_at__isnull=False)

        # Optional: filter by tenant_id
        if tenant_id:
            qs = qs.filter(tenant_id=tenant_id)

        # Date filtering
        if start:
            qs = qs.filter(created_at__date__gte=start)
        if end:
            qs = qs.filter(created_at__date__lte=end)

        # Find all unique days with tickets
        dates = (
            qs.annotate(day=TruncDate('created_at'))
              .values_list('day', flat=True)
              .distinct()
        )

        results = []
        for date in sorted(dates):
            day_qs = qs.filter(created_at__date=date)
            resolved = day_qs.filter(
                completed_at__isnull=False,
                resolution_status__iexact='resolved'  # case-insensitive match
            ).count()
            unresolved = day_qs.exclude(
                Q(completed_at__isnull=False) & Q(resolution_status__iexact='resolved')
            ).count()
            results.append({
                'date': date.strftime("%Y-%m-%d"),
                'resolved': resolved,
                'unresolved': unresolved
            })
        return Response(results)


class DailyPercentileResolutionTimeView(APIView):
    permission_classes = []  # Use IsAuthenticated for production

    def get(self, request):
        percentile = float(request.query_params.get('percentile', 90))
        unit = request.query_params.get('unit', 'hours').lower()
        start_date = request.query_params.get('start')
        end_date = request.query_params.get('end')

        qs = SupportTicket.objects.filter(
            completed_at__isnull=False,
            created_at__isnull=False
        )

        # Optional date filtering
        if start_date:
            qs = qs.filter(completed_at__date__gte=start_date)
        if end_date:
            qs = qs.filter(completed_at__date__lte=end_date)

        # Annotate each ticket with its resolution day
        qs = qs.annotate(resolved_date=TruncDate('completed_at'))

        # Helper: convert seconds to desired unit
        def convert(seconds, unit):
            if unit == 'seconds':
                return round(seconds, 2)
            elif unit == 'minutes':
                return round(seconds / 60, 2)
            elif unit == 'hours':
                return round(seconds / 3600, 2)
            elif unit == 'days':
                return round(seconds / 86400, 2)
            return round(seconds / 3600, 2)  # default: hours

        # Build dict of day -> list of resolution times
        data_by_day = {}
        for ticket in qs:
            day = ticket.resolved_date
            res_time = (ticket.completed_at - ticket.created_at).total_seconds()
            if day not in data_by_day:
                data_by_day[day] = []
            data_by_day[day].append(res_time)

        # Calculate desired percentile for each day
        result = []
        for day, times in sorted(data_by_day.items()):
            if times:
                pct_val = float(np.percentile(times, percentile))
                result.append({
                    "date": day.strftime("%Y-%m-%d"),
                    f"percentile_{int(percentile)}_{unit}": convert(pct_val, unit)
                })

        return Response(result)



class DailyResolvedTicketsView(APIView):
    permission_classes = [IsAuthenticated]  # Will add it in future with tenant id support

    def get(self, request):
        # Only show tickets for this tenant if required
        start_date = request.query_params.get('start')
        end_date = request.query_params.get('end')
        print()
        qs = SupportTicket.objects.all()
        # Consider ticket resolved if 'completed_at' is not null
        if start_date:
            qs = qs.filter(completed_at__date__gte=start_date)
        if end_date:
            qs = qs.filter(completed_at__date__lte=end_date)
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
    permission_classes = [IsAuthenticated]

    def get(self, request):
        start_date = request.query_params.get('start')
        end_date = request.query_params.get('end')
        unit = request.query_params.get('unit', 'hours').lower()

        qs = SupportTicket.objects.filter(completed_at__isnull=False)
        if start_date:
            qs = qs.filter(completed_at__date__gte=start_date)
        if end_date:
            qs = qs.filter(completed_at__date__lte=end_date)

        qs = qs.annotate(
            closure_time=ExpressionWrapper(
                F('completed_at') - F('created_at'),
                output_field=DurationField()
            ),
            day=TruncDate('completed_at')
        )

        results = (
            qs.values('day')
              .annotate(avg_closure=Avg('closure_time'))
              .order_by('day')
        )

        # Conversion factor
        def convert_timedelta(td, unit):
            if unit == 'seconds':
                return round(td.total_seconds(), 2)
            elif unit == 'minutes':
                return round(td.total_seconds() / 60, 2)
            elif unit == 'hours':
                return round(td.total_seconds() / 3600, 2)
            elif unit == 'days':
                return round(td.total_seconds() / 86400, 2)
            else:
                return round(td.total_seconds() / 3600, 2)  # default to hours

        data = [
            {
                "date": r["day"],
                f"avg_closure_time": convert_timedelta(r["avg_closure"], unit)
            }
            for r in results if r["avg_closure"] is not None
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
