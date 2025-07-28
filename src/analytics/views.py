import logging
import numpy as np
from django.db.models import F, ExpressionWrapper, DurationField, Avg, Count, Q
from django.db.models.functions import TruncDate
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .models import SupportTicket
from rest_framework.permissions import IsAuthenticated
from datetime import datetime
from .utils import (
    extract_date_range_from_request,
    filter_by_tenant,
    get_date_range,
    convert_seconds,
    convert_timedelta,
)

# --- Config & Logging ---
logger = logging.getLogger(__name__)

class StackedBarResolvedUnresolvedView(APIView):
    """Stacked bar data for resolved/unresolved support tickets per day."""
    permission_classes = [IsAuthenticated]

    def get(self, request):
        qs = SupportTicket.objects.filter(dumped_at__isnull=False)
        qs = filter_by_tenant(qs, request)
        start_date, end_date = extract_date_range_from_request(qs, request, created_field='dumped_at')
        results = []
        date_range = list(get_date_range(start_date, end_date))
        if not date_range:
            today = datetime.today().date()
            return Response([{'x': today.strftime("%Y-%m-%d"), 'y1': 0, 'y2': 0}])
        for date in date_range:
            day_qs = qs.filter(dumped_at__date=date)
            resolved = day_qs.filter(
                completed_at__isnull=False,
                resolution_status__iexact='resolved'
            ).count()
            unresolved = day_qs.exclude(
                Q(completed_at__isnull=False) & Q(resolution_status__iexact='resolved')
            ).count()
            results.append({
                'x': date.strftime("%Y-%m-%d"),
                'y1': resolved,
                'y2': unresolved
            })
        return Response(results)

class DailyPercentileResolutionTimeView(APIView):
    """
    Returns daily Nth percentile (default 90th) of ticket resolution time for a date range.
    Query params: percentile, unit, start, end, tenant_id
    """
    permission_classes = []

    def get(self, request):
        logger.info("DailyPercentileResolutionTimeView called with query_params: %s", request.query_params)
        
        try:
            percentile = float(request.query_params.get('percentile', 90))
        except ValueError:
            return Response(
                {"detail": "Percentile must be a number."},
                status=status.HTTP_400_BAD_REQUEST
            )
        unit = request.query_params.get('unit', 'hours').lower()

        qs = SupportTicket.objects.filter(completed_at__isnull=False, dumped_at__isnull=False)
        qs = filter_by_tenant(qs, request)
        if not qs.exists():
            today = datetime.today().date()
            logger.warning("No support tickets found for given filters. Returning today's date with y=0.")
            return Response([{"x": today.strftime("%Y-%m-%d"), "y": 0}])

        start_date, end_date = extract_date_range_from_request(qs, request, created_field='completed_at')
        qs = qs.filter(completed_at__date__gte=start_date, completed_at__date__lte=end_date)
        qs = qs.annotate(resolved_date=TruncDate('completed_at'))

        data_by_day = {}
        for ticket in qs:
            try:
                day = ticket.resolved_date
                res_time = (ticket.completed_at - ticket.dumped_at).total_seconds()
                data_by_day.setdefault(day, []).append(res_time)
            except Exception as e:
                logger.warning("Failed to calculate resolution time for ticket %s: %s", getattr(ticket, 'id', None), e)

        result = []
        for date in get_date_range(start_date, end_date):
            times = data_by_day.get(date, [])
            y = 0
            if times:
                try:
                    pct_val = float(np.percentile(times, percentile))
                    y = convert_seconds(pct_val, unit)
                except Exception as e:
                    logger.warning("Percentile calculation failed on %s: %s", date, e)
            result.append({"x": date.strftime("%Y-%m-%d"), "y": y})
        logger.info("Returning %d data points", len(result))
        return Response(result)

class DailyResolvedTicketsView(APIView):
    permission_classes = []

    def get(self, request):
        qs = SupportTicket.objects.filter(completed_at__isnull=False)
        qs = filter_by_tenant(qs, request)
        start_date, end_date = extract_date_range_from_request(qs, request, created_field='completed_at')
        if not start_date or not end_date:
            today = datetime.today().date()
            return Response([{"x": today.strftime("%Y-%m-%d"), "y": 0}])

        qs = qs.filter(completed_at__date__gte=start_date, completed_at__date__lte=end_date)
        resolved_data = (
            qs.annotate(date=TruncDate('completed_at'))
            .values('date')
            .annotate(count=Count('id'))
        )
        resolved_map = {entry['date']: entry['count'] for entry in resolved_data}
        all_dates = get_date_range(start_date, end_date)
        result = [
            {"x": date.strftime("%Y-%m-%d"), "y": resolved_map.get(date, 0)}
            for date in all_dates
        ]
        return Response(result)

class TicketClosureTimeAnalytics(APIView):
    """
    Returns daily average ticket closure time.
    Query params: start, end, unit
    """
    permission_classes = []

    def get(self, request):
        qs = SupportTicket.objects.filter(completed_at__isnull=False, dumped_at__isnull=False)
        qs = filter_by_tenant(qs, request)
        start_date, end_date = extract_date_range_from_request(qs, request, created_field='completed_at')
        if not start_date or not end_date:
            today = datetime.today().date()
            return Response([{"x": today.strftime("%Y-%m-%d"), "y": 0}])

        qs = qs.filter(completed_at__date__gte=start_date, completed_at__date__lte=end_date)
        qs = qs.annotate(
            closure_time=ExpressionWrapper(
                F('completed_at') - F('dumped_at'),
                output_field=DurationField()
            ),
            day=TruncDate('completed_at')
        )

        aggregated = (
            qs.values('day')
            .annotate(avg_closure=Avg('closure_time'))
        )
        avg_map = {
            item['day']: convert_timedelta(item['avg_closure'], request.query_params.get('unit', 'hours').lower())
            for item in aggregated if item['avg_closure'] is not None
        }
        result = [
            {"x": date.strftime("%Y-%m-%d"), "y": avg_map.get(date, 0)}
            for date in get_date_range(start_date, end_date)
        ]
        return Response(result)

