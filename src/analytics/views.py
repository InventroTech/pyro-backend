import logging
import numpy as np
from django.db.models import F, ExpressionWrapper, DurationField, Avg, Count, Q, Func, IntegerField
from django.db.models.functions import TruncDate
from django.db import connection
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from support_ticket.models import SupportTicket
from rest_framework.permissions import IsAuthenticated, AllowAny
from datetime import datetime, time
from django.utils import timezone
from django.db.models import Count
import uuid
from .utils import (
    extract_date_range_from_request,
    filter_by_tenant,
    get_date_range,
    convert_seconds,
    convert_timedelta,
)
from analytics_ai.executor import execute_safe_sql
from analytics_ai.formatter import format_results_for_table
from analytics_ai.llm_query import get_sql_from_llm, clean_llm_sql_output
from analytics_ai.logging_utils import log_analytics_event
from analytics_ai.prompt_builder import build_llm_prompt
from analytics_ai.sql_validator import is_safe_sql
from analytics_ai.schema_loader import generate_schema_summary


from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from analytics.models import AnalyticsRunCore
from analytics.utils import preview_result
from rest_framework.generics import ListAPIView
from rest_framework.exceptions import ValidationError
from .serializers import SupportTicketSerializer
from core.pagination import MetaPageNumberPagination
from .filters import (
    get_multi_values, build_nullable_in_q,
    POSTER_CHOICES, RESOLUTION_CHOICES,
    SafeSearchFilter, SafeOrderingFilter
)
from .utils import tenant_scoped_qs
from django.db import models
from django.contrib.auth import get_user_model
from authz.permissions import IsTenantAuthenticated
from .utils import _distinct_list






# "How many support tickets did each executive resolve last week?"
# "Which executive had the fastest average resolution time last month?"
# "Show me the number of open vs closed tickets handled by each support executive."
# "List the top 3 executives by the number of tickets resolved in the past month."
# "Which executive has the highest unresolved ticket count right now?"

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


class AnalyticsQueryView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        question = request.data.get('question', '').strip()
        print("question = ", question)
        user_id = 'ff1e3660-2c8d-45a1-bda8-09c76b857a89'

        if not question:
            log_analytics_event("input_error", user_id, question, error="Question is required")
            return Response({"error": "Question is required."}, status=status.HTTP_400_BAD_REQUEST)

        # Create run row (only the fields we keep)
        run = AnalyticsRunCore.objects.create(
            user_id=user_id,
            question=question,
            status="started",
        )

        # 2. Schema Generation (no DB save of schema_str)
        try:
            schema_str = generate_schema_summary(app_labels=['analytics'])
            log_analytics_event("schema_generated", user_id, question, llm_prompt=None, sql_query=None, result=schema_str)
        except Exception as e:
            log_analytics_event("schema_error", user_id, question, error=str(e))
            run.status = "schema_error"
            run.completed_at = timezone.now()
            run.save(update_fields=["status", "completed_at"])
            return Response({"error": "Failed to generate schema."}, status=500)

        # 3. Prompt Build (no DB save of prompt/examples)
        try:
            extra_instruction = ""
            if "resolution_time" in schema_str:
                extra_instruction = (
                    "IMPORTANT: The field 'resolution_time' in the support_ticket table is stored as a string in 'MM:SS' format. "
                    "To calculate averages or aggregates, convert it to seconds in SQL using: "
                    "(SPLIT_PART(resolution_time, ':', 1)::int * 60 + SPLIT_PART(resolution_time, ':', 2)::int). "
                    "Use this conversion in your SQL. Do NOT use CAST(resolution_time AS INTEGER) or CAST(resolution_time AS DOUBLE PRECISION)."
                )
            extra_instruction += (
                "\n\nWhen a time range or date is needed, use parameterized placeholders "
                "compatible with psycopg2: %(start)s, %(end)s, %(today)s. "
                "NEVER use square-bracket tokens like [start], [end], [today]. "
                "Prefer half-open ranges: completed_at >= %(start)s AND completed_at < %(end)s."
            )

            
            examples = (
    "Example:\n"
    "Q: Which agent resolved the most support tickets last month?\n"
    "A: SELECT cse_name, COUNT(*) AS tickets_resolved "
    "FROM support_ticket "
    "WHERE resolution_status = 'Resolved' "
    "  AND completed_at >= %(start)s "
    "  AND completed_at < %(end)s "
    "GROUP BY cse_name "
    "ORDER BY tickets_resolved DESC "
    "LIMIT 5;\n"
    "\n"
    "Example:\n"
    "Q: How many tickets remain unresolved as of today?\n"
    "A: SELECT COUNT(*) AS unresolved_tickets "
    "FROM support_ticket "
    "WHERE resolution_status != 'Resolved' "
    "  AND dumped_at <= %(today)s;\n"
    "\n"
    "Example:\n"
    "Q: What is the average resolution time (in seconds) for resolved tickets for each agent?\n"
    "A: SELECT cse_name, "
    "AVG(CASE WHEN resolution_status = 'Resolved' THEN "
    "(SPLIT_PART(resolution_time, ':', 1)::int * 60 + SPLIT_PART(resolution_time, ':', 2)::int) END) "
    "AS avg_resolution_time_seconds "
    "FROM support_ticket "
    "GROUP BY cse_name;\n"
)




            prompt = build_llm_prompt(
                user_question=question,
                schema_str=schema_str,
                instructions=extra_instruction,
                examples=examples
            )
            log_analytics_event("prompt_built", user_id, question, llm_prompt=prompt)
        except Exception as e:
            log_analytics_event("prompt_build_error", user_id, question, error=str(e))
            run.status = "prompt_error"
            run.completed_at = timezone.now()
            run.save(update_fields=["status", "completed_at"])
            return Response({"error": "Failed to build LLM prompt."}, status=500)

        # 4. LLM SQL Generation (save final SQL only)
        try:
            raw_sql_query, llm_raw_response = get_sql_from_llm(prompt)
            if not raw_sql_query:
                log_analytics_event("llm_generation_error", user_id, question, llm_prompt=prompt, error="No SQL generated")
                run.status = "llm_generation_error"
                run.completed_at = timezone.now()
                run.save(update_fields=["status", "completed_at"])
                return Response({"error": "LLM could not generate a SQL query. Try rephrasing your question."}, status=400)

            sql_query = clean_llm_sql_output(raw_sql_query)
            log_analytics_event("llm_sql_generated", user_id, question, llm_prompt=prompt, sql_query=raw_sql_query, result=llm_raw_response)
            log_analytics_event("llm_sql_cleaned", user_id, question, llm_prompt=prompt, sql_query=sql_query)

            run.sql_query = sql_query
            run.save(update_fields=["sql_query"])
        except Exception as e:
            log_analytics_event("llm_call_error", user_id, question, llm_prompt=prompt, error=str(e))
            run.status = "llm_call_error"
            run.completed_at = timezone.now()
            run.save(update_fields=["status", "completed_at"])
            return Response({"error": "LLM service error. Please try again later."}, status=500)

        # 5. SQL Validation (save validation fields)
        allowed_tables = {"support_ticket"}
        try:
            is_safe, reason = is_safe_sql(sql_query, allowed_tables)
            run.validation_ok = bool(is_safe)
            run.validation_reason = reason or ""
            if not is_safe:
                log_analytics_event("sql_validation_failed", user_id, question, llm_prompt=prompt, sql_query=sql_query, error=reason)
                run.status = "validation_failed"
                run.completed_at = timezone.now()
                run.save(update_fields=["validation_ok", "validation_reason", "status", "completed_at"])
                return Response({"error": reason}, status=400)

            log_analytics_event("sql_validated", user_id, question, llm_prompt=prompt, sql_query=sql_query)
            run.save(update_fields=["validation_ok", "validation_reason"])
        except Exception as e:
            log_analytics_event("sql_validation_error", user_id, question, llm_prompt=prompt, sql_query=sql_query, error=str(e))
            run.status = "sql_validation_error"
            run.completed_at = timezone.now()
            run.save(update_fields=["status", "completed_at"])
            return Response({"error": "Internal SQL validation error."}, status=500)

        # 6. Execute SQL (save execution_ok and final_result preview)
        try:
            results, exec_error = execute_safe_sql(sql_query)
            if exec_error:
                log_analytics_event("sql_execution_failed", user_id, question, llm_prompt=prompt, sql_query=sql_query, error=exec_error)
                run.execution_ok = False
                run.status = "exec_error"
                run.completed_at = timezone.now()
                run.save(update_fields=["execution_ok", "status", "completed_at"])
                return Response({"error": "There was an error executing your query: " + exec_error}, status=400)

            log_analytics_event("sql_executed", user_id, question, llm_prompt=prompt, sql_query=sql_query, result=results)
            run.execution_ok = True
            run.final_result = preview_result(results)  # keep it light
            run.save(update_fields=["execution_ok", "final_result"])
        except Exception as e:
            log_analytics_event("sql_execution_error", user_id, question, llm_prompt=prompt, sql_query=sql_query, error=str(e))
            run.status = "sql_execution_error"
            run.completed_at = timezone.now()
            run.save(update_fields=["status", "completed_at"])
            return Response({"error": "Error executing SQL query."}, status=500)

        # 7. Format Result (no extra fields to save)
        try:
            formatted = format_results_for_table(results)
            log_analytics_event("result_formatted", user_id, question, llm_prompt=prompt, sql_query=sql_query, result=formatted)
        except Exception as e:
            log_analytics_event("result_formatting_error", user_id, question, llm_prompt=prompt, sql_query=sql_query, error=str(e))
            run.status = "result_formatting_error"
            run.completed_at = timezone.now()
            run.save(update_fields=["status", "completed_at"])
            return Response({"error": "Failed to format analytics result."}, status=500)

        # 8. Done
        run.status = "success"
        run.completed_at = timezone.now()
        run.save(update_fields=["status", "completed_at"])
        return Response(formatted)



class SupportTicketView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        count = SupportTicket.objects.filter(tenant_id=request.user.tenant_id).filter(poster__in=["paid", "in_trial"]).filter(resolution_status__not__in=["Resolved"]).count()
        return Response({"count": count}, status=status.HTTP_200_OK)

class CSEAverageResolutionTimeView(APIView):
    """
    Returns average resolution time for each CSE (Customer Support Executive) for a given date range.
    Query params: start, end, unit, tenant_id
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        # Get date parameters directly and clean them
        start_param = request.query_params.get('start', '').strip()
        end_param = request.query_params.get('end', '').strip()
        
        # Parse dates
        start_date = None
        end_date = None
        
        if start_param:
            try:
                start_date = datetime.strptime(start_param, "%Y-%m-%d").date()
            except ValueError:
                print(f"Invalid start date format: {start_param}")
                start_date = None
                
        if end_param:
            try:
                end_date = datetime.strptime(end_param, "%Y-%m-%d").date()
            except ValueError:
                print(f"Invalid end date format: {end_param}")
                end_date = None
        
        # Debug: Print the date range
        print(f"Date range: {start_date} to {end_date}")
        
        # Use Django ORM instead of raw SQL
        qs = SupportTicket.objects.filter(
            completed_at__isnull=False,
            resolution_time__isnull=False
        ).exclude(
            resolution_time=''
        ).exclude(
            cse_name__isnull=True
        ).exclude(
            cse_name=''
        )
        
        # Apply date filters if provided
        if start_date:
            qs = qs.filter(completed_at__date__gte=start_date)
        if end_date:
            qs = qs.filter(completed_at__date__lte=end_date)
        
        # Apply tenant filter if provided
        tenant_id = request.query_params.get('tenant_id')
        if tenant_id:
            qs = qs.filter(tenant_id=tenant_id)
        
        # Create a custom function to convert MM:SS to seconds
        class TimeToSeconds(Func):
            function = 'CAST'
            template = "CAST(SPLIT_PART(%(expressions)s, ':', 1) AS INTEGER) * 60 + CAST(SPLIT_PART(%(expressions)s, ':', 2) AS INTEGER)"
            output_field = IntegerField()
        
        # Annotate with resolution time in seconds using Django ORM
        qs = qs.annotate(
            resolution_seconds=TimeToSeconds('resolution_time')
        ).values('cse_name').annotate(
            avg_resolution_seconds=Avg('resolution_seconds'),
            ticket_count=Count('id')
        ).order_by('cse_name')
        
        # Debug: Print the number of results
        print(f"Found {qs.count()} CSEs with data")
        
        unit = request.query_params.get('unit', 'minutes').lower()
        
        result = []
        for item in qs:
            if item['avg_resolution_seconds'] is not None:
                avg_time = convert_seconds(item['avg_resolution_seconds'], unit)
                result.append({
                    'cse_name': item['cse_name'],
                    'average_resolution_time': round(avg_time, 2),
                    'ticket_count': item['ticket_count'],
                    'unit': unit
                })
        
        return Response({"data": result}, status=status.HTTP_200_OK)




class SupportTicketListView(ListAPIView):
    serializer_class = SupportTicketSerializer
    permission_classes = [IsTenantAuthenticated]
    pagination_class = MetaPageNumberPagination
    filter_backends = [SafeOrderingFilter] 
    ordering = "-created_at"
   
    search_fields = ["name", "phone", "user_id"]

    def get_queryset(self):
        qs = SupportTicket.objects.filter(tenant_id=self.request.tenant.id)
        qp = self.request.query_params
        raw_term = (qp.get("search_fields")or "").strip()
        if raw_term:
            digits = "".join(ch for ch in raw_term if ch.isdigit())
            if digits:
                q = (
                    Q(phone__icontains=digits) | 
                    Q(user_id__icontains=digits)
                )    
                qs = qs.filter(q)
            else:
                qs = qs.filter(Q(name__icontains=raw_term))

        res_vals = get_multi_values(qp, "resolution_status", "resolution_status__in")
        if res_vals:
            qs = qs.filter(build_nullable_in_q("resolution_status", res_vals, allowed=RESOLUTION_CHOICES))

        poster_vals = get_multi_values(qp, "poster", "poster__in")
        if poster_vals:
            include_null = any(v.lower() == "null" for v in poster_vals)
            vals = [v for v in poster_vals if v.lower() != "null"]
            bad = [v for v in vals if v not in POSTER_CHOICES]
            if bad:
                raise ValidationError({"poster": f"Invalid values: {bad}"})
            q = Q()
            if vals: q |= Q(poster__in=vals)
            if include_null: q |= Q(poster__isnull=True)
            qs = qs.filter(q)

        assigned_vals = get_multi_values(qp, "assigned_to", "assigned_to__in")
        if assigned_vals:
            include_null = any(v.lower() == "null" for v in assigned_vals)
            ids = [v for v in assigned_vals if v.lower() != "null"]
            q = Q()
            if ids: q |= Q(assigned_to__in=ids)
            if include_null: q |= Q(assigned_to__isnull=True)
            qs = qs.filter(q)

        gte = qp.get("created_at__gte")
        lte = qp.get("created_at__lte")
        if gte: qs = qs.filter(created_at__gte=gte)
        if lte: qs = qs.filter(created_at__lte=lte)

        return qs.select_related(None).only(
            "id","created_at","ticket_date","user_id","name","phone","source",
            "subscription_status","atleast_paid_once","reason","other_reasons",
            "badge","poster","tenant_id","assigned_to","layout_status",
            "resolution_status","resolution_time","cse_name","cse_remarks",
            "call_status","call_attempts","rm_name","completed_at","snooze_until",
            "praja_dashboard_user_link","display_pic_url","dumped_at"
        )

class SupportTicketFilterOptionsView(APIView):
    permission_classes = [IsTenantAuthenticated]
    def get(self, request):
        tenant = request.tenant
        qs = SupportTicket.objects.filter(tenant_id=tenant.id)
        resolution_statuses = _distinct_list(qs, "resolution_status")
        poster_statuses = _distinct_list(qs, "poster")
        return Response({
            "resolution_statuses": resolution_statuses,
            "poster_statuses": poster_statuses,
        }, status=status.HTTP_200_OK)


class GetTicketStatusView(APIView):
    """
    API endpoint to get ticket status statistics for the current user.
    Returns various ticket counts including resolved today, pending, WIP, etc.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            # Get current user info
            user = request.user
            user_email = getattr(user, 'email', '')
            user_supabase_uid = getattr(user, 'supabase_uid', None)
            
            logger.info(f"User supabase_uid: {user_supabase_uid}")
            
            if not user_supabase_uid:
                return Response({
                    "error": "User supabase_uid not found"
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Get today's date range (start and end of day)
            today = timezone.now().date()
            start_of_day = timezone.make_aware(datetime.combine(today, time.min))
            end_of_day = timezone.make_aware(datetime.combine(today, time.max))
            
            # 1. Resolved By You Today - For the current CSE
            # Use assigned_to field with supabase_uid
            resolved_today_count = SupportTicket.objects.filter(
                assigned_to=user_supabase_uid,
                resolution_status='Resolved',
                completed_at__gte=start_of_day,
                completed_at__lte=end_of_day
            ).count()
            
            # 2. Total Pending Tickets (Overall. Not specific to this CSE)
            total_pending_count = SupportTicket.objects.filter(
                resolution_status__isnull=True
            ).count()
            
            # 2.5. Pending Tickets Breakdown by Poster
            # First get distinct poster values for pending tickets
            distinct_posters = SupportTicket.objects.filter(
                resolution_status__isnull=True,
                poster__isnull=False
            ).values_list('poster', flat=True).distinct()

            # Then count for each poster
            pending_by_poster_array = []
            for poster in distinct_posters:
                count = SupportTicket.objects.filter(
                    resolution_status__isnull=True,
                    poster=poster
                ).count()
                pending_by_poster_array.append({"poster": poster, "count": count})

            # Sort by count (descending)
            pending_by_poster_array.sort(key=lambda x: x['count'], reverse=True)
            
            # 2.6. Total Tickets (All tickets in the system)
            total_tickets_count = SupportTicket.objects.count()
            
            # 3. WIP tickets (For this CSE) - Not filtered by today
            wip_tickets_count = SupportTicket.objects.filter(
                assigned_to=user_supabase_uid,
                resolution_status='WIP'
            ).count()
            
            # 4. Can't Resolve (Today) (For this CSE)
            cant_resolve_today_count = SupportTicket.objects.filter(
                assigned_to=user_supabase_uid,
                resolution_status="Can't Resolve",
                completed_at__gte=start_of_day,
                completed_at__lte=end_of_day
            ).count()
            
            # Prepare response
            ticket_stats = {
                "resolvedByYouToday": resolved_today_count,
                "totalPendingTickets": total_pending_count,
                "pendingByPoster": pending_by_poster_array,
                "totalTickets": total_tickets_count,
                "wipTickets": wip_tickets_count,
                "cantResolveToday": cant_resolve_today_count
            }
            
            return Response({
                "success": True,
                "ticketStats": ticket_stats,
                "dateRange": {
                    "startOfDay": start_of_day.isoformat(),
                    "endOfDay": end_of_day.isoformat()
                }
            }, status=status.HTTP_200_OK)
            
        except Exception as error:
            logger.error('Error in get-ticket-status function: %s', error)
            return Response({
                "error": "Internal server error"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




class GetCseStatsView(APIView):

    """
    Returns CSE statistics for tickets under a specific tenant slug.
    Uses X-Tenant-Slug header to identify the tenant and returns stats for all CSEs under that tenant.
    
    Response format:
    - List of CSEs with their ticket statistics (resolved, not-connected, not-resolved, call-later)
    
    Headers:
    - X-Tenant-Slug: Required tenant slug to identify which tenant's CSEs to analyze
    
    Query params:
    - start: Start date (YYYY-MM-DD) - defaults to 7 days ago  
    - end: End date (YYYY-MM-DD) - defaults to today
    - cse_name: Filter by specific CSE name (optional)
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        from datetime import datetime, timedelta
        from django.db.models import Count, Case, When, IntegerField, Q
        from django.db.models.functions import TruncDate
        from django.db import models
        
        # Check if tenant is resolved from X-Tenant-Slug header
        if not hasattr(request, 'tenant') or not request.tenant:
            return Response(
                {"error": "X-Tenant-Slug header is required"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        tenant = request.tenant
        
        # Parse date parameters
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)  # Default to last 7 days
        
        start_param = request.query_params.get('start', '').strip()
        end_param = request.query_params.get('end', '').strip()
        
        if start_param:
            try:
                start_date = datetime.strptime(start_param, "%Y-%m-%d").date()
            except ValueError:
                return Response(
                    {"error": "Invalid start date format. Use YYYY-MM-DD"}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
                
        if end_param:
            try:
                end_date = datetime.strptime(end_param, "%Y-%m-%d").date()
            except ValueError:
                return Response(
                    {"error": "Invalid end date format. Use YYYY-MM-DD"}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        # Validate date range
        if start_date > end_date:
            return Response(
                {"error": "Start date cannot be after end date"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Base queryset - filter for CSE activity under this tenant
        qs = SupportTicket.objects.filter(
            tenant_id=tenant.id,      # Filter by tenant from X-Tenant-Slug
            cse_name__isnull=False,   # Only tickets assigned to a CSE
        ).exclude(cse_name='')
        
        # Apply date filter based on CSE activity date
        # For resolved/not-resolved tickets, use completed_at (when CSE completed work)
        # For other tickets, use dumped_at (when ticket was assigned/created)
        date_filter = Q()
        
        # Tickets completed by CSE in date range
        date_filter |= Q(
            completed_at__isnull=False,
            completed_at__date__gte=start_date,
            completed_at__date__lte=end_date
        )
        
        # Active/pending tickets assigned to CSE (use dumped_at for assignment date)
        date_filter |= Q(
            completed_at__isnull=True,
            dumped_at__isnull=False,
            dumped_at__date__gte=start_date,
            dumped_at__date__lte=end_date
        )
        
        qs = qs.filter(date_filter)
        
        # Apply optional CSE name filter
        cse_name_filter = request.query_params.get('cse_name', '').strip()
        if cse_name_filter:
            qs = qs.filter(cse_name__icontains=cse_name_filter)
        
        # Aggregate data by CSE (no daily breakdown, just totals per CSE)
        stats = (
            qs.values('cse_name')
            .annotate(
                resolved=Count(
                    Case(
                        When(resolution_status__iexact='resolved', then=1),
                        output_field=IntegerField()
                    )
                ),
                not_resolved=Count(
                    Case(
                        When(resolution_status__iexact="can't resolve", then=1),
                        output_field=IntegerField()
                    )
                ),
                wip=Count(
                    Case(
                        When(resolution_status__iexact='wip', then=1),
                        output_field=IntegerField()
                    )
                ),
                not_connected=Count(
                    Case(
                        When(call_status__icontains='not connected', then=1),
                        When(call_status__icontains='no answer', then=1),
                        When(call_status__icontains='unreachable', then=1),
                        output_field=IntegerField()
                    )
                ),
                call_later=Count(
                    Case(
                        When(call_status__icontains='call later', then=1),
                        When(call_status__icontains='callback', then=1),
                        When(snooze_until__isnull=False, then=1),
                        output_field=IntegerField()
                    )
                ),
                total_tickets=Count('id')
            )
            .order_by('cse_name')
        )
        
        # Format response data - simple list of CSEs with their stats
        cse_list = []
        for stat in stats:
            cse_list.append({
                'cse_name': stat['cse_name'],
                'resolved': stat['resolved'],
                'not_connected': stat['not_connected'],
                'not_resolved': stat['not_resolved'],
                'call_later': stat['call_later'],
                'wip': stat['wip'],
                'total_tickets': stat['total_tickets']
            })

            
        
        return Response({
            'tenant_slug': tenant.slug,
            'tenant_name': tenant.name,
            'date_range': {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
            },
            'cse_stats': cse_list,
            'total_cses': len(cse_list)
        }, status=status.HTTP_200_OK)
