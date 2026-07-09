import logging
import numpy as np
from django.db.models import F, ExpressionWrapper, DurationField, Avg, Count, Q, Func, IntegerField, Case, When
from django.db.models.functions import TruncDate
from django.db import connection
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from support_ticket.records import (
    TICKET_DATA_SEARCH_FIELDS,
    annotate_ticket_datetimes,
    distinct_data_values,
    extract_date_range_from_ticket_data,
    q_data_json_null,
    q_record_pending_resolution,
    q_record_unassigned,
    support_ticket_records_qs,
)
from rest_framework.permissions import IsAuthenticated, AllowAny
from datetime import datetime, time
from django.utils import timezone
from django.db.models import Count, Sum, Avg
import uuid
from .utils import (
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
    RESOLUTION_CHOICES,
    SafeSearchFilter, SafeOrderingFilter
)
from django.db import models
from django.contrib.auth import get_user_model
from authz.permissions import IsTenantAuthenticated
from config.supabase_auth import SupabaseJWTAuthentication
from crm_records.mixins import TenantScopedMixin
from .services import TeamResolver, TeamMetricsService
from .serializers import (
    TeamOverviewSerializer,
    MemberBreakdownSerializer,
    EventBreakdownSerializer,
    TimeSeriesSerializer
)


# "How many support tickets did each executive resolve last week?"
# "Which executive had the fastest average resolution time last month?"
# "Show me the number of open vs closed tickets handled by each support executive."
# "List the top 3 executives by the number of tickets resolved in the past month."
# "Which executive has the highest unresolved ticket count right now?"

# --- Config & Logging ---
logger = logging.getLogger(__name__)

class StackedBarResolvedUnresolvedView(APIView):
    """Stacked bar data for resolved/unresolved support tickets per day."""
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        qs = annotate_ticket_datetimes(
            support_ticket_records_qs(tenant=request.tenant)
        ).filter(ticket_dumped_at__isnull=False)
        start_date, end_date = extract_date_range_from_ticket_data(
            qs, request, data_field_name="dumped_at"
        )
        date_range = list(get_date_range(start_date, end_date))
        if not date_range:
            today = datetime.today().date()
            return Response([{'x': today.strftime("%Y-%m-%d"), 'y1': 0, 'y2': 0}])
        
        # Filter by date range
        qs = qs.filter(
            ticket_dumped_at__date__gte=start_date,
            ticket_dumped_at__date__lte=end_date,
        )
        
        # Aggregate resolved and unresolved counts per day in a single query
        aggregated_data = (
            qs.annotate(date=TruncDate("ticket_dumped_at"))
            .values('date')
            .annotate(
                resolved=Count(
                    'id',
                    filter=Q(
                        ticket_completed_at__isnull=False,
                        data__resolution_status__iexact='resolved',
                    ),
                ),
                unresolved=Count(
                    'id',
                    filter=~Q(
                        ticket_completed_at__isnull=False,
                        data__resolution_status__iexact='resolved',
                    ),
                ),
            )
        )
        
        # Build a map for quick lookup
        data_map = {
            entry['date']: {
                'resolved': entry['resolved'],
                'unresolved': entry['unresolved']
            }
            for entry in aggregated_data
        }
        
        # Build results for all dates in range, filling missing dates with zeros
        results = []
        for date in date_range:
            day_data = data_map.get(date, {'resolved': 0, 'unresolved': 0})
            results.append({
                'x': date.strftime("%Y-%m-%d"),
                'y1': day_data['resolved'],
                'y2': day_data['unresolved']
            })
        
        return Response(results)

class DailyPercentileResolutionTimeView(APIView):
    """
    Returns daily Nth percentile (default 90th) of ticket resolution time for a date range.
    Query params: percentile, unit, start, end, tenant_id
    """
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        logger.info("DailyPercentileResolutionTimeView called with query_params: %s", request.query_params)
        
        try:
            percentile = float(request.query_params.get('percentile', 90))
        except ValueError:
            return Response(
                {"detail": "Percentile must be a number."},
                status=status.HTTP_400_BAD_REQUEST
            )
        unit = request.query_params.get('unit', 'minutes').lower()

        qs = annotate_ticket_datetimes(
            support_ticket_records_qs(tenant=request.tenant)
        ).filter(
            ticket_completed_at__isnull=False,
            ticket_dumped_at__isnull=False,
            data__resolution_status__in=['Resolved', "Can't Resolve"],
        )
        if not qs.exists():
            today = datetime.today().date()
            logger.warning("No support tickets found for given filters. Returning today's date with y=0.")
            return Response([{"x": today.strftime("%Y-%m-%d"), "y": 0}])

        start_date, end_date = extract_date_range_from_ticket_data(
            qs, request, data_field_name="completed_at"
        )
        qs = qs.filter(
            ticket_completed_at__date__gte=start_date,
            ticket_completed_at__date__lte=end_date,
        )
        qs = qs.annotate(
            resolved_date=TruncDate("ticket_completed_at"),
            resolution_time=F("data__resolution_time"),
        )

        # Filter out tickets without resolution_time
        qs = qs.exclude(data__resolution_time__isnull=True).exclude(data__resolution_time="")
        
        data_by_day = {}
        for ticket in qs:
            try:
                day = ticket.resolved_date
                # Convert MM:SS format to seconds
                if ticket.resolution_time and ':' in ticket.resolution_time:
                    time_parts = ticket.resolution_time.split(':')
                    if len(time_parts) == 2:
                        minutes = int(time_parts[0])
                        seconds = int(time_parts[1])
                        res_time_seconds = minutes * 60 + seconds
                        data_by_day.setdefault(day, []).append(res_time_seconds)
            except (ValueError, IndexError) as e:
                logger.warning("Failed to parse resolution time '%s' for ticket %s: %s", 
                             getattr(ticket, 'resolution_time', None), getattr(ticket, 'id', None), e)
            except Exception as e:
                logger.warning("Failed to process resolution time for ticket %s: %s", getattr(ticket, 'id', None), e)

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

class DailyAverageResolutionTimeView(APIView):
    """
    Returns daily average of ticket resolution time for a date range.
    Query params: unit, start, end, tenant_id
    """
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        logger.info("DailyAverageResolutionTimeView called with query_params: %s", request.query_params)

        unit = request.query_params.get('unit', 'minutes').lower()

        qs = annotate_ticket_datetimes(
            support_ticket_records_qs(tenant=request.tenant)
        ).filter(
            ticket_completed_at__isnull=False,
            ticket_dumped_at__isnull=False,
            data__resolution_status__in=['Resolved', "Can't Resolve"],
        )
        if not qs.exists():
            today = datetime.today().date()
            logger.warning("No support tickets found for given filters. Returning today's date with y=0.")
            return Response([{"x": today.strftime("%Y-%m-%d"), "y": 0}])

        start_date, end_date = extract_date_range_from_ticket_data(
            qs, request, data_field_name="completed_at"
        )
        qs = qs.filter(
            ticket_completed_at__date__gte=start_date,
            ticket_completed_at__date__lte=end_date,
        )
        qs = qs.annotate(
            resolved_date=TruncDate("ticket_completed_at"),
            resolution_time=F("data__resolution_time"),
        )

        # Filter out tickets without resolution_time
        qs = qs.exclude(data__resolution_time__isnull=True).exclude(data__resolution_time="")

        data_by_day = {}
        for ticket in qs:
            try:
                day = ticket.resolved_date
                # Convert MM:SS format to seconds
                if ticket.resolution_time and ':' in ticket.resolution_time:
                    time_parts = ticket.resolution_time.split(':')
                    if len(time_parts) == 2:
                        minutes = int(time_parts[0])
                        seconds = int(time_parts[1])
                        res_time_seconds = minutes * 60 + seconds
                        data_by_day.setdefault(day, []).append(res_time_seconds)
            except (ValueError, IndexError) as e:
                logger.warning("Failed to parse resolution time '%s' for ticket %s: %s",
                             getattr(ticket, 'resolution_time', None), getattr(ticket, 'id', None), e)
            except Exception as e:
                logger.warning("Failed to process resolution time for ticket %s: %s", getattr(ticket, 'id', None), e)

        result = []
        for date in get_date_range(start_date, end_date):
            times = data_by_day.get(date, [])
            y = 0
            if times:
                try:
                    avg_val = float(np.mean(times))
                    y = convert_seconds(avg_val, unit)
                except Exception as e:
                    logger.warning("Average calculation failed on %s: %s", date, e)
            result.append({"x": date.strftime("%Y-%m-%d"), "y": y})
        logger.info("Returning %d data points", len(result))
        return Response(result)
class DailyResolvedTicketsView(APIView):
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        qs = annotate_ticket_datetimes(
            support_ticket_records_qs(tenant=request.tenant)
        ).filter(ticket_completed_at__isnull=False)
        start_date, end_date = extract_date_range_from_ticket_data(
            qs, request, data_field_name="completed_at"
        )
        if not start_date or not end_date:
            today = datetime.today().date()
            return Response([{"x": today.strftime("%Y-%m-%d"), "y": 0}])

        qs = qs.filter(
            ticket_completed_at__date__gte=start_date,
            ticket_completed_at__date__lte=end_date,
        )
        resolved_data = (
            qs.annotate(date=TruncDate("ticket_completed_at"))
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
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        qs = annotate_ticket_datetimes(
            support_ticket_records_qs(tenant=request.tenant)
        ).filter(
            ticket_completed_at__isnull=False,
            ticket_dumped_at__isnull=False,
        )
        
        start_date, end_date = extract_date_range_from_ticket_data(
            qs, request, data_field_name="completed_at"
        )
        if not start_date or not end_date:
            today = datetime.today().date()
            return Response([{"x": today.strftime("%Y-%m-%d"), "y": 0}])

        qs = qs.filter(
            ticket_completed_at__date__gte=start_date,
            ticket_completed_at__date__lte=end_date,
        )
        qs = qs.annotate(
            closure_time=ExpressionWrapper(
                F("ticket_completed_at") - F("ticket_dumped_at"),
                output_field=DurationField(),
            ),
            day=TruncDate("ticket_completed_at"),
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
            if "resolution_time" in schema_str or "records" in schema_str:
                extra_instruction = (
                    "IMPORTANT: Support tickets live in the records table with entity_type = 'support_ticket'. "
                    "Ticket fields are in the JSONB data column (e.g. data->>'resolution_time'). "
                    "resolution_time is stored as 'MM:SS' text; convert to seconds using "
                    "(SPLIT_PART(data->>'resolution_time', ':', 1)::int * 60 + "
                    "SPLIT_PART(data->>'resolution_time', ':', 2)::int). "
                    "Datetime fields in data are ISO strings — cast with (data->>'completed_at')::timestamptz."
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
    "A: SELECT data->>'cse_name' AS cse_name, COUNT(*) AS tickets_resolved "
    "FROM records "
    "WHERE entity_type = 'support_ticket' "
    "  AND data->>'resolution_status' = 'Resolved' "
    "  AND (data->>'completed_at')::timestamptz >= %(start)s "
    "  AND (data->>'completed_at')::timestamptz < %(end)s "
    "GROUP BY data->>'cse_name' "
    "ORDER BY tickets_resolved DESC "
    "LIMIT 5;\n"
    "\n"
    "Example:\n"
    "Q: How many tickets remain unresolved as of today?\n"
    "A: SELECT COUNT(*) AS unresolved_tickets "
    "FROM records "
    "WHERE entity_type = 'support_ticket' "
    "  AND data->>'resolution_status' != 'Resolved' "
    "  AND (data->>'dumped_at')::timestamptz <= %(today)s;\n"
    "\n"
    "Example:\n"
    "Q: What is the average resolution time (in seconds) for resolved tickets for each agent?\n"
    "A: SELECT data->>'cse_name' AS cse_name, "
    "AVG(CASE WHEN data->>'resolution_status' = 'Resolved' THEN "
    "(SPLIT_PART(data->>'resolution_time', ':', 1)::int * 60 + "
    "SPLIT_PART(data->>'resolution_time', ':', 2)::int) END) "
    "AS avg_resolution_time_seconds "
    "FROM records "
    "WHERE entity_type = 'support_ticket' "
    "GROUP BY data->>'cse_name';\n"
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
        allowed_tables = {"records"}
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
        count = (
            support_ticket_records_qs(tenant_id=request.user.tenant_id)
            .filter(data__poster__in=["paid", "in_trial"])
            .exclude(data__resolution_status__in=["Resolved"])
            .count()
        )
        return Response({"count": count}, status=status.HTTP_200_OK)

class CSEAverageResolutionTimeView(APIView):
    """
    Returns average resolution time for each CSE (Customer Support Executive) for a given date range.
    Query params: start, end, unit
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

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
        
        qs = annotate_ticket_datetimes(
            support_ticket_records_qs(tenant=request.tenant)
        ).filter(
            ticket_completed_at__isnull=False,
        ).exclude(data__resolution_time__isnull=True).exclude(
            data__resolution_time=""
        ).exclude(data__cse_name__isnull=True).exclude(data__cse_name="")
        
        if start_date:
            qs = qs.filter(ticket_completed_at__date__gte=start_date)
        if end_date:
            qs = qs.filter(ticket_completed_at__date__lte=end_date)
        
        class TimeToSeconds(Func):
            function = 'CAST'
            template = (
                "CAST(SPLIT_PART(%(expressions)s, ':', 1) AS INTEGER) * 60 + "
                "CAST(SPLIT_PART(%(expressions)s, ':', 2) AS INTEGER)"
            )
            output_field = IntegerField()
        
        qs = qs.annotate(
            resolution_seconds=TimeToSeconds("data__resolution_time"),
        ).values("data__cse_name").annotate(
            avg_resolution_seconds=Avg("resolution_seconds"),
            ticket_count=Count("id"),
        ).order_by("data__cse_name")
        
        # Debug: Print the number of results
        print(f"Found {qs.count()} CSEs with data")
        
        unit = request.query_params.get('unit', 'minutes').lower()
        
        result = []
        for item in qs:
            if item['avg_resolution_seconds'] is not None:
                avg_time = convert_seconds(item['avg_resolution_seconds'], unit)
                result.append({
                    'cse_name': item['data__cse_name'],
                    'average_resolution_time': round(avg_time, 2),
                    'ticket_count': item['ticket_count'],
                    'unit': unit
                })
        
        return Response({"data": result}, status=status.HTTP_200_OK)


class SLATimeView(APIView):
    """
    Returns average SLA (Service Level Agreement) time for Non-Snoozed and Snoozed tickets separately.
    SLA time is calculated as the time from ticket creation to resolution (completed_at - created_at).
    This helps track first contact resolution time.
    Query params: start, end, unit
    """
    permission_classes = [IsTenantAuthenticated]

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
        
        qs = annotate_ticket_datetimes(
            support_ticket_records_qs(tenant=request.tenant)
        ).filter(
            ticket_completed_at__isnull=False,
        )
        
        if start_date:
            qs = qs.filter(ticket_completed_at__date__gte=start_date)
        if end_date:
            qs = qs.filter(ticket_completed_at__date__lte=end_date)
        
        qs_with_sla = qs.annotate(
            sla_seconds=ExpressionWrapper(
                F("ticket_completed_at") - F("created_at"),
                output_field=DurationField(),
            )
        )
        
        non_snoozed_qs = qs_with_sla.filter(ticket_snooze_until__isnull=True)
        snoozed_qs = qs_with_sla.filter(ticket_snooze_until__isnull=False)
        
        # Calculate average SLA time for Non-Snoozed tickets
        non_snoozed_avg = non_snoozed_qs.aggregate(
            avg_sla=Avg('sla_seconds'),
            ticket_count=Count('id')
        )
        
        # Calculate average SLA time for Snoozed tickets
        snoozed_avg = snoozed_qs.aggregate(
            avg_sla=Avg('sla_seconds'),
            ticket_count=Count('id')
        )
        
        # Get unit parameter (default: minutes)
        unit = request.query_params.get('unit', 'minutes').lower()
        
        # Convert timedelta to requested unit
        result = {
            'non_snoozed': {
                'average_sla_time': None,
                'ticket_count': non_snoozed_avg['ticket_count'] or 0,
                'unit': unit
            },
            'snoozed': {
                'average_sla_time': None,
                'ticket_count': snoozed_avg['ticket_count'] or 0,
                'unit': unit
            }
        }
        
        # Convert Non-Snoozed average SLA time
        if non_snoozed_avg['avg_sla'] is not None:
            avg_seconds = non_snoozed_avg['avg_sla'].total_seconds()
            result['non_snoozed']['average_sla_time'] = round(convert_seconds(avg_seconds, unit), 2)
        
        # Convert Snoozed average SLA time
        if snoozed_avg['avg_sla'] is not None:
            avg_seconds = snoozed_avg['avg_sla'].total_seconds()
            result['snoozed']['average_sla_time'] = round(convert_seconds(avg_seconds, unit), 2)
        
        return Response(result, status=status.HTTP_200_OK)


class SupportTicketListView(ListAPIView):
    serializer_class = SupportTicketSerializer
    permission_classes = [IsTenantAuthenticated]
    pagination_class = MetaPageNumberPagination
    filter_backends = [SafeOrderingFilter]
    ordering = "-created_at"

    def get_queryset(self):
        qs = support_ticket_records_qs(tenant=self.request.tenant)
        qp = self.request.query_params

        search_term = (qp.get("search") or "").strip()
        if search_term:
            raw_fields = (qp.get("search_fields") or "").strip()
            if raw_fields:
                field_list = [f.strip() for f in raw_fields.split(",") if f.strip()]
            else:
                field_list = list(TICKET_DATA_SEARCH_FIELDS)

            allowed_search_fields = frozenset(
                TICKET_DATA_SEARCH_FIELDS + ("layout_status", "state", "call_status", "rm_name")
            )
            q_search = Q()
            for field in field_list:
                if field not in allowed_search_fields:
                    continue
                path = f"data__{field}"
                if field == "phone":
                    digits = "".join(ch for ch in search_term if ch.isdigit())
                    if digits:
                        q_search |= Q(**{f"{path}__icontains": digits}) | Q(**{f"{path}__icontains": search_term})
                    else:
                        q_search |= Q(**{f"{path}__icontains": search_term})
                else:
                    q_search |= Q(**{f"{path}__icontains": search_term})
            if not q_search.children:
                field_list = list(TICKET_DATA_SEARCH_FIELDS)
                q_search = Q()
                for field in field_list:
                    path = f"data__{field}"
                    if field == "phone":
                        digits = "".join(ch for ch in search_term if ch.isdigit())
                        if digits:
                            q_search |= Q(**{f"{path}__icontains": digits}) | Q(**{f"{path}__icontains": search_term})
                        else:
                            q_search |= Q(**{f"{path}__icontains": search_term})
                    else:
                        q_search |= Q(**{f"{path}__icontains": search_term})
            qs = qs.filter(q_search)

        res_vals = get_multi_values(qp, "resolution_status", "resolution_status__in")
        if res_vals:
            qs = qs.filter(
                build_nullable_in_q("data__resolution_status", res_vals, allowed=RESOLUTION_CHOICES)
            )

        poster_vals = get_multi_values(qp, "poster", "poster__in")
        if poster_vals:
            include_null = any(v.lower() == "null" for v in poster_vals)
            vals = [v for v in poster_vals if v.lower() != "null"]
            q = Q()
            if vals:
                q |= Q(data__poster__in=vals)
            if include_null:
                q |= q_data_json_null("poster")
            qs = qs.filter(q)

        assigned_vals = get_multi_values(qp, "assigned_to", "assigned_to__in")
        if assigned_vals:
            include_null = any(v.lower() == "null" for v in assigned_vals)
            ids = [v for v in assigned_vals if v.lower() != "null"]
            q = Q()
            if ids:
                q |= Q(data__assigned_to__in=ids)
            if include_null:
                q |= q_record_unassigned()
            qs = qs.filter(q)

        state_vals = get_multi_values(qp, "state", "state__in")
        if state_vals:
            include_null = any(v.lower() == "null" for v in state_vals)
            vals = [v for v in state_vals if v.lower() != "null"]
            q = Q()
            if vals:
                q |= Q(data__state__in=vals)
            if include_null:
                q |= q_data_json_null("state") | Q(data__state="")
            qs = qs.filter(q)

        ca_vals = get_multi_values(qp, "call_attempts", "call_attempts__in")
        if ca_vals:
            ints = []
            for v in ca_vals:
                try:
                    ints.append(int(v))
                except ValueError:
                    raise ValidationError({"call_attempts": f"Invalid integer: {v!r}"})
            if ints:
                qs = qs.filter(data__call_attempts__in=ints)

        gte = qp.get("created_at__gte")
        lte = qp.get("created_at__lte")
        if gte:
            qs = qs.filter(created_at__gte=gte)
        if lte:
            qs = qs.filter(created_at__lte=lte)

        return qs

class SupportTicketFilterOptionsView(APIView):
    permission_classes = [IsTenantAuthenticated]
    def get(self, request):
        tenant = request.tenant
        qs = support_ticket_records_qs(tenant=tenant)
        resolution_statuses = distinct_data_values(qs, "resolution_status")
        poster_statuses = distinct_data_values(qs, "poster")
        return Response({
            "resolution_statuses": resolution_statuses,
            "poster_statuses": poster_statuses,
        }, status=status.HTTP_200_OK)

class GetTicketStatusView(APIView):
    """
    API endpoint to get ticket status statistics for the current user.
    Returns various ticket counts including resolved today, pending, WIP, etc.
    """
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        try:
            # Get current user info
            user = request.user
            user_email = getattr(user, "email", "")
            user_supabase_uid = getattr(user, "supabase_uid", None)

            logger.info("GetTicketStatusView called", extra={
                "event": "get_ticket_status_start",
                "user_email": user_email,
                "user_supabase_uid": user_supabase_uid,
            })

            if not user_supabase_uid:
                return Response(
                    {"error": "User supabase_uid not found"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Today's date range (start and end of local day)
            today = timezone.now().date()
            start_of_day = timezone.make_aware(datetime.combine(today, time.min))
            end_of_day = timezone.make_aware(datetime.combine(today, time.max))


            user_id = str(user_supabase_uid)
            ticket_qs = annotate_ticket_datetimes(
                support_ticket_records_qs(tenant=request.tenant)
            )

            agg = ticket_qs.aggregate(
                resolved_today=Count(
                    "id",
                    filter=Q(
                        data__assigned_to=user_id,
                        data__resolution_status="Resolved",
                        ticket_completed_at__gte=start_of_day,
                        ticket_completed_at__lte=end_of_day,
                    ),
                ),
                total_pending=Count(
                    "id",
                    filter=(
                        q_record_pending_resolution() & q_record_unassigned()
                        | Q(
                            data__resolution_status="Snoozed",
                            data__assigned_to=user_id,
                        )
                    ),
                ),
                total_tickets=Count("id"),
                wip=Count(
                    "id",
                    filter=Q(
                        data__assigned_to=user_id,
                        data__resolution_status="WIP",
                    ),
                ),
                cant_resolve_today=Count(
                    "id",
                    filter=Q(
                        data__assigned_to=user_id,
                        data__resolution_status="Can't Resolve",
                        ticket_completed_at__gte=start_of_day,
                        ticket_completed_at__lte=end_of_day,
                    ),
                ),
            )

            pending_by_poster_array = [
                {"poster": row["data__poster"], "count": row["count"]}
                for row in (
                    support_ticket_records_qs(tenant=request.tenant)
                    .filter(q_record_pending_resolution())
                    .filter(q_record_unassigned())
                    .exclude(q_data_json_null("poster"))
                    .values("data__poster")
                    .annotate(count=Count("id"))
                    .order_by("-count")
                )
            ]
            
            # Prepare response
            ticket_stats = {
                "resolvedByYouToday": agg["resolved_today"],
                "totalPendingTickets": agg["total_pending"],
                "pendingByPoster": pending_by_poster_array,
                "totalTickets": agg["total_tickets"],
                "wipTickets": agg["wip"],
                "cantResolveToday": agg["cant_resolve_today"],
            }

            return Response(
                {
                    "success": True,
                    "ticketStats": ticket_stats,
                    "dateRange": {
                        "startOfDay": start_of_day.isoformat(),
                        "endOfDay": end_of_day.isoformat(),
                    },
                },
                status=status.HTTP_200_OK,
            )

        except Exception as error:
            logger.exception("Error in get-ticket-status function: %s", error)
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
        
        qs = annotate_ticket_datetimes(
            support_ticket_records_qs(tenant=tenant)
        ).exclude(data__cse_name__isnull=True).exclude(data__cse_name="")
        
        date_filter = Q(
            ticket_completed_at__isnull=False,
            ticket_completed_at__date__gte=start_date,
            ticket_completed_at__date__lte=end_date,
        ) | Q(
            ticket_completed_at__isnull=True,
            ticket_dumped_at__isnull=False,
            ticket_dumped_at__date__gte=start_date,
            ticket_dumped_at__date__lte=end_date,
        )
        qs = qs.filter(date_filter)
        
        cse_name_filter = request.query_params.get('cse_name', '').strip()
        if cse_name_filter:
            qs = qs.filter(data__cse_name__icontains=cse_name_filter)
        
        stats = (
            qs.values("data__cse_name")
            .annotate(
                resolved=Count(
                    Case(
                        When(data__resolution_status__iexact="resolved", then=1),
                        output_field=IntegerField(),
                    )
                ),
                not_resolved=Count(
                    Case(
                        When(data__resolution_status__iexact="can't resolve", then=1),
                        output_field=IntegerField(),
                    )
                ),
                wip=Count(
                    Case(
                        When(data__resolution_status__iexact="wip", then=1),
                        output_field=IntegerField(),
                    )
                ),
                not_connected=Count(
                    Case(
                        When(data__call_status__icontains="not connected", then=1),
                        When(data__call_status__icontains="no answer", then=1),
                        When(data__call_status__icontains="unreachable", then=1),
                        output_field=IntegerField(),
                    )
                ),
                call_later=Count(
                    Case(
                        When(data__call_status__icontains="call later", then=1),
                        When(data__call_status__icontains="callback", then=1),
                        When(ticket_snooze_until__isnull=False, then=1),
                        output_field=IntegerField(),
                    )
                ),
                total_tickets=Count("id"),
            )
            .order_by("data__cse_name")
        )
        
        cse_list = []
        for stat in stats:
            cse_list.append({
                'cse_name': stat['data__cse_name'],
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


# Team Analytics Views
class TeamOverviewView(TenantScopedMixin, APIView):
    """
    Get team overview metrics for a specific date.
    Query params: date (YYYY-MM-DD)
    """
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        date_param = request.query_params.get('date', '').strip()
        
        if not date_param:
            return Response(
                {"error": "date parameter is required (YYYY-MM-DD)"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            target_date = datetime.strptime(date_param, "%Y-%m-%d").date()
        except ValueError:
            return Response(
                {"error": "Invalid date format. Use YYYY-MM-DD"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        user_id = getattr(request.user, "supabase_uid", None) or getattr(request.user, "id", None)
        if not user_id:
            return Response(
                {"error": "User ID not found"},
                status=status.HTTP_400_BAD_REQUEST
            )

        team_user_ids = TeamResolver.get_team_user_ids(str(user_id), request.tenant)
        metrics_service = TeamMetricsService(team_user_ids, request.tenant)
        overview = metrics_service.get_overview(target_date, manager_user_id=str(user_id))

        serializer = TeamOverviewSerializer(overview)
        return Response(serializer.data, status=status.HTTP_200_OK)


class TeamMembersView(TenantScopedMixin, APIView):
    """
    Get per-member metrics breakdown for a specific date or date range.
    Query params: date (YYYY-MM-DD) or from (YYYY-MM-DD) and to (YYYY-MM-DD)
    """
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        logger = logging.getLogger(__name__)
        logger.info("=" * 80)
        logger.info("TeamMembersView.get() called")
        logger.info(f"Request user: {request.user}")
        logger.info(f"Request tenant: {request.tenant} (id: {getattr(request.tenant, 'id', None)})")
        
        date_param = request.query_params.get('date', '').strip()
        from_param = request.query_params.get('from', '').strip()
        to_param = request.query_params.get('to', '').strip()
        
        logger.info(f"Query params - date: {date_param}, from: {from_param}, to: {to_param}")
        
        start_date = None
        end_date = None
        
        if date_param:
            try:
                target_date = datetime.strptime(date_param, "%Y-%m-%d").date()
                start_date = target_date
                end_date = target_date
                logger.info(f"Parsed date: {start_date}")
            except ValueError:
                logger.error(f"Invalid date format: {date_param}")
                return Response(
                    {"error": "Invalid date format. Use YYYY-MM-DD"},
                    status=status.HTTP_400_BAD_REQUEST
                )
        elif from_param and to_param:
            try:
                start_date = datetime.strptime(from_param, "%Y-%m-%d").date()
                end_date = datetime.strptime(to_param, "%Y-%m-%d").date()
                logger.info(f"Parsed date range: {start_date} to {end_date}")
            except ValueError:
                logger.error(f"Invalid date format: from={from_param}, to={to_param}")
                return Response(
                    {"error": "Invalid date format. Use YYYY-MM-DD"},
                    status=status.HTTP_400_BAD_REQUEST
                )
        else:
            logger.error("Missing date parameters")
            return Response(
                {"error": "Either 'date' or both 'from' and 'to' parameters are required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        user_id = getattr(request.user, "supabase_uid", None) or getattr(request.user, "id", None)
        logger.info(f"Manager user_id: {user_id}")
        
        if not user_id:
            logger.error("User ID not found for manager")
            return Response(
                {"error": "User ID not found"},
                status=status.HTTP_400_BAD_REQUEST
            )

        logger.info(f"Resolving team for manager user_id: {user_id}, tenant: {request.tenant.id}")
        team_user_ids = TeamResolver.get_team_user_ids(str(user_id), request.tenant)
        logger.info(f"Team resolved - found {len(team_user_ids)} user_ids: {team_user_ids}")

        metrics_service = TeamMetricsService(team_user_ids, request.tenant)
        member_breakdown = metrics_service.get_member_breakdown(
            start_date, end_date, manager_user_id=str(user_id)
        )

        serializer = MemberBreakdownSerializer(member_breakdown, many=True)
        logger.info("=" * 80)
        return Response(serializer.data, status=status.HTTP_200_OK)


class TeamEventsView(TenantScopedMixin, APIView):
    """
    Get event type breakdown for a specific date or date range.
    Query params: date (YYYY-MM-DD) or from (YYYY-MM-DD) and to (YYYY-MM-DD)
    """
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        date_param = request.query_params.get('date', '').strip()
        from_param = request.query_params.get('from', '').strip()
        to_param = request.query_params.get('to', '').strip()
        
        start_date = None
        end_date = None
        
        if date_param:
            try:
                target_date = datetime.strptime(date_param, "%Y-%m-%d").date()
                start_date = target_date
                end_date = target_date
            except ValueError:
                return Response(
                    {"error": "Invalid date format. Use YYYY-MM-DD"},
                    status=status.HTTP_400_BAD_REQUEST
                )
        elif from_param and to_param:
            try:
                start_date = datetime.strptime(from_param, "%Y-%m-%d").date()
                end_date = datetime.strptime(to_param, "%Y-%m-%d").date()
            except ValueError:
                return Response(
                    {"error": "Invalid date format. Use YYYY-MM-DD"},
                    status=status.HTTP_400_BAD_REQUEST
                )
        else:
            return Response(
                {"error": "Either 'date' or both 'from' and 'to' parameters are required"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        user_id = getattr(request.user, "supabase_uid", None) or getattr(request.user, "id", None)
        if not user_id:
            return Response(
                {"error": "User ID not found"},
                status=status.HTTP_400_BAD_REQUEST
            )

        team_user_ids = TeamResolver.get_team_user_ids(str(user_id), request.tenant)
        metrics_service = TeamMetricsService(team_user_ids, request.tenant)
        event_breakdown = metrics_service.get_event_breakdown(start_date, end_date)

        result = [{"event_type": k, "count": v} for k, v in event_breakdown.items()]
        serializer = EventBreakdownSerializer(result, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class TeamTimeSeriesView(TenantScopedMixin, APIView):
    """
    Get daily time series data over a date range.
    Query params: from (YYYY-MM-DD) and to (YYYY-MM-DD)
    """
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        from_param = request.query_params.get('from', '').strip()
        to_param = request.query_params.get('to', '').strip()
        
        if not from_param or not to_param:
            return Response(
                {"error": "Both 'from' and 'to' parameters are required (YYYY-MM-DD)"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            start_date = datetime.strptime(from_param, "%Y-%m-%d").date()
            end_date = datetime.strptime(to_param, "%Y-%m-%d").date()
        except ValueError:
            return Response(
                {"error": "Invalid date format. Use YYYY-MM-DD"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        if start_date > end_date:
            return Response(
                {"error": "Start date cannot be after end date"},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        user_id = getattr(request.user, "supabase_uid", None) or getattr(request.user, "id", None)
        if not user_id:
            return Response(
                {"error": "User ID not found"},
                status=status.HTTP_400_BAD_REQUEST
            )

        team_user_ids = TeamResolver.get_team_user_ids(str(user_id), request.tenant)
        metrics_service = TeamMetricsService(team_user_ids, request.tenant)
        time_series = metrics_service.get_time_series(start_date, end_date)

        serializer = TimeSeriesSerializer(time_series, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class UnassignedLeadsBreakdownView(TenantScopedMixin, APIView):
    """
    Breakdown of unassigned leads by lead_source and lead_stage.
    Returns total count plus grouped counts for filtering.
    """
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        user_id = getattr(request.user, "supabase_uid", None) or getattr(request.user, "id", None)

        if not user_id:
            return Response(
                {"error": "User ID not found"},
                status=status.HTTP_400_BAD_REQUEST
            )

        lead_source_raw = request.query_params.get('lead_source', '').strip()
        lead_source_list = [s.strip() for s in lead_source_raw.split(',') if s.strip()] or None
        lead_stage = request.query_params.get('lead_stage', '').strip() or None

        team_user_ids = TeamResolver.get_team_user_ids(str(user_id), request.tenant)
        metrics_service = TeamMetricsService(team_user_ids, request.tenant)
        breakdown = metrics_service.get_unassigned_leads_breakdown(
            lead_source_filter=lead_source_list,
            lead_stage_filter=lead_stage,
        )

        return Response(breakdown, status=status.HTTP_200_OK)
