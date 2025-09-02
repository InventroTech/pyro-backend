import logging
import numpy as np
from django.db.models import F, ExpressionWrapper, DurationField, Avg, Count, Q
from django.db.models.functions import TruncDate
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .models import SupportTicket
from rest_framework.permissions import IsAuthenticated, AllowAny
from datetime import datetime
from django.utils import timezone
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
    
class SupportTicketListView(ListAPIView):
    serializer_class = SupportTicketSerializer
    permission_classes = [IsAuthenticated]
    pagination_class = MetaPageNumberPagination
    filter_backends = [SafeSearchFilter, SafeOrderingFilter]
    search_fields = ["name", "phone", "user_id"] 
    ordering = "-created_at"

    def get_queryset(self):
        qs = tenant_scoped_qs(self.request.user)
        qp = self.request.query_params

        # resolution_status (multi + null)
        res_vals = get_multi_values(qp, "resolution_status", "resolution_status__in")
        if res_vals:
            qs = qs.filter(build_nullable_in_q("resolution_status", res_vals, allowed=RESOLUTION_CHOICES))

        # poster (multi)
        poster_vals = get_multi_values(qp, "poster", "poster__in")
        if poster_vals:
            # poster != null selection behaves the same as resolution
            include_null = any(v.lower() == "null" for v in poster_vals)
            vals = [v for v in poster_vals if v.lower() != "null"]
            bad = [v for v in vals if v not in POSTER_CHOICES]
            if bad:
                raise ValidationError({"poster": f"Invalid values: {bad}"})
            q = Q()
            if vals:
                q |= Q(poster__in=vals)
            if include_null:
                q |= Q(poster__isnull=True)
            qs = qs.filter(q)

        # assigned_to (multi + null) — accepts UUID strings or "null"
        assigned_vals = get_multi_values(qp, "assigned_to", "assigned_to__in")
        if assigned_vals:
            include_null = any(v.lower() == "null" for v in assigned_vals)
            uuids = [v for v in assigned_vals if v.lower() != "null"]
            q = Q()
            if uuids:
                q |= Q(assigned_to__in=uuids)
            if include_null:
                q |= Q(assigned_to__isnull=True)
            qs = qs.filter(q)

        # date range
        gte = qp.get("created_at__gte")
        lte = qp.get("created_at__lte")
        if gte:
            qs = qs.filter(created_at__gte=gte)
        if lte:
            qs = qs.filter(created_at__lte=lte)


        return qs.select_related(None).only(
            "id","created_at","ticket_date","user_id","name","phone","source",
            "subscription_status","atleast_paid_once","reason","other_reasons",
            "badge","poster","tenant_id","assigned_to","layout_status",
            "resolution_status","resolution_time","cse_name","cse_remarks",
            "call_status","call_attempts","rm_name","completed_at","snooze_until",
            "praja_dashboard_user_link","display_pic_url","dumped_at"
        )