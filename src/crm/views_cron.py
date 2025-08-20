import os
from django.utils import timezone
from django.db import connection
from rest_framework.views import APIView
from rest_framework.response import Response
from .models import Lead
from .policy import MAX_ATTEMPTS

LOCK_ID = 902001

def acquire_lock():
    with connection.cursor() as c:
        c.execute("SELECT pg_try_advisory_lock(%s);", [LOCK_ID])
        return c.fetchone()[0]

def release_lock():
    with connection.cursor() as c:
        c.execute("SELECT pg_advisory_unlock(%s);", [LOCK_ID])

class CronFixLeadsView(APIView):
    # gated by header secret; no DRF auth/permissions
    authentication_classes, permission_classes = [], []

    def post(self, request):
        # simple shared-secret gate
        if request.headers.get("X-Cron-Secret") != os.environ.get("CRON_SECRET"):
            return Response({"error": "forbidden"}, status=403)

        if not acquire_lock():
            return Response({"skipped": True, "reason": "another run active"}, status=200)
        try:
            now = timezone.now()
            batch = int(request.query_params.get("batch", 2000))
            from_status = request.query_params.get("from", "WIP")
            to_pending = request.query_params.get("to_pending", "Pending")
            to_lost = request.query_params.get("to_lost", "Lost")

            # 1) do_not_call -> Lost (idempotent; skip rows already Lost)
            dnc_total = 0
            while True:
                ids = list(
                    Lead.objects
                    .filter(do_not_call=True)
                    .exclude(lead_status=to_lost)
                    .values_list("id", flat=True)[:batch]
                )
                if not ids:
                    break
                dnc_total += Lead.objects.filter(id__in=ids).update(
                    lead_status=to_lost,
                    updated_at=now
                )

            # 2) attempt_count >= MAX_ATTEMPTS -> Lost (regardless of next_call_at)
            maxed_total = 0
            while True:
                ids = list(
                    Lead.objects
                    .filter(attempt_count__gte=MAX_ATTEMPTS)
                    .exclude(lead_status=to_lost)
                    .values_list("id", flat=True)[:batch]
                )
                if not ids:
                    break
                maxed_total += Lead.objects.filter(id__in=ids).update(
                    lead_status=to_lost,
                    updated_at=now
                )

            # 3) WIP + next_call_at due -> Pending (keep assigned_to as-is)
            #    Also ensure: callable (do_not_call=False) and not already Pending.
            pend_total = 0
            while True:
                ids = list(
                    Lead.objects
                    .filter(
                        do_not_call=False,
                        lead_status=from_status,
                        next_call_at__isnull=False,
                        next_call_at__lte=now,
                        attempt_count__lt=MAX_ATTEMPTS,  # still under max attempts
                    )
                    .exclude(lead_status=to_pending)
                    .values_list("id", flat=True)
                    .order_by("next_call_at")[:batch]
                )
                if not ids:
                    break
                # assigned_to is preserved automatically since we don't touch it
                pend_total += Lead.objects.filter(id__in=ids).update(
                    lead_status=to_pending,
                    updated_at=now
                )

            return Response({
                "ok": True,
                "at": now,
                "dnc_to_lost": dnc_total,
                "max_attempts_to_lost": maxed_total,
                "wip_due_to_pending": pend_total,
            }, status=200)

        finally:
            release_lock()
