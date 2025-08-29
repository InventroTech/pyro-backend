from django.core.management.base import BaseCommand
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from django.utils import timezone
from datetime import timedelta

from scheduler.models import ScheduledTask, TaskPolicy, TaskStatus
from scheduler.utils import next_due_from_policy
from crm.models import Lead

class Command(BaseCommand):
    help = "Run one scheduler tick (backfill, pick-due, requeue-stale)."

    def add_arguments(self, parser):
        parser.add_argument("--batch", type=int, default=100)
        parser.add_argument("--stale-minutes", type=int, default=15)
        parser.add_argument("--policy", default="lead.call.v1")

    def handle(self, *args, **opts):
        now = timezone.now()
        batch = opts["batch"]
        stale_minutes = opts["stale_minutes"]
        policy_key = opts["policy"]

        policy = TaskPolicy.objects.get(key=policy_key)
        lead_ct = ContentType.objects.get_for_model(Lead)

        # 1) BACKFILL: schedule first/next call for eligible leads, if no active task exists
        eligible = (
            Lead.objects
            .filter(do_not_call=False)
            .exclude(lead_status__in=("Resolved","Won","Lost","Can't Resolve","INVALID_NUMBER","EXHAUSTED"))
            .filter(next_call_at__isnull=False, next_call_at__lte=now)
            .order_by("next_call_at")[:batch]
        )

        backfilled = 0
        for lead in eligible:
            exists = ScheduledTask.objects.filter(
                content_type=lead_ct, object_id=str(lead.pk),
                action="call",
                status__in=[TaskStatus.PENDING, TaskStatus.IN_PROGRESS],
            ).exists()
            if exists or lead.attempt_count >= policy.max_attempts:
                continue
            ScheduledTask.objects.create(
                content_type=lead_ct, object_id=str(lead.pk),
                action="call", policy=policy,
                status=TaskStatus.PENDING,
                due_at=now, max_attempts=policy.max_attempts, payload={}
            )
            backfilled += 1

        # 2) PICK DUE: move due tasks to IN_PROGRESS (locks prevent races)
        picked = 0
        with transaction.atomic():
            due_tasks = (
                ScheduledTask.objects
                .select_for_update(skip_locked=True)
                .filter(status=TaskStatus.PENDING, due_at__lte=now)
                .order_by("priority", "due_at")[:batch]
            )
            for t in due_tasks:
                t.status = TaskStatus.IN_PROGRESS
                t.locked_by = "timer"
                t.locked_at = now
                t.save(update_fields=["status","locked_by","locked_at","updated_at"])
                picked += 1
                # convenience mirror for UI: "call now"
                if t.content_type_id == lead_ct.id:
                    Lead.objects.filter(pk=t.object_id).update(next_call_at=now)

        # 3) REQUEUE STALE IN_PROGRESS (> N minutes)
        stale_cutoff = now - timedelta(minutes=stale_minutes)
        requeued = (
            ScheduledTask.objects
            .filter(status=TaskStatus.IN_PROGRESS, locked_at__lt=stale_cutoff)
            .update(status=TaskStatus.PENDING, locked_by=None, locked_at=None)
        )

        self.stdout.write(self.style.SUCCESS(
            f"tick ok | backfilled={backfilled} picked_due={picked} requeued_stale={requeued}"
        ))
