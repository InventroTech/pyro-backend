from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from scheduler.models import ScheduledTask, TaskStatus

class Command(BaseCommand):
    help = "Lightweight scheduler loop that locks due tasks."

    def add_arguments(self, parser):
        parser.add_argument("--worker", default="worker-1")
        parser.add_argument("--batch", type=int, default=50)
        parser.add_argument("--sleep", type=float, default=5.0)

    def handle(self, *args, **opts):
        import time
        worker = opts["worker"]
        batch = opts["batch"]
        sleep_s = opts["sleep"]

        self.stdout.write(self.style.SUCCESS(f"Scheduler running as {worker}"))
        while True:
            picked = 0
            with transaction.atomic():
                tasks = (
                    ScheduledTask.objects
                    .select_for_update(skip_locked=True)
                    .filter(status=TaskStatus.PENDING, due_at__lte=timezone.now())
                    .order_by("priority", "due_at")[:batch]
                )
                for t in tasks:
                    t.status = TaskStatus.IN_PROGRESS
                    t.locked_by = worker
                    t.locked_at = timezone.now()
                    t.save(update_fields=["status", "locked_by", "locked_at", "updated_at"])
                    picked += 1
                    # In a real system, push to a queue/webhook to tell the caller UI/agent.

            if picked == 0:
                time.sleep(sleep_s)
