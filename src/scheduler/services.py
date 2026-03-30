from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from django.utils import timezone
from .models import ScheduledTask, TaskPolicy, TaskStatus, AttemptLog, AttemptOutcome

def schedule_call_for(obj, policy_key: str, due_at=None, payload=None):
    """
    Create/ensure a PENDING call task for a target object (any model instance).
    If an active PENDING/IN_PROGRESS call already exists → do nothing (idempotent).
    """
    ct = ContentType.objects.get_for_model(obj.__class__)
    policy = TaskPolicy.objects.get(key=policy_key)
    due_at = due_at or timezone.now()
    payload = payload or {}

    exists = ScheduledTask.objects.filter(
        content_type=ct, object_id=str(obj.pk),
        action="call",
        status__in=[TaskStatus.PENDING, TaskStatus.IN_PROGRESS],
    ).exists()
    if exists:
        return None

    return ScheduledTask.objects.create(
        content_type=ct,
        object_id=str(obj.pk),
        action="call",
        policy=policy,
        status=TaskStatus.PENDING,
        due_at=due_at,
        max_attempts=policy.max_attempts,
        payload=payload,
    )

@transaction.atomic
def record_call_outcome(task_id: int, outcome: str, notes: str = ""):
    """
    Log the attempt, update the target object (if it has attempt fields), and schedule
    the next attempt if needed.
    Returns: ('terminal' | 'rescheduled', next_due_at | None)
    """
    task = ScheduledTask.objects.select_for_update().get(id=task_id)
    obj = task.target

    # 1) log attempt
    attempt_no = task.attempts + 1
    AttemptLog.objects.create(
        task=task, attempt_no=attempt_no,
        outcome=outcome, notes=notes
    )

    # 2) mirror on resource (optional but handy)
    if hasattr(obj, "attempt_count"):
        obj.attempt_count = attempt_no
    if hasattr(obj, "last_call_outcome"):
        obj.last_call_outcome = outcome

    # 3) compute next step
    terminal = False
    now = timezone.now()

    if outcome in (AttemptOutcome.CONNECTED_POSITIVE, AttemptOutcome.CONNECTED_NEGATIVE, AttemptOutcome.INVALID_NUMBER):
        terminal = True

    elif attempt_no >= task.max_attempts and outcome not in (AttemptOutcome.CALLBACK_REQUESTED,):
        terminal = True

    if terminal:
        task.status = TaskStatus.DONE
        task.updated_at = now
        task.save(update_fields=["status", "updated_at"])

        if hasattr(obj, "next_call_at"):
            obj.next_call_at = None
        obj.save(update_fields=["attempt_count", "last_call_outcome", "next_call_at"] if hasattr(obj, "next_call_at") else ["attempt_count", "last_call_outcome"])
        return "terminal", None

    # reschedule
    task.attempts = attempt_no
    if outcome == AttemptOutcome.CALLBACK_REQUESTED:
        # expect UI to have set a requested time in payload
        when = task.payload.get("callback_at")
        due_at = when if when else now  # fallback: immediate if not provided
    else:
        due_at = __import__("scheduler.utils", fromlist=["*"]).utils.next_due_from_policy(
            now, task.policy, attempt_no - 1
        )
    task.due_at = due_at
    task.status = TaskStatus.PENDING
    task.locked_by = None
    task.locked_at = None
    task.save(update_fields=["attempts", "due_at", "status", "locked_by", "locked_at", "updated_at"])

    if hasattr(obj, "next_call_at"):
        obj.next_call_at = due_at
        obj.save(update_fields=["attempt_count", "last_call_outcome", "next_call_at"])
    else:
        obj.save(update_fields=["attempt_count", "last_call_outcome"])

    return "rescheduled", due_at
