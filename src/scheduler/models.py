from django.db import models
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.utils import timezone
from django.core.validators import MinValueValidator
from core.models import BaseModel
from object_history.models import HistoryTrackedModel



class TaskStatus(models.TextChoices):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class Action(models.TextChoices):
    CALL = "call"

class AttemptOutcome(models.TextChoices):
    NO_ANSWER = "NO_ANSWER"
    BUSY = "BUSY"
    CALL_DROPPED = "CALL_DROPPED"
    CONNECTED_POSITIVE = "CONNECTED_POSITIVE"
    CONNECTED_NEGATIVE = "CONNECTED_NEGATIVE"
    CALLBACK_REQUESTED = "CALLBACK_REQUESTED"
    INVALID_NUMBER = "INVALID_NUMBER"

class TaskPolicy(HistoryTrackedModel, BaseModel):
    """
    Declarative policy. :
    Example key='lead.call.v1', intervals=['30m','3h','10h','24h','72h'], max_attempts=5
    """
    key = models.CharField(max_length=100, unique=True)
    intervals = models.JSONField(default=list)  # list of strings like '30m','3h'
    max_attempts = models.PositiveSmallIntegerField(validators=[MinValueValidator(1)], default=5)
    business_hours_only = models.BooleanField(default=False)  # optional
    timezone = models.CharField(max_length=64, null=True, blank=True)  # optional

    def __str__(self):
        return self.key
    
class ScheduledTask(HistoryTrackedModel, BaseModel):
    """
    A scheduled action for any model instance via Generic FK.
    """
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.CharField(max_length=64)  # supports int/uuid as text
    target = GenericForeignKey("content_type", "object_id")

    action = models.CharField(max_length=50, choices=Action.choices)
    policy = models.ForeignKey(TaskPolicy, on_delete=models.PROTECT, related_name="tasks")

    status = models.CharField(max_length=20, choices=TaskStatus.choices, default=TaskStatus.PENDING)
    due_at = models.DateTimeField(db_index=True)
    priority = models.SmallIntegerField(default=0)
    attempts = models.PositiveSmallIntegerField(default=0)
    max_attempts = models.PositiveSmallIntegerField(default=5)

    # worker locks
    locked_by = models.CharField(max_length=64, null=True, blank=True)
    locked_at = models.DateTimeField(null=True, blank=True)

    # arbitrary info for the worker (e.g., phone number override or user notes)
    payload = models.JSONField(default=dict, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "due_at"]),
            models.Index(fields=["content_type", "object_id", "status"]),
        ]

class AttemptLog(models.Model):
    task = models.ForeignKey(ScheduledTask, on_delete=models.CASCADE, related_name="attempt_logs")
    attempt_no = models.PositiveSmallIntegerField()
    started_at = models.DateTimeField(auto_now_add=True)
    ended_at = models.DateTimeField(null=True, blank=True)
    outcome = models.CharField(max_length=50, choices=AttemptOutcome.choices)
    notes = models.TextField(null=True, blank=True)

