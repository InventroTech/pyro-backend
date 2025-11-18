from django.db import models
from core.models import BaseModel


class Record(BaseModel):
    """
    Universal record model that can hold any tenant's data dynamically using JSONB.
    All future entities (leads, tickets, job applications, etc.) will be built on top of this.
    """
    entity_type = models.CharField(max_length=100, db_index=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    data = models.JSONField(default=dict, blank=True)

    class Meta:
        db_table = "records"
        indexes = [
            models.Index(fields=["tenant", "entity_type", "-created_at"]),
        ]

    def __str__(self):
        return f"{self.entity_type}: {self.name or 'Unnamed'}"


class EventLog(BaseModel):
    """
    Event logging model for tracking all record-related events.
    Stores events triggered by user actions or system processes.
    """
    record = models.ForeignKey("Record", on_delete=models.CASCADE, related_name="events")
    event = models.CharField(max_length=100, db_index=True)
    payload = models.JSONField(default=dict, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "event_logs"
        indexes = [
            models.Index(fields=["tenant", "record", "event", "-timestamp"]),
        ]

    def __str__(self):
        return f"{self.event} for {self.record} at {self.timestamp}"


class RuleSet(BaseModel):
    """
    Rule configuration model for declarative event-driven workflows.
    Allows tenants to define rules that trigger actions when specific events occur.
    """
    event_name = models.CharField(max_length=100, db_index=True)
    condition = models.JSONField(default=dict, blank=True)   # e.g. JSONLogic
    actions = models.JSONField(default=list, blank=True)     # list of action objects
    enabled = models.BooleanField(default=True)
    description = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "rule_sets"
        indexes = [
            models.Index(fields=["tenant", "event_name", "enabled"]),
        ]

    def __str__(self):
        return f"Rule: {self.event_name} ({'enabled' if self.enabled else 'disabled'})"


class RuleExecutionLog(BaseModel):
    """
    Execution logging model for tracking rule executions.
    Stores every time a rule is evaluated and executed for debugging and auditing.
    """
    record = models.ForeignKey("Record", on_delete=models.CASCADE, related_name="rule_executions")
    rule = models.ForeignKey("RuleSet", on_delete=models.SET_NULL, null=True, blank=True)
    event_name = models.CharField(max_length=100, db_index=True)
    matched = models.BooleanField(default=False)
    actions = models.JSONField(default=list, blank=True)
    errors = models.JSONField(default=list, blank=True)
    duration_ms = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "rule_exec_logs"
        indexes = [
            models.Index(fields=["tenant", "event_name", "-created_at"]),
        ]

    def __str__(self):
        return f"Rule execution: {self.event_name} ({'matched' if self.matched else 'no match'}) at {self.created_at}"
