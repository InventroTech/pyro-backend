from background_jobs.models import BackgroundJob
from crm_records.models import (
    ApiSecretKey,
    Bucket,
    CallAttemptMatrix,
    EntityTypeSchema,
    EventLog,
    PartnerEvent,
    Record,
    RuleExecutionLog,
    RuleSet,
    ScoringRule,
    UserBucketAssignment,
)
from scheduler.models import ScheduledTask, TaskPolicy
from support_ticket.models import SupportTicket
from whatsapp.models import WhatsAppTemplate

from .registry import register

register(
    SupportTicket,
    track_fields=[
        "resolution_status",
        "call_status",
        "assigned_to_id",
        "cse_name",
        "cse_remarks",
        "call_attempts",
        "snooze_until",
        "review_requested",
    ],
    redact_fields={"cse_remarks"},
    snapshot_strategy="minimal",
)

register(
    Record,
    track_fields=[
        "entity_type",
        "data",  # include JSON payload for diffs (name is now stored inside data)
        "tenant_id",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)


register(
    BackgroundJob,
    track_fields=[
        "job_type",
        "status",
        "priority",
        "payload",
        "attempts",
        "max_attempts",
        "last_error",
        "scheduled_at",
        "locked_by",
        "locked_at",
        "completed_at",
        "result",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)



register(
    CallAttemptMatrix,
    track_fields=[
        "lead_type",
        "max_call_attempts",
        "sla_days",
        "min_time_between_calls_hours",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    EntityTypeSchema,
    track_fields=[
        "entity_type",
        "attributes",
        "rules",
        "description",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    PartnerEvent,
    track_fields=[
        "partner_slug",
        "event",
        "payload",
        "status",
        "record_id",
        "job_id",
        "processed_at",
        "error_message",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)


register(
    RuleSet,
    track_fields=[
        "event_name",
        "condition",
        "actions",
        "enabled",
        "description",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)


register(
    ScoringRule,
    track_fields=[
        "entity_type",
        "attribute",
        "data",
        "weight",
        "order",
        "is_active",
        "description",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)



register(
    UserBucketAssignment,
    track_fields=[
        "user_id",
        "bucket_id",
        "priority",
        "pull_strategy",
        "is_active",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    WhatsAppTemplate,
    track_fields=[
        "title",
        "description",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)
