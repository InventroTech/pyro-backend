from analytics.models import AnalyticsRunCore
from authentication.models import PasswordResetOTP, User
from authz.models import (
    GroupMembership,
    GroupPermission,
    GroupRole,
    Permission,
    Role,
    RolePermission,
    TenantMembership,
    UserGroup,
    UserPermission,
)
from background_jobs.models import BackgroundJob
from core.models import EntityTypeDiscoverySyncState, TenantEntityType, TenantSettings
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
from pages.models import CustomIcon, Page
from pyro_jobs.models import PyroJob
from scheduler.models import AttemptLog, ScheduledTask, TaskPolicy
from support_ticket.models import SupportTicketDump
from user_settings.models import Group, TenantMemberSetting
from whatsapp.models import WhatsAppTemplate

from .registry import register

register(
    TenantMembership,
    track_fields=[
        "tenant_id",
        "user_id",
        "user_parent_id",
        "email",
        "role_id",
        "is_active",
        "name",
        "company_name",
        "department",
        "created_at",
    ],
    redact_fields=set(),
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
    EventLog,
    track_fields=[
        "record_id",
        "event",
        "payload",
        "timestamp",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    RuleExecutionLog,
    track_fields=[
        "record_id",
        "rule_id",
        "event_name",
        "matched",
        "actions",
        "errors",
        "duration_ms",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    ApiSecretKey,
    track_fields=[
        "secret_key_last4",
        "tenant_id",
        "description",
        "is_active",
        "last_used_at",
        "created_at",
        "updated_at",
    ],
    redact_fields={"secret"},
    snapshot_strategy="minimal",
)

register(
    Bucket,
    track_fields=[
        "name",
        "slug",
        "description",
        "filter_conditions",
        "is_system",
        "is_active",
        "tenant_id",
        "created_at",
        "updated_at",
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

# --- authz ---

register(
    Permission,
    track_fields=[
        "perm_key",
        "is_deleted",
        "deleted_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    Role,
    track_fields=[
        "tenant_id",
        "key",
        "name",
        "description",
        "is_deleted",
        "deleted_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    RolePermission,
    track_fields=[
        "role_id",
        "permission_id",
        "is_deleted",
        "deleted_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    UserGroup,
    track_fields=[
        "tenant_id",
        "key",
        "name",
        "description",
        "is_deleted",
        "deleted_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    GroupMembership,
    track_fields=[
        "group_id",
        "user_id",
        "is_deleted",
        "deleted_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    GroupPermission,
    track_fields=[
        "group_id",
        "permission_id",
        "is_deleted",
        "deleted_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    GroupRole,
    track_fields=[
        "group_id",
        "role_id",
        "is_deleted",
        "deleted_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    UserPermission,
    track_fields=[
        "membership_id",
        "permission_id",
        "effect",
        "is_deleted",
        "deleted_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

# --- core ---

register(
    TenantSettings,
    track_fields=[
        "tenant_id",
        "persistent_object_history",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    TenantEntityType,
    track_fields=[
        "entity_type",
        "schema_json",
        "fields_count",
        "first_seen_at",
        "last_seen_at",
        "last_seen_record_id",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    EntityTypeDiscoverySyncState,
    track_fields=[
        "job_name",
        "last_processed_updated_at",
        "last_processed_record_id",
        "last_success_at",
        "last_error",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

# --- pages ---

register(
    Page,
    track_fields=[
        "user_id",
        "name",
        "header_title",
        "display_order",
        "icon_name",
        "config",
        "role_id",
        "tenant_id",
        "created_at",
        "updated_at",
        "is_deleted",
        "deleted_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    CustomIcon,
    track_fields=[
        "name",
        "svg_content",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

# --- user_settings ---

register(
    Group,
    track_fields=[
        "name",
        "group_data",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    TenantMemberSetting,
    track_fields=[
        "tenant_id",
        "tenant_membership_id",
        "key",
        "value",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

# --- authentication ---

register(
    User,
    track_fields=[
        "supabase_uid",
        "email",
        "role",
        "tenant_id",
        "is_active",
        "is_staff",
    ],
    redact_fields={"password"},
    snapshot_strategy="minimal",
)

register(
    PasswordResetOTP,
    track_fields=[
        "email",
        "otp_hash",
        "expires_at",
        "created_at",
    ],
    redact_fields={"otp_hash"},
    snapshot_strategy="minimal",
)

# --- scheduler ---

register(
    TaskPolicy,
    track_fields=[
        "key",
        "intervals",
        "max_attempts",
        "business_hours_only",
        "timezone",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    ScheduledTask,
    track_fields=[
        "content_type_id",
        "object_id",
        "action",
        "policy_id",
        "status",
        "due_at",
        "priority",
        "attempts",
        "max_attempts",
        "locked_by",
        "locked_at",
        "payload",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

register(
    AttemptLog,
    track_fields=[
        "task_id",
        "attempt_no",
        "started_at",
        "ended_at",
        "outcome",
        "notes",
        "is_deleted",
        "deleted_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

# --- analytics ---

register(
    AnalyticsRunCore,
    track_fields=[
        "user_id",
        "question",
        "sql_query",
        "validation_ok",
        "validation_reason",
        "execution_ok",
        "status",
        "error_summary",
        "rows_returned",
        "completed_at",
        "tenant_id",
        "created_at",
        "updated_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

# --- pyro_jobs ---

register(
    PyroJob,
    track_fields=[
        "job_name",
        "payload",
        "run_at",
        "status",
        "is_deleted",
        "attempts",
        "max_attempts",
        "result",
        "error",
        "started_at",
        "completed_at",
        "created_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

# --- support_ticket ---

register(
    SupportTicketDump,
    track_fields=[
        "tenant_id",
        "data",
        "is_processed",
        "is_deleted",
        "deleted_at",
        "created_at",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)
