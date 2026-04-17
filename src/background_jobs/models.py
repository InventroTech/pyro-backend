from django.db import models
from core.models import BaseModel
from object_history.models import HistoryTrackedModel


class JobStatus(models.TextChoices):
    """Status choices for background jobs"""
    PENDING = "PENDING", "Pending"
    PROCESSING = "PROCESSING", "Processing"
    COMPLETED = "COMPLETED", "Completed"
    FAILED = "FAILED", "Failed"
    RETRYING = "RETRYING", "Retrying"


class JobType(models.TextChoices):
    """Job type choices for background jobs"""
    SEND_MIXPANEL_EVENT = "send_mixpanel_event", "Send Mixpanel Event"
    SEND_RM_ASSIGNED_EVENT = "send_rm_assigned_event", "Send RM Assigned Event"
    SEND_WEBHOOK = "send_webhook", "Send Webhook"
    EXECUTE_FUNCTION = "execute_function", "Execute Function"
    SCORE_LEADS = "score_leads", "Score Leads"
    SCORE_LEADS_CHUNK = "score_leads_chunk", "Score Leads Chunk"
    SEND_TO_PRAJA = "send_to_praja", "Send to Praja Server"
    PARTNER_LEAD_ASSIGN = "partner_lead_assign", "Partner Lead Assign"
    UNASSIGN_SNOOZED_LEADS = "unassign_snoozed_leads", "Unassign Snoozed Leads"
    RELEASE_LEADS_AFTER_12H = "release_leads_after_12h", "Release Leads After 12h"
    AGGREGATE_RECORDS = "aggregate_records", "Aggregate Records Schema"
    CLOSE_STALE_SUBSCRIPTION_LEADS = "close_stale_subscription_leads", "Close Stale Subscription Leads"
    SNOOZED_TO_NOT_CONNECTED_MIDNIGHT = (
        "snoozed_to_not_connected_midnight",
        "Snoozed To Not Connected (midnight)",
    )
    # Future job types can be added here:
    # SEND_EMAIL = "send_email", "Send Email"
    # GENERATE_REPORT = "generate_report", "Generate Report"
    # PROCESS_FILE = "process_file", "Process File"
    # SYNC_DATA = "sync_data", "Sync Data"
    # SEND_SMS = "send_sms", "Send SMS"
    # EXPORT_DATA = "export_data", "Export Data"


class BackgroundJob(HistoryTrackedModel, BaseModel):
    """
    Database-backed job queue for async task processing.
    Stores jobs that need to be executed asynchronously by background workers.
    """
    job_type = models.CharField(
        max_length=50,
        choices=JobType.choices,
        db_index=True,
        help_text="Type of job to execute"
    )
    status = models.CharField(
        max_length=20,
        choices=JobStatus.choices,
        default=JobStatus.PENDING,
        db_index=True,
        help_text="Current status of the job"
    )
    priority = models.SmallIntegerField(
        default=0,
        db_index=True,
        help_text="Job priority (higher = more priority). 10=critical, 5=high, 0=normal, -5=low"
    )
    
    # Job payload - stores all data needed to execute the job
    payload = models.JSONField(
        default=dict,
        blank=True,
        help_text="Job-specific data required for execution"
    )
    
    # Retry logic
    attempts = models.PositiveSmallIntegerField(
        default=0,
        help_text="Number of processing attempts made"
    )
    max_attempts = models.PositiveSmallIntegerField(
        default=3,
        help_text="Maximum number of retry attempts before marking as failed"
    )
    last_error = models.TextField(
        null=True,
        blank=True,
        help_text="Last error message if job failed"
    )
    scheduled_at = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="When to schedule this job (for delayed execution)"
    )
    
    # Worker locking (prevent multiple workers from picking same job)
    locked_by = models.CharField(
        max_length=64,
        null=True,
        blank=True,
        help_text="Worker identifier that has locked this job"
    )
    locked_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When this job was locked by a worker"
    )
    
    # Completion tracking
    completed_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the job was completed"
    )
    result = models.JSONField(
        null=True,
        blank=True,
        help_text="Job result data (if any)"
    )

    class Meta:
        db_table = "background_jobs"
        indexes = [
            models.Index(fields=["status", "priority", "-created_at"]),
            models.Index(fields=["status", "scheduled_at"]),
            models.Index(fields=["job_type", "status"]),
            models.Index(fields=["tenant", "status"]),
        ]
        ordering = ["-priority", "created_at"]

    def __str__(self):
        return f"Job {self.id}: {self.job_type} ({self.status})"

