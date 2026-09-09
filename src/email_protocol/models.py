"""
Models for inbound email integrations (Zoho Mail OAuth, etc.).
"""

from __future__ import annotations

from django.db import models

from core.models import Tenant, TimeStampedModel


class ZohoMailConnection(TimeStampedModel):
    """
    Zoho Mail OAuth connection row for a tenant ops inbox.

    Each connect creates a new row. Disconnect sets ``disconnected_at`` and
    clears tokens but keeps ``email_address`` for history. Only one row per
    tenant may be active at a time.
    """

    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.CASCADE,
        related_name="zoho_mail_connections",
        db_column="tenant_id",
    )
    email_address = models.EmailField(blank=True, default="")
    account_id = models.CharField(max_length=64, blank=True, default="")
    inbox_folder_id = models.CharField(max_length=64, blank=True, default="")

    refresh_token = models.TextField()
    access_token = models.TextField(blank=True, default="")
    access_token_expires_at = models.DateTimeField(null=True, blank=True)

    # Zoho data-center URLs from OAuth callback (accounts-server / location).
    accounts_base_url = models.URLField(
        max_length=255,
        default="https://accounts.zoho.com",
        help_text="e.g. https://accounts.zoho.com or https://accounts.zoho.in",
    )
    mail_api_base_url = models.URLField(
        max_length=255,
        default="https://mail.zoho.com/api",
        help_text="e.g. https://mail.zoho.com/api or https://mail.zoho.in/api",
    )

    is_active = models.BooleanField(default=True, db_index=True)
    disconnected_at = models.DateTimeField(null=True, blank=True, db_index=True)
    last_synced_at = models.DateTimeField(null=True, blank=True)
    # Incremental sync cursor: Zoho receivedTime (ms since epoch) of newest processed mail.
    last_received_time_ms = models.BigIntegerField(null=True, blank=True)
    # False until the first full inbox backfill finishes (paginated, no time cursor).
    initial_backfill_completed = models.BooleanField(default=False, db_index=True)
    # Zoho list_messages ``start`` index; resumes backfill across job runs.
    backfill_next_start = models.PositiveIntegerField(default=1)
    connected_by_email = models.EmailField(blank=True, default="")

    class Meta:
        db_table = "email_protocol_zoho_mail_connection"
        constraints = [
            models.UniqueConstraint(
                fields=["tenant"],
                condition=models.Q(is_active=True, disconnected_at__isnull=True),
                name="uniq_active_zoho_mail_connection_per_tenant",
            ),
        ]

    def __str__(self) -> str:
        return f"ZohoMailConnection(tenant={self.tenant_id}, email={self.email_address or '?'})"


class ZohoMailProcessedMessage(TimeStampedModel):
    """Idempotency: skip shipment emails we already tried to apply."""

    connection = models.ForeignKey(
        ZohoMailConnection,
        on_delete=models.CASCADE,
        related_name="processed_messages",
    )
    message_id = models.CharField(max_length=64, db_index=True)
    subject = models.CharField(max_length=512, blank=True, default="")
    matched_record_id = models.UUIDField(null=True, blank=True)
    applied = models.BooleanField(default=False)
    skip_reason = models.CharField(max_length=255, blank=True, default="")

    class Meta:
        db_table = "email_protocol_zoho_mail_processed_message"
        constraints = [
            models.UniqueConstraint(
                fields=["connection", "message_id"],
                name="uniq_zoho_mail_processed_message",
            ),
        ]

    def __str__(self) -> str:
        return f"ZohoMailProcessedMessage({self.message_id})"
