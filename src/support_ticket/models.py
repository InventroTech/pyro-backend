from django.db import models

from core.soft_delete import SoftDeleteMixin
from object_history.models import HistoryTrackedModel


class SupportTicketDump(HistoryTrackedModel, SoftDeleteMixin):
    """
    Temporary staging table for support tickets before they're processed.

    Ticket payload fields live in ``data`` (JSON); ``tenant_id`` and ``is_processed``
    remain columns for worker queries and indexing.
    """
    id = models.BigAutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    tenant_id = models.UUIDField()
    data = models.JSONField(default=dict, blank=True)
    is_processed = models.BooleanField(default=False)

    class Meta:
        db_table = "support_ticket_dump"
        managed = True
        indexes = [
            models.Index(fields=["tenant_id", "-created_at"], name="std_tn_cr_desc"),
            models.Index(fields=["is_processed", "-created_at"], name="std_proc_cr"),
        ]

    def __str__(self):
        name = (self.data or {}).get("name")
        return f"SupportTicketDump {self.id} - {name or 'Unknown'} ({self.tenant_id})"
