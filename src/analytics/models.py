from django.db import models
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex, BrinIndex
import uuid
from django.db.models import Q

from core.soft_delete import SoftDeleteModel


class AnalyticsRunCore(SoftDeleteModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    user_id = models.CharField(max_length=128, db_index=True)

    question = models.TextField()  
    sql_query = models.TextField(null=True, blank=True)
    validation_ok = models.BooleanField(default=False)
    validation_reason = models.TextField(null=True, blank=True)
    execution_ok = models.BooleanField(default=False)
    final_result = models.JSONField(null=True, blank=True)  # storing a small preview to avoid bloat
    status = models.CharField(max_length=32, default="started", db_index=True)

    error_summary = models.TextField(null=True, blank=True)  # short error msg if any
    rows_returned = models.IntegerField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["created_at"]),
            models.Index(fields=["user_id", "created_at"]),
            models.Index(fields=["status"]),
            models.Index(fields=["is_deleted"]),
            models.Index(fields=["deleted_at"]),
        ]
