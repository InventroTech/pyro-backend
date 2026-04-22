import uuid
from django.db import models

from core.models import BaseModel


class AnalyticsRunCore(BaseModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    completed_at = models.DateTimeField(null=True, blank=True)
    user_id = models.CharField(max_length=128, db_index=True)

    question = models.TextField()
    sql_query = models.TextField(null=True, blank=True)
    validation_ok = models.BooleanField(default=False)
    validation_reason = models.TextField(null=True, blank=True)
    execution_ok = models.BooleanField(default=False)
    final_result = models.JSONField(null=True, blank=True)
    status = models.CharField(max_length=32, default="started", db_index=True)

    error_summary = models.TextField(null=True, blank=True)
    rows_returned = models.IntegerField(null=True, blank=True)

    class Meta:
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["created_at"]),
            models.Index(fields=["user_id", "created_at"]),
            models.Index(fields=["status"]),
        ]
