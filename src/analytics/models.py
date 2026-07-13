import uuid
from django.db import models

from core.models import BaseModel
from core.soft_delete import alive_q


class AnalyticsBoard(BaseModel):
    """
    A single saved analytics board (one report card) for a user.

    Each row is one board/report, so creating a board inserts a row and deleting
    a board removes its row. Generic across analytics types via ``board_type``
    (CSE, RM, ...). ``config`` stores that board's definition (title, chart type,
    breakdown, metrics and filters) as an opaque JSON object owned by the
    frontend. ``report_id`` is the frontend-generated identifier for the board.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user_id = models.CharField(max_length=128, db_index=True)
    board_type = models.CharField(
        max_length=64,
        default="cse",
        db_index=True,
        help_text="Analytics type this board belongs to, e.g. 'cse', 'rm'.",
    )
    report_id = models.CharField(
        max_length=128,
        default="",
        db_index=True,
        help_text="Frontend-generated identifier for this board/report.",
    )
    config = models.JSONField(
        default=dict,
        blank=True,
        help_text="This board's definition (title, chart type, metrics, filters).",
    )

    class Meta(BaseModel.Meta):
        db_table = "analytics_boards"
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "user_id", "board_type", "report_id"],
                condition=alive_q(),
                name="analytics_board_tenant_user_type_report_uniq_alive",
            ),
        ]
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["tenant", "user_id", "board_type"]),
        ]

    def __str__(self):
        return (
            f"AnalyticsBoard({self.tenant_id}:{self.user_id}:"
            f"{self.board_type}:{self.report_id})"
        )
from object_history.models import HistoryTrackedModel


class AnalyticsRunCore(HistoryTrackedModel, BaseModel):
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
