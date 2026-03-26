from django.db import models
from core.models import BaseModel
from django.utils import timezone
from object_history.models import HistoryTrackedModel

TERMINAL_STATUSES = ("won", "lost", "closed")

class Lead(HistoryTrackedModel, BaseModel):
    id = models.BigAutoField(primary_key=True)
    name = models.TextField()
    phone_no = models.TextField(unique=True)
    user_id = models.CharField(max_length=255, null=True, blank=True)
    lead_description = models.TextField(null=True, blank=True)
    other_description = models.TextField(null=True, blank=True)
    badge = models.TextField(null=True, blank=True)
    lead_creation_date = models.DateField(null=True, blank=True)
    praja_dashboard_user_link = models.TextField(null=True, blank=True)
    lead_score = models.FloatField(null=True, blank=True)
    atleast_paid_once = models.BooleanField(null=True, blank=True)
    reason = models.TextField(null=True, blank=True) 
    badge = models.CharField(max_length=255, null=True, blank=True)
    display_pic_url = models.TextField(null=True, blank=True)
    assigned_to = models.ForeignKey(
        'authentication.User',
        to_field='supabase_uid',
        db_column='assigned_to',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='assigned_leads',
    )
    lead_status = models.CharField(max_length=50, null=True, blank=True)  
    attempt_count = models.PositiveSmallIntegerField(default=0)
    last_call_outcome = models.CharField(max_length=50, null=True, blank=True)
    next_call_at = models.DateTimeField(null=True, blank=True, db_index=True) 
    do_not_call = models.BooleanField(default=False, db_index=True)
    resolved_at = models.DateTimeField(null=True, blank=True, db_index=True) 

    def is_terminal(self, status=None):
        s = status if status is not None else self.lead_status
        return s in TERMINAL_STATUSES

    def set_status(self, new_status: str, *, when=None, set_reason=None, set_by=None):
        """
        Central method to update status & resolved_at consistently.
        - Sets resolved_at when entering a terminal state and it's not already set.
        - (Optional) Unsets resolved_at if leaving terminal (choose policy below).
        """
        now = when or timezone.now()
        old_terminal = self.is_terminal()
        new_terminal = new_status in TERMINAL_STATUSES

        self.lead_status = new_status

        # resolution time
        if new_terminal and not self.resolved_at:
            self.resolved_at = now

        # If we want to clear it when leaving a terminal state:
        if old_terminal and not new_terminal:
            self.resolved_at = None

        if set_reason is not None:
            self.reason = set_reason
        if set_by is not None:
            self.assigned_to = set_by



    class Meta:
        db_table = 'leads'
        managed = True
        indexes = BaseModel.Meta.indexes + [
            models.Index(fields=['assigned_to', 'lead_status', '-created_at']),
            models.Index(fields=['lead_status', 'next_call_at'])
        ]

    def __str__(self):
        return f"{self.name} - {self.phone_no}"
