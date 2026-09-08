from django.db import models
from django.utils import timezone


class InAppNotification(models.Model):
    """
    Maps to existing public.in_app_notifications.
    Unread = read_at IS NULL. Marking read sets read_at and hides from the inbox.
    """

    id = models.BigAutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    user_id = models.CharField(max_length=255, db_index=True)
    notification_type = models.CharField(max_length=64, db_index=True)
    title = models.CharField(max_length=255)
    message = models.TextField()
    read_at = models.DateTimeField(null=True, blank=True, db_index=True)
    record_id = models.BigIntegerField(null=True, blank=True, db_index=True)
    tenant_id = models.UUIDField(null=True, blank=True, db_index=True)

    class Meta:
        db_table = "in_app_notifications"
        ordering = ["-created_at"]

    @property
    def is_read(self) -> bool:
        return self.read_at is not None

    def mark_read(self) -> None:
        if self.read_at is not None:
            return
        self.read_at = timezone.now()
        self.save(update_fields=["read_at", "updated_at"])
