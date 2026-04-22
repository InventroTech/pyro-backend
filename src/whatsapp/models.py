from django.db import models
from core.models import BaseModel
from object_history.models import HistoryTrackedModel


class WhatsAppTemplate(HistoryTrackedModel, BaseModel):
    """
    WhatsApp template model to store tenant-wise WhatsApp templates.
    Each template has a title and description.
    """
    title = models.CharField(
        max_length=255,
        help_text="Title of the WhatsApp template"
    )
    description = models.TextField(
        help_text="Description/content of the WhatsApp template"
    )

    class Meta:
        db_table = 'whatsapp_templates'
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=['tenant', 'title']),
        ]
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.title} ({self.tenant.name if self.tenant else 'No Tenant'})"
