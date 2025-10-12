from django.db import models
from core.models import BaseModel


class Record(BaseModel):
    """
    Universal record model that can hold any tenant's data dynamically using JSONB.
    All future entities (leads, tickets, job applications, etc.) will be built on top of this.
    """
    entity_type = models.CharField(max_length=100, db_index=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    data = models.JSONField(default=dict, blank=True)

    class Meta:
        db_table = "records"
        indexes = [
            models.Index(fields=["tenant", "entity_type", "-created_at"]),
        ]

    def __str__(self):
        return f"{self.entity_type}: {self.name or 'Unnamed'}"
