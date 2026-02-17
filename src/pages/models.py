import uuid
from django.db import models
from core.models import Tenant, TimeStampedModel


class Page(TimeStampedModel):
    """
    User-defined dashboard page: name, role visibility, and widget config (JSON).
    Stored in public.pages; role references authz_role (not legacy roles).
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.CASCADE,
        db_column='tenant_id',
        related_name='pages',
    )
    user_id = models.UUIDField(
        help_text='Supabase auth user id (owner of this page).',
        db_index=True,
    )
    name = models.CharField(max_length=255)
    config = models.JSONField(
        default=list,
        blank=True,
        help_text='List of widget configs, e.g. [{"id": "...", "type": "ticketTable", "config": {...}}].',
    )
    role = models.ForeignKey(
        'authz.Role',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        db_column='role',
        related_name='pages',
        help_text='Role this page is scoped to (from authz_role).',
    )

    class Meta:
        db_table = 'pages'
        ordering = ['-updated_at']
        indexes = [
            models.Index(fields=['tenant', 'user_id']),
            models.Index(fields=['tenant', 'role']),
        ]

    def __str__(self):
        return f"{self.name} (tenant={self.tenant_id}, user={self.user_id})"
