import uuid
from django.db import models
from core.models import BaseModel, RoleBaseModel
from core.soft_delete import alive_q


class Page(RoleBaseModel):
    """
    User-defined dashboard page: name, role visibility, and widget config (JSON).
    Stored in public.pages; tenant + role from RoleModel (role column is "role").
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user_id = models.UUIDField(
        help_text='Supabase auth user id (owner of this page).',
        db_index=True,
    )
    name = models.CharField(max_length=255)
    header_title = models.CharField(max_length=255, blank=True, null=True)
    display_order = models.IntegerField(default=0)
    icon_name = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        default='Sparkles',
        help_text="Key matching the navigationIconMap in the frontend."
    )
    config = models.JSONField(
        default=list,
        blank=True,
        help_text='List of widget configs, e.g. [{"id": "...", "type": "ticketTable", "config": {...}}].',
    )
    role = models.ForeignKey(
        'authz.Role',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        db_index=True,
        db_column='role',
        related_name='pages',
    )

    class Meta(RoleBaseModel.Meta):
        db_table = 'pages'
        ordering = ['display_order', '-updated_at']
        indexes = [
            *RoleBaseModel.Meta.indexes,
            models.Index(fields=['tenant', 'user_id']),
            models.Index(fields=['tenant', 'role']),
        ]

    def __str__(self):
        return f"{self.name} (tenant={self.tenant_id}, user={self.user_id})"


class CustomIcon(BaseModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey('core.Tenant', on_delete=models.CASCADE)
    name = models.CharField(max_length=255)
    svg_content = models.TextField()

    class Meta(BaseModel.Meta):
        db_table = 'custom_icons'
        constraints = [
            models.UniqueConstraint(
                fields=['tenant', 'name'],
                condition=alive_q(),
                name='pages_customicon_tenant_name_uniq_alive',
            ),
        ]
        indexes = list(BaseModel.Meta.indexes)

    def __str__(self):
        return f"{self.name} ({self.tenant.slug if self.tenant else 'No Tenant'})"
