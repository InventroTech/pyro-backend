import uuid
from django.db import models
from django.db.models import Q
from django.db.models.functions import Lower

class Permission(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    perm_key = models.CharField(max_length=128, unique=True)

class Role(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey('core.Tenant', on_delete=models.CASCADE)
    key = models.CharField(max_length=64)   # e.g. GM, OWNER, AGENT
    name = models.CharField(max_length=128)
    description = models.TextField(blank=True, null=True)
    class Meta:
        unique_together = (('tenant','key'),)

class RolePermission(models.Model):
    role = models.ForeignKey(Role, on_delete=models.CASCADE)
    permission = models.ForeignKey(Permission, on_delete=models.CASCADE)
    class Meta:
        unique_together = (('role','permission'),)

class TenantMembership(models.Model):
    class Meta:
        db_table = "authz_tenantmembership"
        constraints = [
            models.CheckConstraint(
                check=Q(email__isnull=True) | Q(email=Lower("email")),
                name="authz_tm_email_lower_chk",
            ),
            models.UniqueConstraint(
                fields=("tenant", "role", "email"),
                name="uniq_authz_tm_tenant_role_email",
            ),
            models.UniqueConstraint(
                fields=("tenant", "user_id"),
                condition=Q(user_id__isnull=False),
                name="uniq_authz_tm_tenant_user_nn",
            ),
        ]
        indexes = [
            models.Index(fields=("tenant",), name="authz_tm_tenant_idx"),
            models.Index(fields=("role",), name="authz_tm_role_idx"),
            models.Index(Lower("email"), name="authz_tm_email_lower_idx"),
        ]

    tenant = models.ForeignKey("core.Tenant", on_delete=models.CASCADE)
    user_id = models.UUIDField(null=True, blank=True)
    email = models.EmailField(null=False)  # or null=True initially for backfill
    role = models.ForeignKey("Role", on_delete=models.RESTRICT)
    is_active = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    def save(self, *args, **kwargs):
        if self.email:
            self.email = self.email.strip().lower()
        super().save(*args, **kwargs)

class UserGroup(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey('core.Tenant', on_delete=models.CASCADE)
    key = models.CharField(max_length=64)
    name = models.CharField(max_length=128)
    description = models.TextField(blank=True, null=True)
    class Meta:
        unique_together = (('tenant','key'),)

class GroupMembership(models.Model):
    group = models.ForeignKey(UserGroup, on_delete=models.CASCADE)
    user_id = models.UUIDField()
    class Meta:
        unique_together = (('group','user_id'),)

class GroupPermission(models.Model):
    group = models.ForeignKey(UserGroup, on_delete=models.CASCADE)
    permission = models.ForeignKey(Permission, on_delete=models.CASCADE)
    class Meta:
        unique_together = (('group','permission'),)

class GroupRole(models.Model):
    group = models.ForeignKey(UserGroup, on_delete=models.CASCADE)
    role = models.ForeignKey(Role, on_delete=models.CASCADE)
    class Meta:
        unique_together = (('group','role'),)

class UserPermission(models.Model):
    tenant = models.ForeignKey('core.Tenant', on_delete=models.CASCADE)
    user_id = models.UUIDField()
    permission = models.ForeignKey(Permission, on_delete=models.CASCADE)
    effect = models.CharField(max_length=8, choices=[('allow','allow'),('deny','deny')])
    class Meta:
        unique_together = (('tenant','user_id','permission'),)
