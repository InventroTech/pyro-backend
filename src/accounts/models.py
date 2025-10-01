from django.db import models


class SupabaseAuthUser(models.Model):
    """
    Unmanaged mirror of auth.users
    """
    id = models.UUIDField(primary_key=True)
    email = models.EmailField(null=True, blank=True)
    phone = models.TextField(null=True, blank=True)
    raw_app_meta_data = models.JSONField(null=True, blank=True)
    raw_user_meta_data = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(null=True, blank=True)
    last_sign_in_at = models.DateTimeField(null=True, blank=True)
    is_super_admin = models.BooleanField(null=True, blank=True)
    is_sso_user = models.BooleanField(null=True, blank=True)
    is_anonymous = models.BooleanField(null=True, blank=True)
    deleted_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'auth"."users'

    def __str__(self):
        return self.email or f"{self.id}"
    
class LegacyUser(models.Model):
    id=models.BigAutoField(primary_key=True)
    name = models.TextField()
    email = models.EmailField(null=True, blank=True)
    tenant = models.ForeignKey(
        'core.Tenant',
        db_column='tenant_id',
        to_field='id',
        on_delete=models.DO_NOTHING,
        null=True,
        blank=True,
    )
    company_name = models.TextField(null=True, blank=True)
    role_id = models.UUIDField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    uid = models.UUIDField(null=True, blank=True)

    class Meta:
        db_table = 'users'
        managed = False
    
    def __str__(self):
        return f"{self.email or self.name} ({self.tenant_id})"



class LegacyRole(models.Model):
    id = models.UUIDField(primary_key=True)
    name = models.TextField()
    description = models.TextField(null=True, blank=True)
    tenant = models.ForeignKey(
        'core.Tenant',
        db_column='tenant_id',
        to_field='id',
        on_delete=models.DO_NOTHING,
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'roles'
        managed = False
        indexes = [
            models.Index(fields=['tenant', 'name']),
        ]
