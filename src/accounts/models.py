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
    
# LegacyUser and LegacyRole removed: public.users and public.roles dropped in 0004_drop_legacy_users_and_roles.
# Use TenantMembership (authz) and Role (authz) instead.


class DeletedUserLog(models.Model):
    """
    One row per user removed through DELETE /accounts/delete-user-with-log/.

    A snapshot taken at delete time (who, role, manager...) so the log stays
    correct even if roles or managers change later.
    """

    tenant = models.ForeignKey("core.Tenant", on_delete=models.CASCADE, related_name="deleted_user_logs")
    membership_id = models.IntegerField(null=True, blank=True, db_index=True)
    name = models.CharField(max_length=255, blank=True, default="")
    email = models.EmailField()
    role_id = models.UUIDField(null=True, blank=True)
    role_name = models.CharField(max_length=255, blank=True, default="")
    manager_name = models.CharField(max_length=255, blank=True, default="")
    manager_email = models.EmailField(blank=True, default="")
    account_created_at = models.DateTimeField(null=True, blank=True)
    deleted_at = models.DateTimeField(db_index=True)
    deleted_by = models.CharField(max_length=255, blank=True, default="")

    class Meta:
        db_table = "accounts_deleted_user_log"
        ordering = ("-deleted_at", "-id")
        indexes = [models.Index(fields=("tenant", "-deleted_at"), name="acc_deluserlog_tenant_idx")]

    def __str__(self):
        return f"{self.email} deleted {self.deleted_at:%Y-%m-%d}"
