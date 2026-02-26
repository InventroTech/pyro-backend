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
