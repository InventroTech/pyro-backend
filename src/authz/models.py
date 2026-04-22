import uuid
from django.db import models
from django.db.models import Q
from django.db.models.functions import Lower

from core.models import SoftDeleteMixin
from core.soft_delete import alive_q


class Permission(SoftDeleteMixin):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    perm_key = models.CharField(max_length=128, db_index=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["perm_key"],
                condition=alive_q(),
                name="authz_permission_perm_key_uniq_alive",
            ),
        ]
        indexes = [
            models.Index(fields=("is_deleted",), name="authz_perm_is_deleted_idx"),
            models.Index(fields=("deleted_at",), name="authz_perm_deleted_at_idx"),
        ]


class Role(SoftDeleteMixin):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey("core.Tenant", on_delete=models.CASCADE)
    key = models.CharField(max_length=64)
    name = models.CharField(max_length=128)
    description = models.TextField(blank=True, null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                Lower("key"),
                "tenant",
                condition=alive_q(),
                name="uniq_authz_role_tenant_lower_key",
            ),
        ]
        indexes = [
            models.Index(Lower("key"), name="authz_role_lower_key_idx"),
            models.Index(fields=("is_deleted",), name="authz_role_is_deleted_idx"),
            models.Index(fields=("deleted_at",), name="authz_role_deleted_at_idx"),
        ]


class RolePermission(SoftDeleteMixin):
    role = models.ForeignKey(Role, on_delete=models.CASCADE)
    permission = models.ForeignKey(Permission, on_delete=models.CASCADE)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("role", "permission"),
                condition=alive_q(),
                name="authz_rolepermission_role_perm_uniq_alive",
            ),
        ]
        indexes = [
            models.Index(fields=("is_deleted",), name="authz_rp_is_deleted_idx"),
            models.Index(fields=("deleted_at",), name="authz_rp_deleted_at_idx"),
        ]


class TenantMembership(SoftDeleteMixin):
    class Meta:
        db_table = "authz_tenantmembership"
        constraints = [
            models.CheckConstraint(
                check=Q(email__isnull=True) | Q(email=Lower("email")),
                name="authz_tm_email_lower_chk",
            ),
            models.UniqueConstraint(
                fields=("tenant", "role", "email"),
                condition=alive_q(),
                name="uniq_authz_tm_tenant_role_email",
            ),
            models.UniqueConstraint(
                fields=("tenant", "user_id"),
                condition=Q(user_id__isnull=False) & alive_q(),
                name="uniq_authz_tm_tenant_user_nn",
            ),
        ]
        indexes = [
            models.Index(fields=("tenant",), name="authz_tm_tenant_idx"),
            models.Index(fields=("role",), name="authz_tm_role_idx"),
            models.Index(Lower("email"), name="authz_tm_email_lower_idx"),
            models.Index(fields=("user_id",), name="authz_tm_user_id_idx"),
            models.Index(fields=("user_parent_id",), name="authz_tm_user_parent_id_idx"),
            models.Index(fields=("tenant", "user_parent_id"), name="authz_tm_tenant_user_parent"),
            models.Index(fields=("is_active",), name="authz_tm_is_active_idx"),
            models.Index(fields=("name",), name="authz_tm_name_idx"),
            models.Index(fields=("company_name",), name="authz_tm_company_name_idx"),
            models.Index(fields=("is_deleted",), name="authz_tm_is_deleted_idx"),
            models.Index(fields=("deleted_at",), name="authz_tm_deleted_at_idx"),
        ]

    tenant = models.ForeignKey("core.Tenant", on_delete=models.CASCADE)
    user_id = models.UUIDField(null=True, blank=True, db_index=True)
    user_parent_id = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="direct_reports",
        db_index=True,
    )
    email = models.EmailField(null=False)
    role = models.ForeignKey("Role", on_delete=models.RESTRICT)
    is_active = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    name = models.CharField(max_length=255, null=True, blank=True, help_text="User's display name")
    company_name = models.CharField(max_length=255, null=True, blank=True, help_text="Optional company name")
    department = models.CharField(max_length=255, null=True, blank=True, help_text="Optional department")

    soft_delete_cascade = (
        "userpermission_set",
        "direct_reports",
    )

    def save(self, *args, **kwargs):
        if self.email:
            self.email = self.email.strip().lower()
        super().save(*args, **kwargs)


class UserGroup(SoftDeleteMixin):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey("core.Tenant", on_delete=models.CASCADE)
    key = models.CharField(max_length=64)
    name = models.CharField(max_length=128)
    description = models.TextField(blank=True, null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("tenant", "key"),
                condition=alive_q(),
                name="authz_usergroup_tenant_key_uniq_alive",
            ),
        ]
        indexes = [
            models.Index(fields=("is_deleted",), name="authz_ug_is_deleted_idx"),
            models.Index(fields=("deleted_at",), name="authz_ug_deleted_at_idx"),
        ]

    soft_delete_cascade = (
        "groupmembership_set",
        "grouppermission_set",
        "grouprole_set",
    )


class GroupMembership(SoftDeleteMixin):
    group = models.ForeignKey(UserGroup, on_delete=models.CASCADE)
    user_id = models.UUIDField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("group", "user_id"),
                condition=alive_q(),
                name="authz_groupmembership_group_user_uniq_alive",
            ),
        ]
        indexes = [
            models.Index(fields=("is_deleted",), name="authz_gm_is_deleted_idx"),
            models.Index(fields=("deleted_at",), name="authz_gm_deleted_at_idx"),
        ]


class GroupPermission(SoftDeleteMixin):
    group = models.ForeignKey(UserGroup, on_delete=models.CASCADE)
    permission = models.ForeignKey(Permission, on_delete=models.CASCADE)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("group", "permission"),
                condition=alive_q(),
                name="authz_grouppermission_group_perm_uniq_alive",
            ),
        ]
        indexes = [
            models.Index(fields=("is_deleted",), name="authz_gp_is_deleted_idx"),
            models.Index(fields=("deleted_at",), name="authz_gp_deleted_at_idx"),
        ]


class GroupRole(SoftDeleteMixin):
    group = models.ForeignKey(UserGroup, on_delete=models.CASCADE)
    role = models.ForeignKey(Role, on_delete=models.CASCADE)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("group", "role"),
                condition=alive_q(),
                name="authz_grouprole_group_role_uniq_alive",
            ),
        ]
        indexes = [
            models.Index(fields=("is_deleted",), name="authz_gr_is_deleted_idx"),
            models.Index(fields=("deleted_at",), name="authz_gr_deleted_at_idx"),
        ]


class UserPermission(SoftDeleteMixin):
    """
    Per-user permission overrides, scoped by TenantMembership.
    No explicit tenant FK is needed because membership.tenant is the source of truth.
    """

    membership = models.ForeignKey(
        TenantMembership,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
    )
    permission = models.ForeignKey(Permission, on_delete=models.CASCADE)
    effect = models.CharField(
        max_length=8,
        choices=[("allow", "allow"), ("deny", "deny")],
        default="allow",
    )

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=("membership", "permission"),
                condition=alive_q(),
                name="authz_userpermission_mship_perm_uniq_alive",
            ),
        ]
        indexes = [
            models.Index(fields=("is_deleted",), name="authz_up_is_deleted_idx"),
            models.Index(fields=("deleted_at",), name="authz_up_deleted_at_idx"),
        ]
