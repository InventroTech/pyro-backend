"""
Soft-delete columns and partial unique constraints for authz models that use
SoftDeleteMixin (Permission, Role, RolePermission, UserGroup, GroupMembership,
GroupPermission, GroupRole, UserPermission). TenantMembership is covered by 0014/0015.
"""

import django.db.models.functions.text
from django.db import migrations, models
from django.db.models import Q

import core.soft_delete


class Migration(migrations.Migration):

    dependencies = [
        ("authz", "0015_tenantmembership_alive_unique_constraints"),
    ]

    operations = [
        migrations.AddField(
            model_name="permission",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="permission",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddField(
            model_name="role",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="role",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddField(
            model_name="rolepermission",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="rolepermission",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddField(
            model_name="usergroup",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="usergroup",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddField(
            model_name="groupmembership",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="groupmembership",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddField(
            model_name="grouppermission",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="grouppermission",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddField(
            model_name="grouprole",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="grouprole",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddField(
            model_name="userpermission",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="userpermission",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AlterField(
            model_name="permission",
            name="perm_key",
            field=models.CharField(db_index=True, max_length=128),
        ),
        migrations.RemoveConstraint(
            model_name="role",
            name="uniq_authz_role_tenant_lower_key",
        ),
        migrations.AlterUniqueTogether(
            name="rolepermission",
            unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="usergroup",
            unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="groupmembership",
            unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="grouppermission",
            unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="grouprole",
            unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="userpermission",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="permission",
            constraint=models.UniqueConstraint(
                fields=["perm_key"],
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="authz_permission_perm_key_uniq_alive",
            ),
        ),
        migrations.AddConstraint(
            model_name="role",
            constraint=models.UniqueConstraint(
                django.db.models.functions.text.Lower("key"),
                models.F("tenant"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="uniq_authz_role_tenant_lower_key",
            ),
        ),
        migrations.AddConstraint(
            model_name="rolepermission",
            constraint=models.UniqueConstraint(
                fields=("role", "permission"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="authz_rolepermission_role_perm_uniq_alive",
            ),
        ),
        migrations.AddConstraint(
            model_name="usergroup",
            constraint=models.UniqueConstraint(
                fields=("tenant", "key"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="authz_usergroup_tenant_key_uniq_alive",
            ),
        ),
        migrations.AddConstraint(
            model_name="groupmembership",
            constraint=models.UniqueConstraint(
                fields=("group", "user_id"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="authz_groupmembership_group_user_uniq_alive",
            ),
        ),
        migrations.AddConstraint(
            model_name="grouppermission",
            constraint=models.UniqueConstraint(
                fields=("group", "permission"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="authz_grouppermission_group_perm_uniq_alive",
            ),
        ),
        migrations.AddConstraint(
            model_name="grouprole",
            constraint=models.UniqueConstraint(
                fields=("group", "role"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="authz_grouprole_group_role_uniq_alive",
            ),
        ),
        migrations.AddConstraint(
            model_name="userpermission",
            constraint=models.UniqueConstraint(
                fields=("membership", "permission"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="authz_userpermission_mship_perm_uniq_alive",
            ),
        ),
        migrations.AlterModelManagers(
            name="permission",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AlterModelManagers(
            name="role",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AlterModelManagers(
            name="rolepermission",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AlterModelManagers(
            name="usergroup",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AlterModelManagers(
            name="groupmembership",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AlterModelManagers(
            name="grouppermission",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AlterModelManagers(
            name="grouprole",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AlterModelManagers(
            name="userpermission",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
    ]
