"""
UserSettings, RoutingRule, Group, TenantMemberSetting extend BaseModel:
is_deleted, deleted_at, partial uniques (alive_q), BaseModel index, managers.
"""

from django.db import migrations, models
from django.db.models import Q

import core.soft_delete


class Migration(migrations.Migration):

    dependencies = [
        ("user_settings", "0016_rename_userkvsetting_to_tenantmembersetting"),
    ]

    operations = [
        migrations.AddField(
            model_name="usersettings",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="usersettings",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddField(
            model_name="routingrule",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="routingrule",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddField(
            model_name="group",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="group",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AddField(
            model_name="tenantmembersetting",
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="tenantmembersetting",
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
        migrations.AlterUniqueTogether(
            name="usersettings",
            unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="routingrule",
            unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="group",
            unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="tenantmembersetting",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="usersettings",
            constraint=models.UniqueConstraint(
                fields=("tenant", "tenant_membership", "key"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="user_settings_tenant_mship_key_uniq_alive",
            ),
        ),
        migrations.AddConstraint(
            model_name="routingrule",
            constraint=models.UniqueConstraint(
                fields=("tenant", "tenant_membership", "queue_type"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="routing_rules_tenant_mship_queue_uniq_alive",
            ),
        ),
        migrations.AddConstraint(
            model_name="group",
            constraint=models.UniqueConstraint(
                fields=("tenant", "name"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="user_settings_groups_tenant_name_uniq_alive",
            ),
        ),
        migrations.AddConstraint(
            model_name="tenantmembersetting",
            constraint=models.UniqueConstraint(
                fields=("tenant", "tenant_membership", "key"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="user_kv_tenant_mship_key_uniq_alive",
            ),
        ),
        migrations.AddIndex(
            model_name="usersettings",
            index=models.Index(
                fields=["tenant", "-created_at"],
                name="user_settin_tnt_cr_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="routingrule",
            index=models.Index(
                fields=["tenant", "-created_at"],
                name="routing_rul_tnt_cr_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="group",
            index=models.Index(
                fields=["tenant", "-created_at"],
                name="groups_tnt_cr_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="tenantmembersetting",
            index=models.Index(
                fields=["tenant", "-created_at"],
                name="user_kv_set_tnt_cr_idx",
            ),
        ),
        migrations.AlterModelManagers(
            name="usersettings",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AlterModelManagers(
            name="routingrule",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AlterModelManagers(
            name="group",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
        migrations.AlterModelManagers(
            name="tenantmembersetting",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
    ]
