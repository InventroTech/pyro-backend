from django.db import migrations, models
from django.db.models import Q

import core.soft_delete


class Migration(migrations.Migration):

    dependencies = [
        ("authz", "0013_tenantmembership_department"),
    ]

    operations = [
        migrations.AddField(
            model_name="tenantmembership",
            name="is_deleted",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="tenantmembership",
            name="deleted_at",
            field=models.DateTimeField(blank=True, default=None, null=True),
        ),
        migrations.RemoveConstraint(
            model_name="tenantmembership",
            name="uniq_authz_tm_tenant_role_email",
        ),
        migrations.RemoveConstraint(
            model_name="tenantmembership",
            name="uniq_authz_tm_tenant_user_nn",
        ),
        migrations.AddConstraint(
            model_name="tenantmembership",
            constraint=models.UniqueConstraint(
                fields=("tenant", "role", "email"),
                condition=Q(deleted_at__isnull=True),
                name="uniq_authz_tm_tenant_role_email",
            ),
        ),
        migrations.AddConstraint(
            model_name="tenantmembership",
            constraint=models.UniqueConstraint(
                fields=("tenant", "user_id"),
                condition=Q(user_id__isnull=False, deleted_at__isnull=True),
                name="uniq_authz_tm_tenant_user_nn",
            ),
        ),
        migrations.AddIndex(
            model_name="tenantmembership",
            index=models.Index(
                fields=("is_deleted",),
                name="authz_tm_is_deleted_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="tenantmembership",
            index=models.Index(
                fields=("deleted_at",),
                name="authz_tm_deleted_at_idx",
            ),
        ),
        migrations.AlterModelManagers(
            name="tenantmembership",
            managers=[
                ("objects", core.soft_delete.SoftDeleteManager()),
                ("all_objects", core.soft_delete.AllObjectsManager()),
            ],
        ),
    ]
