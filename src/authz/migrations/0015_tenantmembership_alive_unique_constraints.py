"""
Tighten partial unique constraints to match SoftDeleteMixin / BaseModel semantics:
uniqueness applies only when is_deleted=False AND deleted_at IS NULL (alive_q).
"""

from django.db import migrations, models
from django.db.models import Q


class Migration(migrations.Migration):

    dependencies = [
        ("authz", "0014_tenantmembership_soft_delete"),
    ]

    operations = [
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
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="uniq_authz_tm_tenant_role_email",
            ),
        ),
        migrations.AddConstraint(
            model_name="tenantmembership",
            constraint=models.UniqueConstraint(
                fields=("tenant", "user_id"),
                condition=Q(user_id__isnull=False)
                & Q(is_deleted=False)
                & Q(deleted_at__isnull=True),
                name="uniq_authz_tm_tenant_user_nn",
            ),
        ),
    ]
