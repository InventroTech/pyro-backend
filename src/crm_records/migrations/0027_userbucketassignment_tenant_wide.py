# Tenant-wide bucket assignments: nullable user + partial unique constraints.

import django.db.models.deletion
from django.db import migrations, models
from django.db.models import Q


class Migration(migrations.Migration):

    dependencies = [
        ("authz", "0013_tenantmembership_department"),
        ("crm_records", "0026_bucket_userbucketassignment_and_more"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="userbucketassignment",
            unique_together=set(),
        ),
        migrations.AlterField(
            model_name="userbucketassignment",
            name="user",
            field=models.ForeignKey(
                blank=True,
                help_text="If null, this row applies to all RMs in the tenant.",
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                to="authz.tenantmembership",
            ),
        ),
        migrations.AddConstraint(
            model_name="userbucketassignment",
            constraint=models.UniqueConstraint(
                condition=Q(user__isnull=True),
                fields=("tenant", "bucket"),
                name="crm_records_uba_tenant_bucket_tenant_default_uniq",
            ),
        ),
        migrations.AddConstraint(
            model_name="userbucketassignment",
            constraint=models.UniqueConstraint(
                condition=Q(user__isnull=False),
                fields=("tenant", "user", "bucket"),
                name="crm_records_uba_tenant_user_bucket_uniq",
            ),
        ),
    ]
