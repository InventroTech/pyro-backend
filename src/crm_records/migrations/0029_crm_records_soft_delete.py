# BaseModel / SoftDeleteMixin: is_deleted, deleted_at, partial uniques, managers.

from django.db import migrations, models
from django.db.models import Q

import core.soft_delete


def _soft_delete_fields(model_name):
    return [
        migrations.AddField(
            model_name=model_name,
            name="is_deleted",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name=model_name,
            name="deleted_at",
            field=models.DateTimeField(
                blank=True, db_index=True, default=None, null=True
            ),
        ),
    ]


class Migration(migrations.Migration):

    dependencies = [
        ("crm_records", "0028_record_lead_field_expression_indexes"),
    ]

    operations = [
        *_soft_delete_fields("record"),
        *_soft_delete_fields("eventlog"),
        *_soft_delete_fields("ruleset"),
        *_soft_delete_fields("ruleexecutionlog"),
        *_soft_delete_fields("partnerevent"),
        *_soft_delete_fields("entitytypeschema"),
        *_soft_delete_fields("scoringrule"),
        *_soft_delete_fields("apisecretkey"),
        *_soft_delete_fields("callattemptmatrix"),
        *_soft_delete_fields("bucket"),
        *_soft_delete_fields("userbucketassignment"),
        # --- partial uniques (alive_q) ---
        migrations.AlterUniqueTogether(
            name="entitytypeschema",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="entitytypeschema",
            constraint=models.UniqueConstraint(
                fields=("tenant", "entity_type"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="entity_type_schemas_tenant_entity_type_uniq_alive",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="callattemptmatrix",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="callattemptmatrix",
            constraint=models.UniqueConstraint(
                fields=("tenant", "lead_type"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="call_attempt_matrix_tenant_lead_type_uniq_alive",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="bucket",
            unique_together=set(),
        ),
        migrations.AddConstraint(
            model_name="bucket",
            constraint=models.UniqueConstraint(
                fields=("tenant", "slug"),
                condition=Q(is_deleted=False) & Q(deleted_at__isnull=True),
                name="crm_records_bucket_tenant_slug_uniq_alive",
            ),
        ),
        migrations.RemoveConstraint(
            model_name="userbucketassignment",
            name="crm_records_uba_tenant_bucket_tenant_default_uniq",
        ),
        migrations.RemoveConstraint(
            model_name="userbucketassignment",
            name="crm_records_uba_tenant_user_bucket_uniq",
        ),
        migrations.AddConstraint(
            model_name="userbucketassignment",
            constraint=models.UniqueConstraint(
                fields=("tenant", "bucket"),
                condition=Q(user__isnull=True)
                & Q(is_deleted=False)
                & Q(deleted_at__isnull=True),
                name="crm_records_uba_tenant_bucket_tenant_default_uniq",
            ),
        ),
        migrations.AddConstraint(
            model_name="userbucketassignment",
            constraint=models.UniqueConstraint(
                fields=("tenant", "user", "bucket"),
                condition=Q(user__isnull=False)
                & Q(is_deleted=False)
                & Q(deleted_at__isnull=True),
                name="crm_records_uba_tenant_user_bucket_uniq",
            ),
        ),
        # --- managers ---
        *[
            migrations.AlterModelManagers(
                name=model_name,
                managers=[
                    ("objects", core.soft_delete.SoftDeleteManager()),
                    ("all_objects", core.soft_delete.AllObjectsManager()),
                ],
            )
            for model_name in (
                "record",
                "eventlog",
                "ruleset",
                "ruleexecutionlog",
                "partnerevent",
                "entitytypeschema",
                "scoringrule",
                "apisecretkey",
                "callattemptmatrix",
                "bucket",
                "userbucketassignment",
            )
        ],
    ]
