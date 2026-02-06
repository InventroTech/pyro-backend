# Key RoutingRule by authz.TenantMembership id instead of user_id (UUID).
# Smooth transition: where authz_tenantmembership.user_id equals rule.user_id,
# set tenant_membership_id; rules with no matching membership are removed.

from django.db import migrations, models
import django.db.models.deletion


def backfill_tenant_membership(apps, schema_editor):
    RoutingRule = apps.get_model("user_settings", "RoutingRule")
    TenantMembership = apps.get_model("authz", "TenantMembership")
    for rule in RoutingRule.objects.all():
        if not rule.user_id:
            continue
        tm = TenantMembership.objects.filter(
            tenant_id=rule.tenant_id,
            user_id=rule.user_id,
        ).first()
        if tm:
            rule.tenant_membership_id = tm.id
            rule.save(update_fields=["tenant_membership_id"])
        else:
            # No matching membership; remove rule so we can enforce unique on tenant_membership
            rule.delete()


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("authz", "0008_change_user_parent_id_to_foreignkey"),
        ("user_settings", "0009_backfill_lead_sources_from_separate_rows"),
    ]

    operations = [
        # 1. Add nullable tenant_membership FK
        migrations.AddField(
            model_name="routingrule",
            name="tenant_membership",
            field=models.ForeignKey(
                blank=True,
                db_column="tenant_membership_id",
                help_text="The tenant membership this rule applies to (primary); use this so rules work when user has not logged in yet.",
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                to="authz.tenantmembership",
            ),
        ),
        # 2. Make user_id nullable
        migrations.AlterField(
            model_name="routingrule",
            name="user_id",
            field=models.UUIDField(
                blank=True,
                help_text="Denormalized Supabase user UUID from TenantMembership when linked; may be null if membership has no auth user yet.",
                null=True,
            ),
        ),
        # 3. Backfill tenant_membership from user_id; delete rules that can't be matched
        migrations.RunPython(backfill_tenant_membership, noop_reverse),
        # 4. Remove old index that included user_id
        migrations.RemoveIndex(
            model_name="routingrule",
            name="routing_rul_tenant__80c28d_idx",
        ),
        # 5. Change unique_together from (tenant, user_id, queue_type) to (tenant, tenant_membership, queue_type)
        migrations.AlterUniqueTogether(
            name="routingrule",
            unique_together={("tenant", "tenant_membership", "queue_type")},
        ),
        # 6. Make tenant_membership required (non-null) now that backfill is done
        migrations.AlterField(
            model_name="routingrule",
            name="tenant_membership",
            field=models.ForeignKey(
                db_column="tenant_membership_id",
                help_text="The tenant membership this rule applies to (primary); use this so rules work when user has not logged in yet.",
                on_delete=django.db.models.deletion.CASCADE,
                to="authz.tenantmembership",
            ),
        ),
        # 7. Add new index for listing by tenant, queue_type, tenant_membership
        migrations.AddIndex(
            model_name="routingrule",
            index=models.Index(
                fields=["tenant", "queue_type", "tenant_membership"],
                name="routing_rul_tenant__tm_idx",
            ),
        ),
    ]
