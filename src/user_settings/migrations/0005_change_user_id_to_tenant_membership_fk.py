# Generated manually

from django.db import migrations, models
import django.db.models.deletion


def migrate_user_id_to_tenant_membership(apps, schema_editor):
    """Migrate user_id to tenant_membership_id by finding matching TenantMembership records"""
    UserSettings = apps.get_model('user_settings', 'UserSettings')
    TenantMembership = apps.get_model('authz', 'TenantMembership')
    
    # Find all UserSettings that need to be migrated
    user_settings_to_migrate = UserSettings.objects.all()
    
    migrated_count = 0
    deleted_count = 0
    
    for setting in user_settings_to_migrate:
        # Find matching TenantMembership by tenant and user_id
        tenant_membership = TenantMembership.objects.filter(
            tenant=setting.tenant,
            user_id=setting.user_id
        ).first()
        
        if tenant_membership:
            # Update the setting to use tenant_membership_id
            setting.tenant_membership_id = tenant_membership.id
            setting.save(update_fields=['tenant_membership_id'])
            migrated_count += 1
        else:
            # If no matching TenantMembership found, delete the setting
            setting.delete()
            deleted_count += 1
    
    print(f"Migrated {migrated_count} UserSettings records")
    if deleted_count > 0:
        print(f"Deleted {deleted_count} UserSettings records with no matching TenantMembership")


def reverse_migrate(apps, schema_editor):
    """Reverse migration: populate user_id from tenant_membership"""
    UserSettings = apps.get_model('user_settings', 'UserSettings')
    TenantMembership = apps.get_model('authz', 'TenantMembership')
    
    for setting in UserSettings.objects.select_related('tenant_membership').all():
        if setting.tenant_membership:
            setting.user_id = setting.tenant_membership.user_id
            setting.save(update_fields=['user_id'])


class Migration(migrations.Migration):

    dependencies = [
        ('user_settings', '0004_add_daily_target_and_daily_limit'),
        ('authz', '0005_alter_tenantmembership_user_id_and_more'),
    ]

    operations = [
        # Step 1: Add the new tenant_membership_id column (nullable initially)
        migrations.AddField(
            model_name='usersettings',
            name='tenant_membership',
            field=models.ForeignKey(
                'authz.TenantMembership',
                null=True,
                blank=True,
                on_delete=django.db.models.deletion.CASCADE,
                db_column='tenant_membership_id',
                help_text='The tenant membership this setting belongs to'
            ),
        ),
        # Step 2: Migrate data
        migrations.RunPython(migrate_user_id_to_tenant_membership, reverse_migrate),
        # Step 3: Remove the old index that includes user_id
        migrations.RemoveIndex(
            model_name='usersettings',
            name='user_settin_tenant__fb0e9c_idx',
        ),
        # Step 4: Remove the old unique_together constraint
        migrations.AlterUniqueTogether(
            name='usersettings',
            unique_together=set(),
        ),
        # Step 5: Remove old user_id column (this will also drop any remaining indexes on user_id)
        migrations.RemoveField(
            model_name='usersettings',
            name='user_id',
        ),
        # Step 6: Make tenant_membership_id NOT NULL
        migrations.AlterField(
            model_name='usersettings',
            name='tenant_membership',
            field=models.ForeignKey(
                'authz.TenantMembership',
                on_delete=django.db.models.deletion.CASCADE,
                db_column='tenant_membership_id',
                help_text='The tenant membership this setting belongs to'
            ),
        ),
        # Step 7: Add new unique_together constraint
        migrations.AlterUniqueTogether(
            name='usersettings',
            unique_together={('tenant', 'tenant_membership', 'key')},
        ),
        # Step 8: Add new index for tenant + tenant_membership
        migrations.AddIndex(
            model_name='usersettings',
            index=models.Index(fields=['tenant', 'tenant_membership'], name='user_settin_tenant__tenant_m_idx'),
        ),
        # Note: The index on ['tenant', 'key'] already exists as 'user_settin_tenant__ebf4d8_idx' and doesn't need to be recreated
    ]

