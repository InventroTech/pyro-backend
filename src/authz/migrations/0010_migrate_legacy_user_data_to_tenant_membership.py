# Generated migration to copy data from LegacyUser (public.users) to TenantMembership (authz_tenantmembership)

from django.db import migrations


def migrate_legacy_user_data(apps, schema_editor):
    """
    Copy name and company_name from LegacyUser (public.users) to TenantMembership (authz_tenantmembership).
    Matches by tenant_id + email.
    No-op when LegacyUser has already been removed (e.g. fresh DB after accounts.0004).
    """
    try:
        LegacyUser = apps.get_model('accounts', 'LegacyUser')
    except LookupError:
        # LegacyUser already dropped (e.g. accounts.0004 ran); nothing to migrate
        return

    TenantMembership = apps.get_model('authz', 'TenantMembership')
    Tenant = apps.get_model('core', 'Tenant')

    updated_count = 0
    skipped_count = 0

    # Get all legacy users
    legacy_users = LegacyUser.objects.all()

    for legacy_user in legacy_users:
        # In migrations, access tenant_id directly (db_column) or use getattr
        tenant_id = getattr(legacy_user, 'tenant_id', None)
        email = getattr(legacy_user, 'email', None)
        
        if not tenant_id or not email:
            skipped_count += 1
            continue
        
        # Get the tenant instance
        try:
            tenant = Tenant.objects.get(id=tenant_id)
        except Tenant.DoesNotExist:
            skipped_count += 1
            continue
        
        # Find matching TenantMembership by tenant and email
        email_normalized = email.lower().strip() if email else None
        if not email_normalized:
            skipped_count += 1
            continue
            
        memberships = TenantMembership.objects.filter(
            tenant=tenant,
            email=email_normalized
        )
        
        if memberships.exists():
            # Update all matching memberships with name and company_name
            for membership in memberships:
                name = getattr(legacy_user, 'name', None)
                company_name = getattr(legacy_user, 'company_name', None)
                
                if not membership.name and name:
                    membership.name = name
                if not membership.company_name and company_name:
                    membership.company_name = company_name
                membership.save()
                updated_count += 1
        else:
            skipped_count += 1
    
    print(f"Migration complete: Updated {updated_count} TenantMembership records, skipped {skipped_count} LegacyUser records")


def reverse_migration(apps, schema_editor):
    """
    Reverse migration: Clear name and company_name from TenantMembership.
    Note: This doesn't restore data to LegacyUser, just clears TenantMembership fields.
    """
    TenantMembership = apps.get_model('authz', 'TenantMembership')
    TenantMembership.objects.all().update(name=None, company_name=None)
    print("Reversed migration: Cleared name and company_name from TenantMembership")


class Migration(migrations.Migration):

    dependencies = [
        ('authz', '0009_add_name_company_to_tenant_membership'),
        ('accounts', '0004_drop_legacy_users_and_roles'),  # Run after accounts; forward no-ops when LegacyUser is gone
    ]

    operations = [
        migrations.RunPython(migrate_legacy_user_data, reverse_migration),
    ]
