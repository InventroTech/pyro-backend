# Generated migration to update pages.role foreign key constraint
# Migration: Update foreign key constraint on pages.role to reference authz_role
# Reason: We're migrating from public.roles to authz_role (Django model)
# The FK constraint needs to point to the new authz_role table instead of public.roles

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('authz', '0011_add_indexes_for_name_company_name'),
    ]

    operations = [
        migrations.RunSQL(
            # Drop the old foreign key constraint that references public.roles
            sql="""
                ALTER TABLE IF EXISTS public.pages 
                DROP CONSTRAINT IF EXISTS pages_role_fkey;
            """,
            reverse_sql="""
                -- Reverse: Recreate old FK constraint (if needed for rollback)
                -- Note: This may fail if public.roles table doesn't exist
                ALTER TABLE IF EXISTS public.pages
                ADD CONSTRAINT pages_role_fkey 
                FOREIGN KEY (role) 
                REFERENCES public.roles(id) 
                ON DELETE SET NULL;
            """
        ),
        migrations.RunSQL(
            # Create new foreign key constraint pointing to authz_role table
            sql="""
                ALTER TABLE IF EXISTS public.pages
                ADD CONSTRAINT pages_role_fkey 
                FOREIGN KEY (role) 
                REFERENCES authz_role(id) 
                ON DELETE SET NULL;
            """,
            reverse_sql="""
                -- Reverse: Drop the new FK constraint
                ALTER TABLE IF EXISTS public.pages 
                DROP CONSTRAINT IF EXISTS pages_role_fkey;
            """
        ),
    ]
