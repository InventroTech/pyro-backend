# support_ticket/migrations/0006_add_foreign_keys_if_missing.py
from django.db import migrations

ADD_TENANT_FK = """
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON kcu.constraint_name = tc.constraint_name
     AND kcu.table_schema = tc.table_schema
    WHERE tc.table_schema = 'public'
      AND tc.table_name   = 'support_ticket'
      AND tc.constraint_type = 'FOREIGN KEY'
      AND kcu.column_name = 'tenant_id'
  ) THEN
    ALTER TABLE public.support_ticket
      ADD CONSTRAINT support_ticket_tenant_id_fkey
      FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) ON DELETE NO ACTION;
  END IF;
END $$;
"""

ADD_ASSIGNED_TO_FK = """
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON kcu.constraint_name = tc.constraint_name
     AND kcu.table_schema = tc.table_schema
    WHERE tc.table_schema = 'public'
      AND tc.table_name   = 'support_ticket'
      AND tc.constraint_type = 'FOREIGN KEY'
      AND kcu.column_name = 'assigned_to'
  ) THEN
    ALTER TABLE public.support_ticket
      ADD CONSTRAINT support_ticket_assigned_to_auth_users_id_fkey_cascade
      FOREIGN KEY (assigned_to) REFERENCES auth.users(id) ON DELETE CASCADE;
  END IF;
END $$;
"""

class Migration(migrations.Migration):
    dependencies = [('support_ticket', '0005_managed_true')]
    operations = [
        migrations.RunSQL(ADD_TENANT_FK, reverse_sql="ALTER TABLE public.support_ticket DROP CONSTRAINT IF EXISTS support_ticket_tenant_id_fkey;"),
        migrations.RunSQL(ADD_ASSIGNED_TO_FK, reverse_sql="ALTER TABLE public.support_ticket DROP CONSTRAINT IF EXISTS support_ticket_assigned_to_auth_users_id_fkey_cascade;"),
    ]
