from django.db import migrations

NOTIFY_SQL = """
CREATE OR REPLACE FUNCTION pyro_notify_record_change()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify(
    'pyro_record_changed',
    json_build_object(
      'id', NEW.id,
      'tenant_id', NEW.tenant_id,
      'entity_type', NEW.entity_type
    )::text
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS records_pyro_notify_change ON records;
CREATE TRIGGER records_pyro_notify_change
  AFTER INSERT OR UPDATE ON records
  FOR EACH ROW
  EXECUTE FUNCTION pyro_notify_record_change();
"""

REVERSE_SQL = """
DROP TRIGGER IF EXISTS records_pyro_notify_change ON records;
DROP FUNCTION IF EXISTS pyro_notify_record_change();
"""


class Migration(migrations.Migration):
    dependencies = [
        ("crm_records", "0036_add_records_updated_at_id_idx"),
    ]

    operations = [
        migrations.RunSQL(NOTIFY_SQL, REVERSE_SQL),
    ]
