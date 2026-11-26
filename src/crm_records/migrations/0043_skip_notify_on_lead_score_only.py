from django.db import migrations

# Bulk lead scoring UPDATEs only touch data.lead_score (+ updated_at) but still
# fired pyro_record_changed for every row, flooding the websocket service.
# Skip NOTIFY when:
# - session asks to skip (pyro.skip_record_notify=1), or
# - UPDATE only changed lead_score inside data (ignore updated_at).

NOTIFY_SQL = """
CREATE OR REPLACE FUNCTION pyro_notify_record_change()
RETURNS TRIGGER AS $$
BEGIN
  IF current_setting('pyro.skip_record_notify', true) = '1' THEN
    RETURN NEW;
  END IF;

  IF TG_OP = 'UPDATE'
     AND (OLD.data - 'lead_score') IS NOT DISTINCT FROM (NEW.data - 'lead_score')
     AND OLD.entity_type IS NOT DISTINCT FROM NEW.entity_type
     AND OLD.tenant_id IS NOT DISTINCT FROM NEW.tenant_id
  THEN
    RETURN NEW;
  END IF;

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
"""

REVERSE_SQL = """
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
"""


class Migration(migrations.Migration):
    dependencies = [
        ("crm_records", "0042_combine_request_status"),
    ]

    operations = [
        migrations.RunSQL(NOTIFY_SQL, REVERSE_SQL),
    ]
