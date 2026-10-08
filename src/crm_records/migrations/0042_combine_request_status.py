"""
Combine request status + shipment status for inventory_request / unmannd_request.

VENDOR_IDENTIFIED → APPROVED
IN_SHIPPING       → DELIVERED / EXCEPTION when shipment_status says so, else ORDERED

status_text is rewritten only when it still holds a generated value (empty or the
old code / label); custom status texts are kept.
"""

from django.db import migrations

FORWARD_SQL = """
WITH mapped AS (
    SELECT
        id,
        CASE
            WHEN UPPER(TRIM(data->>'status')) = 'VENDOR_IDENTIFIED' THEN 'APPROVED'
            WHEN UPPER(TRIM(COALESCE(data->>'shipment_status', ''))) = 'DELIVERED' THEN 'DELIVERED'
            WHEN UPPER(TRIM(COALESCE(data->>'shipment_status', ''))) = 'EXCEPTION' THEN 'EXCEPTION'
            ELSE 'ORDERED'
        END AS new_status
    FROM records
    WHERE entity_type IN ('inventory_request', 'unmannd_request')
      AND UPPER(TRIM(COALESCE(data->>'status', ''))) IN ('VENDOR_IDENTIFIED', 'IN_SHIPPING')
)
UPDATE records r
SET data = r.data
    || jsonb_build_object('status', m.new_status)
    || CASE
        WHEN UPPER(REPLACE(TRIM(COALESCE(r.data->>'status_text', '')), ' ', '_'))
             IN ('', 'VENDOR_IDENTIFIED', 'IN_SHIPPING')
        THEN jsonb_build_object(
            'status_text',
            CASE m.new_status
                WHEN 'APPROVED' THEN 'Approved'
                WHEN 'DELIVERED' THEN 'Delivered'
                WHEN 'EXCEPTION' THEN 'Exception'
                ELSE 'Ordered'
            END
        )
        ELSE '{}'::jsonb
    END
FROM mapped m
WHERE r.id = m.id;
"""

REVERSE_SQL = """
UPDATE records
SET data = data
    || jsonb_build_object(
        'status',
        CASE WHEN UPPER(TRIM(data->>'status')) = 'APPROVED' THEN 'VENDOR_IDENTIFIED' ELSE 'IN_SHIPPING' END
    )
    || CASE
        WHEN TRIM(COALESCE(data->>'status_text', '')) IN ('', 'Approved', 'Ordered', 'Delivered', 'Exception')
        THEN jsonb_build_object(
            'status_text',
            CASE WHEN UPPER(TRIM(data->>'status')) = 'APPROVED' THEN 'VENDOR_IDENTIFIED' ELSE 'IN_SHIPPING' END
        )
        ELSE '{}'::jsonb
    END
WHERE entity_type IN ('inventory_request', 'unmannd_request')
  AND UPPER(TRIM(COALESCE(data->>'status', ''))) IN ('APPROVED', 'ORDERED', 'DELIVERED', 'EXCEPTION');
"""


class Migration(migrations.Migration):

    dependencies = [
        ("crm_records", "0041_entitytypeschema_status_config"),
    ]

    operations = [
        migrations.RunSQL(FORWARD_SQL, REVERSE_SQL),
    ]
