# Migration to add poster field to existing records
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('crm_records', '0004_ruleexecutionlog'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
                -- Update all records where poster field doesn't exist or is empty
                -- Valid poster values: 'in_trail', 'paid', 'in_trial_extension', 'premium_extension', 
                -- 'trial_expired', 'premium_expired', 'grace', 'auto_pay_not_set_up', 'free'
                -- Randomly assign one of the valid poster values to existing records
                UPDATE records
                SET data = jsonb_set(
                    COALESCE(data, '{}'::jsonb),
                    '{poster}',
                    CASE (FLOOR(random() * 9)::int)
                        WHEN 0 THEN '"in_trail"'::jsonb
                        WHEN 1 THEN '"paid"'::jsonb
                        WHEN 2 THEN '"in_trial_extension"'::jsonb
                        WHEN 3 THEN '"premium_extension"'::jsonb
                        WHEN 4 THEN '"trial_expired"'::jsonb
                        WHEN 5 THEN '"premium_expired"'::jsonb
                        WHEN 6 THEN '"grace"'::jsonb
                        WHEN 7 THEN '"auto_pay_not_set_up"'::jsonb
                        ELSE '"free"'::jsonb
                    END,
                    true
                )
                WHERE data->>'poster' IS NULL
                   OR data->>'poster' = '';
            """,
            reverse_sql="""
                -- Remove poster field from all records (reverse migration)
                UPDATE records
                SET data = data - 'poster'
                WHERE data ? 'poster';
            """,
        ),
    ]

