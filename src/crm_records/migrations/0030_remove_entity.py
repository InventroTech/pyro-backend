# Generated migration to remove Entity model from crm_records (moved to core) and drop old table

from django.db import migrations


def drop_old_entity_table(apps, schema_editor):
    """Drop the old crm_records_entity table"""
    if schema_editor.connection.alias != 'default':
        return
    
    with schema_editor.connection.cursor() as cursor:
        cursor.execute("""
            DROP TABLE IF EXISTS crm_records_entity CASCADE;
        """)


def reverse_drop_table(apps, schema_editor):
    """This is irreversible - we can't recreate the table on rollback"""
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0006_migrate_entity_data'),
        ('crm_records', '0029_entity'),
    ]

    operations = [
        migrations.DeleteModel(
            name='Entity',
        ),
        migrations.RunPython(drop_old_entity_table, reverse_drop_table),
    ]
