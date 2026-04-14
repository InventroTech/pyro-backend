# Data migration to copy Entity data from crm_records_entity to core_entity

from django.db import migrations


def copy_entity_data(apps, schema_editor):
    """Copy data from old crm_records_entity table to new core_entity table"""
    if schema_editor.connection.alias != 'default':
        return
    
    with schema_editor.connection.cursor() as cursor:
        # Check if old table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'crm_records_entity'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            # Copy data from old table to new table
            cursor.execute("""
                INSERT INTO core_entity (id, name, schema, last_processed_record_id, created_at, updated_at, tenant_id)
                SELECT id, name, schema, last_processed_record_id, created_at, updated_at, tenant_id
                FROM crm_records_entity
                WHERE NOT EXISTS (
                    SELECT 1 FROM core_entity ce 
                    WHERE ce.id = crm_records_entity.id
                );
            """)


def reverse_copy_entity_data(apps, schema_editor):
    """Remove data from core_entity (in case of migration rollback)"""
    if schema_editor.connection.alias != 'default':
        return
    
    with schema_editor.connection.cursor() as cursor:
        cursor.execute("DELETE FROM core_entity;")


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0005_entity'),
        ('crm_records', '0029_entity'),
    ]

    operations = [
        migrations.RunPython(copy_entity_data, reverse_copy_entity_data),
    ]
