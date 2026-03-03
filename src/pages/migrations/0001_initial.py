from django.db import migrations, models
import django.db.models.deletion
import uuid


CREATE_PAGES_IF_NOT_EXISTS = """
CREATE TABLE IF NOT EXISTS public.pages (
    id UUID NOT NULL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    user_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    config JSONB NOT NULL DEFAULT '[]',
    role UUID NULL REFERENCES authz_role(id) ON DELETE SET NULL,
    tenant_id UUID NULL REFERENCES tenants(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS pages_tenant__138dc7_idx ON public.pages (tenant_id, user_id);
CREATE INDEX IF NOT EXISTS pages_tenant__7bb27b_idx ON public.pages (tenant_id, role);
"""

DROP_PAGES = """
DROP TABLE IF EXISTS public.pages;
"""


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('authz', '0011_add_indexes_for_name_company_name'),
        ('core', '0001_initial'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(CREATE_PAGES_IF_NOT_EXISTS, reverse_sql=DROP_PAGES),
            ],
            state_operations=[
                migrations.CreateModel(
                    name='Page',
                    fields=[
                        ('created_at', models.DateTimeField(auto_now_add=True, db_index=True)),
                        ('updated_at', models.DateTimeField(auto_now=True, db_index=True)),
                        ('id', models.UUIDField(
                            primary_key=True,
                            default=uuid.uuid4,
                            editable=False,
                            serialize=False
                        )),
                        ('user_id', models.UUIDField(
                            db_index=True,
                            help_text='Supabase auth user id (owner of this page).'
                        )),
                        ('name', models.CharField(max_length=255)),
                        ('config', models.JSONField(
                            default=list,
                            blank=True,
                            help_text='List of widget configs, e.g. [{"id": "...", "type": "ticketTable", "config": {...}}].'
                        )),
                        ('role', models.ForeignKey(
                            blank=True,
                            null=True,
                            db_column='role',
                            related_name='pages',
                            on_delete=django.db.models.deletion.SET_NULL,
                            to='authz.role',
                            help_text='Role this page is scoped to (from authz_role).'
                        )),
                        ('tenant', models.ForeignKey(
                            db_column='tenant_id',
                            related_name='pages',
                            on_delete=django.db.models.deletion.CASCADE,
                            to='core.tenant'
                        )),
                    ],
                    options={
                        'db_table': 'pages',
                        'ordering': ['-updated_at'],
                        'indexes': [
                            models.Index(fields=['tenant', 'user_id'], name='pages_tenant__138dc7_idx'),
                            models.Index(fields=['tenant', 'role'], name='pages_tenant__7bb27b_idx'),
                        ],
                    },
                ),
            ],
        ),
    ]
