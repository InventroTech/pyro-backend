# Generated manually for Zoho connection history rows.

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("email_protocol", "0001_zoho_mail_shipment_sync"),
        ("core", "0011_drop_tenant_users_and_party"),
    ]

    operations = [
        migrations.AddField(
            model_name="zohomailconnection",
            name="disconnected_at",
            field=models.DateTimeField(blank=True, db_index=True, null=True),
        ),
        migrations.AlterField(
            model_name="zohomailconnection",
            name="tenant",
            field=models.ForeignKey(
                db_column="tenant_id",
                on_delete=django.db.models.deletion.CASCADE,
                related_name="zoho_mail_connections",
                to="core.tenant",
            ),
        ),
        migrations.AddConstraint(
            model_name="zohomailconnection",
            constraint=models.UniqueConstraint(
                condition=models.Q(("disconnected_at__isnull", True), ("is_active", True)),
                fields=("tenant",),
                name="uniq_active_zoho_mail_connection_per_tenant",
            ),
        ),
    ]
