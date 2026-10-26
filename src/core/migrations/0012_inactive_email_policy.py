import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0011_drop_tenant_users_and_party"),
    ]

    operations = [
        migrations.AddField(
            model_name="tenantsettings",
            name="inactive_emails_enabled",
            field=models.BooleanField(
                default=True,
                help_text=(
                    "When enabled, an email with no login for inactive_login_days "
                    "consecutive days is marked inactive."
                ),
            ),
        ),
        migrations.AddField(
            model_name="tenantsettings",
            name="inactive_login_days",
            field=models.PositiveIntegerField(
                default=72,
                help_text="Consecutive days without a login before an email is marked inactive.",
                validators=[
                    django.core.validators.MinValueValidator(1),
                    django.core.validators.MaxValueValidator(3650),
                ],
            ),
        ),
    ]
