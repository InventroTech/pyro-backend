import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0012_inactive_email_policy"),
    ]

    operations = [
        migrations.RenameField(
            model_name="tenantsettings",
            old_name="inactive_login_days",
            new_name="inactive_login_hours",
        ),
        migrations.AlterField(
            model_name="tenantsettings",
            name="inactive_emails_enabled",
            field=models.BooleanField(
                default=True,
                help_text=(
                    "When enabled, the settings user list counts an email as inactive "
                    "after inactive_login_hours without a login. Memberships stay active."
                ),
            ),
        ),
        migrations.AlterField(
            model_name="tenantsettings",
            name="inactive_login_hours",
            field=models.PositiveIntegerField(
                default=72,
                help_text="Consecutive hours without a login before the settings list counts an email as inactive.",
                validators=[
                    django.core.validators.MinValueValidator(1),
                    django.core.validators.MaxValueValidator(8760),
                ],
            ),
        ),
    ]
