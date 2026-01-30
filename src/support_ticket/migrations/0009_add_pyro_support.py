# Generated manually for PyroSupport (Submit Ticket form)

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('support_ticket', '0008_add_state_field'),
    ]

    operations = [
        migrations.CreateModel(
            name='PyroSupport',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('full_name', models.CharField(max_length=255)),
                ('email_address', models.EmailField(max_length=255)),
                ('subject', models.CharField(max_length=500)),
                ('category', models.CharField(max_length=100)),
                ('priority', models.CharField(max_length=50)),
                ('description', models.TextField()),
                ('status', models.CharField(default='Open', max_length=50)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'pyro_support',
                'managed': True,
                'ordering': ['-created_at'],
            },
        ),
        migrations.AddIndex(
            model_name='pyrosupport',
            index=models.Index(fields=['status'], name='pyro_support_status'),
        ),
        migrations.AddIndex(
            model_name='pyrosupport',
            index=models.Index(fields=['category'], name='pyro_support_category'),
        ),
        migrations.AddIndex(
            model_name='pyrosupport',
            index=models.Index(fields=['-created_at'], name='pyro_support_created'),
        ),
    ]
