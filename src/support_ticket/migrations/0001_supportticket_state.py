from django.db import migrations, models
from django.utils import timezone
from django.contrib.postgres.fields import ArrayField

class Migration(migrations.Migration):
    initial = False
    dependencies = [
        ('core', '__latest__'),
        ('accounts', '__latest__'),
    ]
    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.CreateModel(
                    name='SupportTicket',
                    fields=[
                        ('id', models.BigAutoField(primary_key=True, serialize=False)),
                        ('created_at', models.DateTimeField(default=timezone.now)),
                        ('ticket_date', models.DateTimeField(blank=True, null=True)),
                        ('user_id', models.CharField(max_length=255, blank=True, null=True)),
                        ('name', models.CharField(max_length=255, blank=True, null=True)),
                        ('phone', models.CharField(max_length=50, blank=True, null=True)),
                        ('source', models.CharField(max_length=255, blank=True, null=True)),
                        ('subscription_status', models.TextField(blank=True, null=True)),
                        ('atleast_paid_once', models.BooleanField(blank=True, null=True)),
                        ('reason', models.TextField(blank=True, null=True)),
                        ('other_reasons', ArrayField(models.TextField(), blank=True, null=True)),
                        ('badge', models.CharField(max_length=255, blank=True, null=True)),
                        ('poster', models.CharField(max_length=255, blank=True, null=True)),
                        ('layout_status', models.CharField(max_length=255, blank=True, null=True)),
                        ('resolution_status', models.CharField(max_length=255, blank=True, null=True)),
                        ('resolution_time', models.CharField(max_length=255, blank=True, null=True)),
                        ('cse_name', models.CharField(max_length=255, blank=True, null=True)),
                        ('cse_remarks', models.TextField(blank=True, null=True)),
                        ('call_status', models.CharField(max_length=255, blank=True, null=True)),
                        ('call_attempts', models.IntegerField(default=0, blank=True, null=True)),
                        ('rm_name', models.TextField(blank=True, null=True)),
                        ('completed_at', models.DateTimeField(blank=True, null=True)),
                        ('snooze_until', models.DateTimeField(blank=True, null=True)),
                        ('praja_dashboard_user_link', models.TextField(blank=True, null=True)),
                        ('display_pic_url', models.TextField(blank=True, null=True)),
                        ('dumped_at', models.DateTimeField(blank=True, null=True)),
                        ('tenant', models.ForeignKey(
                            to='core.tenant', db_column='tenant_id',
                            on_delete=models.DO_NOTHING, null=True, blank=True,
                            related_name='support_tickets',
                        )),
                        ('assigned_to', models.ForeignKey(
                            to='accounts.supabaseauthuser', db_column='assigned_to',
                            on_delete=models.CASCADE, null=True, blank=True,
                            related_name='assigned_tickets',
                        )),
                    ],
                    options={'db_table': 'support_ticket', 'managed': False},
                ),
            ],
            database_operations=[],   # <- NO DB TOUCH
        ),
    ]
