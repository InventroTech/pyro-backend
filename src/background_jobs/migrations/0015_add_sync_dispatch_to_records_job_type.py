# Generated for sync_dispatch_to_records job type

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("background_jobs", "0014_alter_backgroundjob_managers_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="backgroundjob",
            name="job_type",
            field=models.CharField(
                choices=[
                    ("send_mixpanel_event", "Send Mixpanel Event"),
                    ("send_rm_assigned_event", "Send RM Assigned Event"),
                    ("send_webhook", "Send Webhook"),
                    ("execute_function", "Execute Function"),
                    ("score_leads", "Score Leads"),
                    ("score_leads_chunk", "Score Leads Chunk"),
                    ("send_to_praja", "Send to Praja Server"),
                    ("partner_lead_assign", "Partner Lead Assign"),
                    ("unassign_snoozed_leads", "Unassign Snoozed Leads"),
                    ("release_leads_after_12h", "Release Leads After 12h"),
                    ("close_stale_subscription_leads", "Close Stale Subscription Leads"),
                    ("snoozed_to_not_connected_midnight", "Snoozed To Not Connected (midnight)"),
                    ("purge_old_log_tables", "Purge Old Log Tables"),
                    ("sync_dispatch_to_records", "Sync Dispatch To Records"),
                ],
                db_index=True,
                help_text="Type of job to execute",
                max_length=50,
            ),
        ),
    ]
