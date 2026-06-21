from django.db import migrations, models


def forwards_rename_close_stale_job_type(apps, schema_editor):
    BackgroundJob = apps.get_model("background_jobs", "BackgroundJob")
    BackgroundJob.objects.filter(
        job_type="close_stale_subscription_leads",
    ).update(job_type="close_stale_self_trial_support_tickets")


def backwards_rename_close_stale_job_type(apps, schema_editor):
    BackgroundJob = apps.get_model("background_jobs", "BackgroundJob")
    BackgroundJob.objects.filter(
        job_type="close_stale_self_trial_support_tickets",
    ).update(job_type="close_stale_subscription_leads")


class Migration(migrations.Migration):

    dependencies = [
        ("background_jobs", "0017_add_discover_entity_types_job_type"),
    ]

    operations = [
        migrations.RunPython(
            forwards_rename_close_stale_job_type,
            backwards_rename_close_stale_job_type,
        ),
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
                    (
                        "close_stale_self_trial_support_tickets",
                        "Close Stale Self Trial Support Tickets",
                    ),
                    ("snoozed_to_not_connected_midnight", "Snoozed To Not Connected (midnight)"),
                    ("purge_old_log_tables", "Purge Old Log Tables"),
                    ("sync_dispatch_to_records", "Sync Dispatch To Records"),
                    ("process_dumped_tickets", "Process Dumped Support Tickets"),
                    ("discover_entity_types", "Discover Entity Types"),
                ],
                db_index=True,
                help_text="Type of job to execute",
                max_length=50,
            ),
        ),
    ]
