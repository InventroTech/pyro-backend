from django.db import migrations, models

_OLD_JOB_TYPE = "close_stale_subscription_leads"
_NEW_JOB_TYPE = "close_stale_self_trial_support_tickets"
_ACTIVE_STATUSES = ("PENDING", "RETRYING", "PROCESSING")
_RENAME_BATCH_SIZE = 500


def _rename_active_job_type(apps, schema_editor, old_type: str, new_type: str) -> None:
    """
    Rename only active queue rows.

    Completed historical jobs keep the old ``job_type`` string; they are never
    reprocessed and renaming them can scan/update a very large table during deploy.
    """
    connection = schema_editor.connection
    if connection.vendor == "postgresql":
        with connection.cursor() as cursor:
            while True:
                cursor.execute(
                    """
                    UPDATE background_jobs AS b
                    SET job_type = %s
                    FROM (
                        SELECT id
                        FROM background_jobs
                        WHERE job_type = %s
                          AND status = ANY(%s)
                        LIMIT %s
                    ) AS batch
                    WHERE b.id = batch.id
                    """,
                    [new_type, old_type, list(_ACTIVE_STATUSES), _RENAME_BATCH_SIZE],
                )
                if cursor.rowcount == 0:
                    break
        return

    BackgroundJob = apps.get_model("background_jobs", "BackgroundJob")
    while True:
        batch_ids = list(
            BackgroundJob.objects.filter(
                job_type=old_type,
                status__in=_ACTIVE_STATUSES,
            ).values_list("id", flat=True)[:_RENAME_BATCH_SIZE]
        )
        if not batch_ids:
            break
        BackgroundJob.objects.filter(id__in=batch_ids).update(job_type=new_type)


def forwards_rename_close_stale_job_type(apps, schema_editor):
    _rename_active_job_type(apps, schema_editor, _OLD_JOB_TYPE, _NEW_JOB_TYPE)


def backwards_rename_close_stale_job_type(apps, schema_editor):
    _rename_active_job_type(apps, schema_editor, _NEW_JOB_TYPE, _OLD_JOB_TYPE)


class Migration(migrations.Migration):
    atomic = False

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
