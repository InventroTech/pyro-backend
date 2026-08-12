from django.db import migrations


def _insert_after_lead_score(order: list) -> list:
    """Insert '-network_density' immediately after '-lead_score' in the order list."""
    if "-network_density" in order or "network_density" in order:
        return order
    result = []
    for token in order:
        result.append(token)
        if token in ("-lead_score", "lead_score", "-lead_score_for_sort", "lead_score_for_sort"):
            result.append("-network_density")
    return result


def backfill_network_density(apps, schema_editor):
    UserBucketAssignment = apps.get_model("crm_records", "UserBucketAssignment")
    to_update = []
    for assignment in UserBucketAssignment.objects.all():
        strategy = assignment.pull_strategy or {}
        order = strategy.get("order")
        if not isinstance(order, list):
            continue
        new_order = _insert_after_lead_score(order)
        if new_order != order:
            assignment.pull_strategy = {**strategy, "order": new_order}
            to_update.append(assignment)
    if to_update:
        UserBucketAssignment.objects.bulk_update(to_update, ["pull_strategy"])


class Migration(migrations.Migration):

    dependencies = [
        ("crm_records", "0039_add_analytics_query_indexes"),
    ]

    operations = [
        migrations.RunPython(backfill_network_density, migrations.RunPython.noop),
    ]
