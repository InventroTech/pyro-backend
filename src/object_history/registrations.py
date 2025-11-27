from support_ticket.models import SupportTicket
from crm_records.models import Record

from .registry import register

register(
    SupportTicket,
    track_fields=[
        "resolution_status",
        "call_status",
        "assigned_to_id",
        "cse_name",
        "cse_remarks",
        "call_attempts",
        "snooze_until",
        "review_requested",
    ],
    redact_fields={"cse_remarks"},
    snapshot_strategy="minimal",
)

register(
    Record,
    track_fields=[
        "entity_type",
        "name",
        "data",  # include JSON payload for diffs
        "tenant_id",
    ],
    redact_fields=set(),
    snapshot_strategy="minimal",
)

