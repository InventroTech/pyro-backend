"""Shared data and helpers for the make-closed lead tests."""

from crm_records.models import Record
from tests.base.test_setup import BaseAPITestCase

MAKE_CLOSED_URL = "/crm-records/leads/make-closed/"

# Cleared by make-closed: anything that would hand the lead back to an RM.
SCHEDULE_KEYS = ("assigned_to", "next_call_at", "snooze_unassign_at", "not_connected_unassign_at")

# Kept by make-closed: the lead's call history.
HISTORY_KEYS = (
    "call_attempts",
    "last_call_outcome",
    "first_assigned_at",
    "first_assigned_to",
    "first_assigned_today_at",
    "first_assignment_today_date",
)


def worked_lead_data(**extra):
    """A lead an RM has already worked on, with every schedule and history key set."""
    data = {
        "name": "Worked Lead",
        "praja_id": "9001",
        "lead_source": "referral",
        "lead_stage": "NOT_CONNECTED",
        "call_attempts": 3,
        "assigned_to": "rm-123",
        "last_call_outcome": "not_connected",
        "next_call_at": "2026-10-28T10:00:00+00:00",
        "snooze_unassign_at": "2026-10-28T12:00:00+00:00",
        "not_connected_unassign_at": "2026-10-28T12:00:00+00:00",
        "first_assigned_at": "2026-10-26T04:00:00+00:00",
        "first_assigned_to": "rm-123",
        "first_assigned_today_at": "2026-10-28T04:00:00+00:00",
        "first_assignment_today_date": "2026-10-28",
    }
    data.update(extra)
    return data


class MakeClosedAPITestCase(BaseAPITestCase):
    """Authenticated client for POST /crm-records/leads/make-closed/."""

    def setUp(self):
        super().setUp()
        self.client.force_authenticate(user=self.user)
        self.headers = {**self.auth_headers, "HTTP_X_TENANT_SLUG": self.tenant.slug}

    def post_ids(self, praja_ids, headers=None):
        return self.post_body({"praja_ids": praja_ids}, headers=headers)

    def post_body(self, body, headers=None):
        return self.client.post(MAKE_CLOSED_URL, body, format="json", **(headers or self.headers))

    def lead(self, tenant=None, **extra):
        return Record.objects.create(
            tenant=tenant or self.tenant, entity_type="lead", data=worked_lead_data(**extra)
        )

    def raw_record(self, data, entity_type="lead", tenant=None):
        return Record.objects.create(tenant=tenant or self.tenant, entity_type=entity_type, data=data)
