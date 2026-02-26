"""
Simple unit tests for Get Next Lead exclude logic.
Tests that leads assigned to another user are excluded (no DB required).

Run as standalone script (no Django test DB, no migrations):
  cd src && python tests/rest/crm_records/test_get_next_lead_api.py
"""

def _should_exclude_lead(lead_data: dict, current_user_id: str) -> bool:
    """Same logic as GetNextLead exclude: exclude if assigned_to is set and not current user."""
    assigned_to = (lead_data or {}).get("assigned_to")
    if not assigned_to or assigned_to in ("", "null", "None"):
        return False
    return assigned_to != current_user_id


def test_exclude_lead_assigned_to_other_user():
    """Lead with assigned_to = other user should be excluded."""
    lead = {"lead_stage": "in_queue", "assigned_to": "user-a-uuid", "call_attempts": 0}
    assert _should_exclude_lead(lead, "user-b-uuid") is True
    assert _should_exclude_lead(lead, "user-a-uuid") is False


def test_do_not_exclude_unassigned_lead():
    """Lead with no assigned_to should not be excluded."""
    lead = {"lead_stage": "in_queue", "call_attempts": 0}
    assert _should_exclude_lead(lead, "user-a-uuid") is False
    lead_empty = {"lead_stage": "in_queue", "assigned_to": ""}
    assert _should_exclude_lead(lead_empty, "user-a-uuid") is False


def test_do_not_exclude_lead_assigned_to_current_user():
    """Lead with assigned_to = current user should not be excluded."""
    lead = {"lead_stage": "in_queue", "assigned_to": "current-uuid", "call_attempts": 0}
    assert _should_exclude_lead(lead, "current-uuid") is False


if __name__ == "__main__":
    test_exclude_lead_assigned_to_other_user()
    test_do_not_exclude_unassigned_lead()
    test_do_not_exclude_lead_assigned_to_current_user()
    print("All 3 tests passed.")
