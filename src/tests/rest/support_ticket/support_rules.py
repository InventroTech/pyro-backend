"""
Test-only helpers to seed support ticket ``RuleSet`` rows (production parity).

Leads do not ship rule definitions in ``src/``; they unit-test rule actions directly
(see ``test_rule_engine_call_back_later.py``). Full ``execute_rules`` integration
tests for support tickets seed rules here.
"""

from __future__ import annotations

from typing import Any, Dict, List

from crm_records.models import RuleSet

SUPPORT_TICKET_TEST_RULES: List[Dict[str, Any]] = [
    {
        "event_name": "support.cannot_resolve",
        "condition": {},
        "description": "Support cannot resolve (SaveAndContinueView)",
        "actions": [
            {
                "action": "update_fields",
                "args": {
                    "updates": {
                        "cse_name": "{{payload.cse_name}}",
                        "assigned_to": "{{payload.assigned_to}}",
                        "call_status": "{{payload.call_status}}",
                        "cse_remarks": "{{payload.cse_remarks}}",
                        "completed_at": "{{now}}",
                        "other_reasons": "{{payload.other_reasons}}",
                        "resolution_time": "{{payload.resolution_time}}",
                        "review_requested": "{{payload.review_requested}}",
                        "resolution_status": "Can't Resolve",
                    },
                    "increments": {"call_attempts": 1},
                },
            }
        ],
    },
    {
        "event_name": "support.call_later",
        "condition": {},
        "description": "Support call later / WIP (SaveAndContinueView)",
        "actions": [
            {
                "action": "update_fields",
                "args": {
                    "updates": {
                        "cse_name": "{{payload.cse_name}}",
                        "assigned_to": "{{payload.assigned_to}}",
                        "call_status": "{{payload.call_status}}",
                        "cse_remarks": "{{payload.cse_remarks}}",
                        "completed_at": "{{now}}",
                        "other_reasons": "{{payload.other_reasons}}",
                        "resolution_time": "{{payload.resolution_time}}",
                        "review_requested": "{{payload.review_requested}}",
                        "resolution_status": "WIP",
                    },
                    "increments": {"call_attempts": 1},
                },
            }
        ],
    },
    {
        "event_name": "support.take_break",
        "condition": {
            "and": [
                {"!=": [{"var": "record_data.resolution_status"}, "WIP"]},
                {"!=": [{"var": "payload.resolution_status"}, "WIP"]},
            ]
        },
        "description": "Support take break — unassign unless WIP (TakeBreakView)",
        "actions": [
            {
                "action": "update_fields",
                "args": {"updates": {"cse_name": None, "assigned_to": None}},
            }
        ],
    },
    {
        "event_name": "support.not_connected",
        "condition": {"<": [{"var": "record_data.call_attempts"}, 2]},
        "description": "Support not connected — attempts 1–2: snooze 1h (UpdateCallStatusView)",
        "actions": [
            {
                "action": "update_fields",
                "args": {
                    "updates": {
                        "cse_name": None,
                        "assigned_to": None,
                        "call_status": "Not Connected",
                        "cse_remarks": "{{payload.cse_remarks}}",
                        "completed_at": "{{now}}",
                        "other_reasons": "{{payload.other_reasons}}",
                        "resolution_status": "Snoozed",
                    },
                    "increments": {"call_attempts": 1},
                },
            },
            {
                "action": "compute_next_call_from_attempts",
                "args": {
                    "target_field": "next_call_at",
                    "fixed_minutes": 60,
                    "attempts_field": "call_attempts",
                },
            },
            {
                "action": "compute_next_call_from_attempts",
                "args": {
                    "target_field": "snooze_until",
                    "fixed_minutes": 60,
                    "attempts_field": "call_attempts",
                },
            },
        ],
    },
    {
        "event_name": "support.not_connected",
        "condition": {">=": [{"var": "record_data.call_attempts"}, 2]},
        "description": "Support not connected — 3rd attempt: close (UpdateCallStatusView)",
        "actions": [
            {
                "action": "update_fields",
                "args": {
                    "updates": {
                        "cse_name": None,
                        "assigned_to": None,
                        "call_status": "Not Connected",
                        "cse_remarks": "{{payload.cse_remarks}}",
                        "completed_at": "{{now}}",
                        "next_call_at": None,
                        "snooze_until": "{{payload.snooze_until}}",
                        "other_reasons": "{{payload.other_reasons}}",
                        "resolution_time": "{{payload.resolution_time}}",
                        "resolution_status": "Closed",
                    },
                    "increments": {"call_attempts": 1},
                },
            }
        ],
    },
    {
        "event_name": "support.resolved",
        "condition": {},
        "description": "Support resolved (SaveAndContinueView)",
        "actions": [
            {
                "action": "update_fields",
                "args": {
                    "updates": {
                        "cse_name": "{{payload.cse_name}}",
                        "assigned_to": "{{payload.assigned_to}}",
                        "call_status": "{{payload.call_status}}",
                        "cse_remarks": "{{payload.cse_remarks}}",
                        "completed_at": "{{now}}",
                        "other_reasons": "{{payload.other_reasons}}",
                        "resolution_time": "{{payload.resolution_time}}",
                        "review_requested": "{{payload.review_requested}}",
                        "resolution_status": "Resolved",
                    },
                    "increments": {"call_attempts": 1},
                },
            }
        ],
    },
]


def seed_support_ticket_rules(tenant) -> None:
    """Insert production-shaped support rules for integration tests."""
    for rule_def in SUPPORT_TICKET_TEST_RULES:
        exists = RuleSet.objects.filter(
            tenant=tenant,
            event_name=rule_def["event_name"],
            description=rule_def["description"],
            enabled=True,
        ).exists()
        if exists:
            continue
        RuleSet.objects.create(
            tenant=tenant,
            event_name=rule_def["event_name"],
            condition=rule_def["condition"],
            actions=rule_def["actions"],
            description=rule_def["description"],
            enabled=True,
        )
