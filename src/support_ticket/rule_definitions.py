"""Production-shaped support ticket RuleSet definitions (NC lock, 90m snooze)."""

from __future__ import annotations

from typing import Any, Dict, List

from support_ticket.ticket_types import (
    SELF_TRIAL_TICKET_TYPE_KEY,
    raw_field_values_for_type_key,
)

_SELF_TRIAL_RAW_VALUES = list(raw_field_values_for_type_key(SELF_TRIAL_TICKET_TYPE_KEY))
# Non–Self Trial: close on 5th NC attempt (condition uses attempts before increment).
NON_SELF_TRIAL_NC_CLOSE_AT = 4
NOT_CONNECTED_SNOOZE_MINUTES = 90


def _record_is_self_trial_condition() -> Dict[str, Any]:
    return {
        "or": [
            {"in": [{"var": "record_data.support_ticket_type"}, _SELF_TRIAL_RAW_VALUES]},
            {"in": [{"var": "record_data.poster"}, _SELF_TRIAL_RAW_VALUES]},
        ]
    }


def _record_not_self_trial_condition() -> Dict[str, Any]:
    return {"!": _record_is_self_trial_condition()}


def _not_connected_snooze_actions() -> List[Dict[str, Any]]:
    """Keep assignee — permanent CSE lock until terminal. Snooze 90 minutes."""
    return [
        {
            "action": "update_fields",
            "args": {
                "updates": {
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
                "fixed_minutes": NOT_CONNECTED_SNOOZE_MINUTES,
                "attempts_field": "call_attempts",
            },
        },
        {
            "action": "compute_next_call_from_attempts",
            "args": {
                "target_field": "snooze_until",
                "fixed_minutes": NOT_CONNECTED_SNOOZE_MINUTES,
                "attempts_field": "call_attempts",
            },
        },
    ]


def _not_connected_close_actions() -> List[Dict[str, Any]]:
    return [
        {
            "action": "update_fields",
            "args": {
                "updates": {
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
    ]


SUPPORT_TICKET_RULE_DEFINITIONS: List[Dict[str, Any]] = [
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
        "description": "Support call later / WIP — keep assignee, snooze 90m",
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
            },
            {
                "action": "compute_next_call_from_attempts",
                "args": {
                    "target_field": "next_call_at",
                    "fixed_minutes": NOT_CONNECTED_SNOOZE_MINUTES,
                    "attempts_field": "call_attempts",
                },
            },
            {
                "action": "compute_next_call_from_attempts",
                "args": {
                    "target_field": "snooze_until",
                    "fixed_minutes": NOT_CONNECTED_SNOOZE_MINUTES,
                    "attempts_field": "call_attempts",
                },
            },
        ],
    },
    {
        "event_name": "support.take_break",
        "condition": {
            "and": [
                {"!=": [{"var": "record_data.resolution_status"}, "WIP"]},
                {"!=": [{"var": "payload.resolution_status"}, "WIP"]},
                # NC tickets are stored as resolution_status=Snoozed (+ call_status Not Connected).
                {"!=": [{"var": "record_data.resolution_status"}, "Snoozed"]},
                {"!=": [{"var": "payload.resolution_status"}, "Snoozed"]},
                {
                    "!": {
                        "in": [
                            {"var": "record_data.call_status"},
                            [
                                "Not Connected",
                                "not connected",
                                "NOT CONNECTED",
                                "not_connected",
                            ],
                        ]
                    }
                },
            ]
        },
        "description": (
            "Support take break — unassign fresh only "
            "(not WIP / Snoozed NC); clear first_assigned"
        ),
        "actions": [
            {
                "action": "update_fields",
                "args": {
                    "updates": {
                        "cse_name": None,
                        "assigned_to": None,
                        "first_assigned_at": None,
                        "first_assigned_to": None,
                    }
                },
            }
        ],
    },
    {
        "event_name": "support.not_connected",
        "condition": {
            "and": [
                _record_not_self_trial_condition(),
                {"<": [{"var": "record_data.call_attempts"}, NON_SELF_TRIAL_NC_CLOSE_AT]},
            ]
        },
        "description": "Support not connected — attempts 1–4: snooze 90m (UpdateCallStatusView)",
        "actions": _not_connected_snooze_actions(),
    },
    {
        "event_name": "support.not_connected",
        "condition": {
            "and": [
                _record_not_self_trial_condition(),
                {">=": [{"var": "record_data.call_attempts"}, NON_SELF_TRIAL_NC_CLOSE_AT]},
            ]
        },
        "description": "Support not connected — 5th attempt: close (UpdateCallStatusView)",
        "actions": _not_connected_close_actions(),
    },
    {
        "event_name": "support.not_connected",
        "condition": _record_is_self_trial_condition(),
        "description": "Self trial not connected — always snooze 90m (no attempt terminal)",
        "actions": _not_connected_snooze_actions(),
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


def sync_support_ticket_rules_for_tenant(tenant, *, disable_stale: bool = True) -> dict:
    """
    Upsert CSE lock / 90m NC rules for a tenant.

    Disables older support.not_connected / support.take_break rows that do not
    match the new descriptions so legacy unassign+60m rules stop firing.
    """
    from crm_records.models import RuleSet

    kept_descriptions = {r["description"] for r in SUPPORT_TICKET_RULE_DEFINITIONS}
    created = updated = disabled = 0

    if disable_stale:
        stale = RuleSet.objects.filter(
            tenant=tenant,
            event_name__in=["support.not_connected", "support.take_break"],
            enabled=True,
        ).exclude(description__in=kept_descriptions)
        disabled = stale.update(enabled=False)

    for rule_def in SUPPORT_TICKET_RULE_DEFINITIONS:
        existing = RuleSet.objects.filter(
            tenant=tenant,
            event_name=rule_def["event_name"],
            description=rule_def["description"],
        ).first()
        if existing:
            existing.condition = rule_def["condition"]
            existing.actions = rule_def["actions"]
            existing.enabled = True
            existing.save(update_fields=["condition", "actions", "enabled", "updated_at"])
            updated += 1
        else:
            RuleSet.objects.create(
                tenant=tenant,
                event_name=rule_def["event_name"],
                condition=rule_def["condition"],
                actions=rule_def["actions"],
                description=rule_def["description"],
                enabled=True,
            )
            created += 1

    return {"created": created, "updated": updated, "disabled": disabled}
