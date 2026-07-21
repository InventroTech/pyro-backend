"""Production-shaped support ticket RuleSet definitions (NC lock, typed snooze)."""

from __future__ import annotations

from typing import Any, Dict, List

from support_ticket.ticket_types import (
    SELF_TRIAL_TICKET_TYPE_KEY,
    raw_field_values_for_type_key,
)

_SELF_TRIAL_RAW_VALUES = list(raw_field_values_for_type_key(SELF_TRIAL_TICKET_TYPE_KEY))
# Non–Self Trial: close on 5th NC attempt (condition uses attempts before increment).
NON_SELF_TRIAL_NC_CLOSE_AT = 4
# WIP + NC snooze: non–Self Trial 60m, Self Trial 90m.
NON_SELF_TRIAL_SNOOZE_MINUTES = 60
SELF_TRIAL_SNOOZE_MINUTES = 90
# Back-compat alias (Self Trial NC snooze).
NOT_CONNECTED_SNOOZE_MINUTES = SELF_TRIAL_SNOOZE_MINUTES


def _record_is_self_trial_condition() -> Dict[str, Any]:
    return {
        "or": [
            {"in": [{"var": "record_data.support_ticket_type"}, _SELF_TRIAL_RAW_VALUES]},
            {"in": [{"var": "record_data.poster"}, _SELF_TRIAL_RAW_VALUES]},
        ]
    }


def _record_not_self_trial_condition() -> Dict[str, Any]:
    return {"!": _record_is_self_trial_condition()}


def _snooze_next_call_actions(*, minutes: int) -> List[Dict[str, Any]]:
    return [
        {
            "action": "compute_next_call_from_attempts",
            "args": {
                "target_field": "next_call_at",
                "fixed_minutes": minutes,
                "attempts_field": "call_attempts",
            },
        },
        {
            "action": "compute_next_call_from_attempts",
            "args": {
                "target_field": "snooze_until",
                "fixed_minutes": minutes,
                "attempts_field": "call_attempts",
            },
        },
    ]


def _call_later_actions(*, minutes: int) -> List[Dict[str, Any]]:
    """Keep assignee; snooze next_call_at / snooze_until by ``minutes``."""
    return [
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
        *_snooze_next_call_actions(minutes=minutes),
    ]


def _not_connected_snooze_actions(*, minutes: int = SELF_TRIAL_SNOOZE_MINUTES) -> List[Dict[str, Any]]:
    """Keep assignee — permanent CSE lock until terminal."""
    return [
        {
            "action": "update_fields",
            "args": {
                "updates": {
                    "cse_name": "{{payload.cse_name}}",
                    "assigned_to": "{{payload.assigned_to}}",
                    "call_status": "Not Connected",
                    "cse_remarks": "{{payload.cse_remarks}}",
                    "completed_at": "{{now}}",
                    "other_reasons": "{{payload.other_reasons}}",
                    "resolution_status": "Snoozed",
                },
                "increments": {"call_attempts": 1},
            },
        },
        *_snooze_next_call_actions(minutes=minutes),
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
        "condition": _record_not_self_trial_condition(),
        "description": "Support call later / WIP — non–Self Trial, snooze 60m",
        "actions": _call_later_actions(minutes=NON_SELF_TRIAL_SNOOZE_MINUTES),
    },
    {
        "event_name": "support.call_later",
        "condition": _record_is_self_trial_condition(),
        "description": "Support call later / WIP — Self Trial, snooze 90m",
        "actions": _call_later_actions(minutes=SELF_TRIAL_SNOOZE_MINUTES),
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
        "description": "Support not connected — attempts 1–4: snooze 60m (UpdateCallStatusView)",
        "actions": _not_connected_snooze_actions(minutes=NON_SELF_TRIAL_SNOOZE_MINUTES),
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
        "actions": _not_connected_snooze_actions(minutes=SELF_TRIAL_SNOOZE_MINUTES),
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
    Upsert CSE lock / typed snooze rules for a tenant.

    Disables older support.not_connected / support.take_break / support.call_later
    rows that do not match the new descriptions so legacy rules stop firing.
    """
    from crm_records.models import RuleSet

    kept_descriptions = {r["description"] for r in SUPPORT_TICKET_RULE_DEFINITIONS}
    created = updated = disabled = 0

    if disable_stale:
        stale = RuleSet.objects.filter(
            tenant=tenant,
            event_name__in=[
                "support.not_connected",
                "support.take_break",
                "support.call_later",
            ],
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
