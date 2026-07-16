"""CSE support-ticket bucket seed (sales-style Bucket + UserBucketAssignment)."""

from __future__ import annotations

from django.core.cache import cache

from crm_records.models import Bucket, UserBucketAssignment
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE

CSE_FRESH_SLUG = "cse_fresh"
CSE_NC_TODAY_SLUG = "cse_nc_today"
CSE_WIP_TODAY_SLUG = "cse_wip_today"
CSE_NC_YESTERDAY_SLUG = "cse_nc_yesterday"
CSE_WIP_YESTERDAY_SLUG = "cse_wip_yesterday"

# Marks which daily-limit KV applies when daily_limit_applies is true.
DAILY_LIMIT_KIND_SELF_TRIAL = "self_trial"  # SUPPORT_DAILY_LIMIT_SELF_TRIAL
DAILY_LIMIT_KIND_OTHER = "other"  # SUPPORT_DAILY_LIMIT_OTHER
# One fresh pull pool; still enforces both ST + other daily targets per ticket type.
DAILY_LIMIT_KIND_DUAL = "dual"

_DAY_TZ = "Asia/Kolkata"


def _pull_strategy_fresh() -> dict:
    return {
        "order": ["call_attempts", "-created_at"],
        "day_timezone": _DAY_TZ,
        "include_snoozed_due": False,
        "ignore_score_for_sources": [],
    }


def _pull_strategy_retry() -> dict:
    """NC / WIP retries — gated by next_call_at (90m snooze from rules)."""
    return {
        "order": ["call_attempts", "-created_at"],
        "day_timezone": _DAY_TZ,
        "include_snoozed_due": False,
        "ignore_score_for_sources": [],
    }


def seed_cse_support_buckets(tenant, *, clear_cache: bool = True) -> dict:
    """
    Create tenant-wide CSE support buckets + assignments (idempotent by slug).

    Pull order (priority):
    1. Fresh Open (unassigned) — ST and other equal pull weight
    2. Not Connected — first_assigned today (IST), call-ready
    3. WIP — first_assigned today (IST), call-ready (90m)
    4. Not Connected — first_assigned yesterday (IST), call-ready
    5. WIP — first_assigned yesterday (IST), call-ready (90m)

    Daily targets (two KV keys):
    - ``SUPPORT_DAILY_LIMIT_SELF_TRIAL`` — caps Self Trial assigns today
    - ``SUPPORT_DAILY_LIMIT_OTHER`` — caps non–Self Trial assigns today
    Both apply inside the single fresh pool (skip a ticket type when its cap is hit).

    Returns a dict of slug -> Bucket.
    """
    buckets: dict = {}

    fresh, _ = Bucket.objects.update_or_create(
        tenant=tenant,
        slug=CSE_FRESH_SLUG,
        defaults={
            "name": "CSE Fresh",
            "description": (
                "Unassigned Open tickets (ST and other equal weight; "
                "dual daily limits per type)"
            ),
            "is_system": True,
            "is_active": True,
            "filter_conditions": {
                "entity_type": SUPPORT_TICKET_ENTITY_TYPE,
                "assigned_scope": "unassigned",
                "resolution_status": ["Open"],
                "daily_limit_applies": True,
                "daily_limit_kind": DAILY_LIMIT_KIND_DUAL,
            },
        },
    )
    buckets[CSE_FRESH_SLUG] = fresh

    nc_today, _ = Bucket.objects.update_or_create(
        tenant=tenant,
        slug=CSE_NC_TODAY_SLUG,
        defaults={
            "name": "CSE Not Connected Today",
            "description": "Assigned NC tickets first-assigned today (IST), call-ready",
            "is_system": True,
            "is_active": True,
            "filter_conditions": {
                "entity_type": SUPPORT_TICKET_ENTITY_TYPE,
                "assigned_scope": "me",
                "resolution_status": ["Open", "Snoozed"],
                "call_status": ["Not Connected"],
                "first_assigned_day": "today",
                "day_timezone": _DAY_TZ,
            },
        },
    )
    buckets[CSE_NC_TODAY_SLUG] = nc_today

    wip_today, _ = Bucket.objects.update_or_create(
        tenant=tenant,
        slug=CSE_WIP_TODAY_SLUG,
        defaults={
            "name": "CSE WIP Today",
            "description": "Assigned WIP tickets first-assigned today (IST), call-ready (90m)",
            "is_system": True,
            "is_active": True,
            "filter_conditions": {
                "entity_type": SUPPORT_TICKET_ENTITY_TYPE,
                "assigned_scope": "me",
                "resolution_status": ["WIP"],
                "first_assigned_day": "today",
                "day_timezone": _DAY_TZ,
            },
        },
    )
    buckets[CSE_WIP_TODAY_SLUG] = wip_today

    nc_yesterday, _ = Bucket.objects.update_or_create(
        tenant=tenant,
        slug=CSE_NC_YESTERDAY_SLUG,
        defaults={
            "name": "CSE Not Connected Yesterday",
            "description": "Assigned NC tickets first-assigned yesterday (IST), call-ready",
            "is_system": True,
            "is_active": True,
            "filter_conditions": {
                "entity_type": SUPPORT_TICKET_ENTITY_TYPE,
                "assigned_scope": "me",
                "resolution_status": ["Open", "Snoozed"],
                "call_status": ["Not Connected"],
                "first_assigned_day": "yesterday",
                "day_timezone": _DAY_TZ,
            },
        },
    )
    buckets[CSE_NC_YESTERDAY_SLUG] = nc_yesterday

    wip_yesterday, _ = Bucket.objects.update_or_create(
        tenant=tenant,
        slug=CSE_WIP_YESTERDAY_SLUG,
        defaults={
            "name": "CSE WIP Yesterday",
            "description": "Assigned WIP tickets first-assigned yesterday (IST), call-ready (90m)",
            "is_system": True,
            "is_active": True,
            "filter_conditions": {
                "entity_type": SUPPORT_TICKET_ENTITY_TYPE,
                "assigned_scope": "me",
                "resolution_status": ["WIP"],
                "first_assigned_day": "yesterday",
                "day_timezone": _DAY_TZ,
            },
        },
    )
    buckets[CSE_WIP_YESTERDAY_SLUG] = wip_yesterday

    assignments = (
        (fresh, 10, _pull_strategy_fresh()),
        (nc_today, 30, _pull_strategy_retry()),
        (wip_today, 35, _pull_strategy_retry()),
        (nc_yesterday, 40, _pull_strategy_retry()),
        (wip_yesterday, 45, _pull_strategy_retry()),
    )
    for bucket, priority, strategy in assignments:
        UserBucketAssignment.objects.update_or_create(
            tenant=tenant,
            bucket=bucket,
            user=None,
            defaults={
                "priority": priority,
                "pull_strategy": strategy,
                "is_active": True,
            },
        )

    if clear_cache:
        for suffix in ("all", "lead", SUPPORT_TICKET_ENTITY_TYPE):
            cache.delete(f"bucket_assignments_tenant:{tenant.id}:v5:{suffix}")

    return buckets
