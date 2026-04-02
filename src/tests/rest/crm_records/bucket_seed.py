"""
Default lead buckets for API/integration tests (mirrors ``test_lead_pipeline._seed_tenant_buckets``).

Production tenants should have rows created via admin or SQL seeds; tests call this so
``LeadPipeline`` has assignments and does not return ``None`` for every request.
"""

from __future__ import annotations

from django.core.cache import cache

from crm_records.models import Bucket, UserBucketAssignment


def seed_default_lead_buckets(tenant) -> dict:
    """
    Production-like order: follow-up → fresh → not-connected retry.

    ``call_attempts`` on NOT_CONNECTED uses **lt: 6** (not lte: 6) so attempts == 6 are excluded.
    """
    cache.clear()

    followup = Bucket.objects.create(
        tenant=tenant,
        name="Followup Callback",
        slug="followup_callback",
        filter_conditions={
            "lead_stage": ["SNOOZED", "IN_QUEUE"],
            "call_attempts": {"lt": 6},
            "next_call_due": True,
            "assigned_scope": "me",
            "apply_routing_rule": True,
            "fallback_assigned_scope": "unassigned",
        },
    )
    fresh = Bucket.objects.create(
        tenant=tenant,
        name="Fresh Leads",
        slug="fresh_leads",
        filter_conditions={
            "lead_stage": ["FRESH", "IN_QUEUE"],
            "call_attempts": {"lte": 0},
            "next_call_due": False,
            "assigned_scope": "unassigned",
            "apply_routing_rule": True,
            "daily_limit_applies": True,
        },
    )
    not_connected = Bucket.objects.create(
        tenant=tenant,
        name="Not Connected Retry",
        slug="not_connected_retry",
        filter_conditions={
            "lead_stage": ["NOT_CONNECTED", "IN_QUEUE"],
            "call_attempts": {"lt": 6, "gte": 1},
            "next_call_due": True,
            "assigned_scope": "me",
            "apply_routing_rule": True,
            "fallback_assigned_scope": "unassigned",
        },
    )

    strategy_snoozed = {
        "order_by": "call_attempts_asc",
        "include_snoozed_due": True,
    }
    strategy_plain = {
        "order_by": "call_attempts_asc",
        "include_snoozed_due": False,
    }

    UserBucketAssignment.objects.create(
        tenant=tenant,
        user=None,
        bucket=followup,
        priority=1,
        pull_strategy=strategy_snoozed,
    )
    UserBucketAssignment.objects.create(
        tenant=tenant,
        user=None,
        bucket=fresh,
        priority=2,
        pull_strategy=strategy_snoozed,
    )
    UserBucketAssignment.objects.create(
        tenant=tenant,
        user=None,
        bucket=not_connected,
        priority=3,
        pull_strategy=strategy_plain,
    )
    return {"followup": followup, "fresh": fresh, "not_connected": not_connected}
