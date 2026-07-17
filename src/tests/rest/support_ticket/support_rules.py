"""
Test helpers that seed support ticket RuleSet rows (production parity).
"""

from __future__ import annotations

from support_ticket.rule_definitions import sync_support_ticket_rules_for_tenant


def seed_support_ticket_rules(tenant) -> None:
    """Upsert production-shaped support rules for integration tests."""
    sync_support_ticket_rules_for_tenant(tenant, disable_stale=True)
