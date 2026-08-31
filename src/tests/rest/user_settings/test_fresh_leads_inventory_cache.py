"""Cached queue inventories are shared by group list and RM pending summary."""

import uuid
from unittest.mock import patch

import pytest
from django.core.cache import cache

from tests.factories.core_factory import TenantFactory
from user_settings.models import Group
from user_settings.services import (
    count_available_fresh_leads_for_group,
    count_available_support_tickets_for_group,
    fresh_leads_counts_for_groups,
)


@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield
    cache.clear()


@pytest.mark.django_db
def test_count_available_fresh_leads_reuses_cached_inventory():
    tenant = TenantFactory()
    group = Group.objects.create(
        tenant=tenant,
        name=f"g-{uuid.uuid4().hex[:8]}",
        group_data={"party": ["INC"], "lead_sources": [], "lead_statuses": [], "states": []},
    )

    inventory = [
        {
            "data__affiliated_party": "INC",
            "data__lead_source": "SRC",
            "data__lead_status": "NEW",
            "data__state": "KA",
            "count": 7,
        },
        {
            "data__affiliated_party": "BJP",
            "data__lead_source": "SRC",
            "data__lead_status": "NEW",
            "data__state": "KA",
            "count": 3,
        },
    ]

    with patch(
        "user_settings.services._fetch_fresh_leads_inventory",
        return_value=inventory,
    ) as fetch:
        assert count_available_fresh_leads_for_group(tenant, group) == 7
        assert count_available_fresh_leads_for_group(tenant, group) == 7
        assert fresh_leads_counts_for_groups(tenant, [group]) == {group.id: 7}
        fetch.assert_called_once()


@pytest.mark.django_db
def test_count_available_support_tickets_reuses_cached_inventory():
    tenant = TenantFactory()
    group = Group.objects.create(
        tenant=tenant,
        name=f"t-{uuid.uuid4().hex[:8]}",
        group_data={
            "queue_type": "ticket",
            "states": ["Karnataka"],
            "support_ticket_types": ["SELF TRIAL"],
        },
    )
    inventory = {
        "open": [
            {
                "data__state": "Karnataka",
                "data__support_ticket_type": "SELF TRIAL",
                "data__poster": "in_trial",
                "count": 4,
            },
            {
                "data__state": "Maharashtra",
                "data__support_ticket_type": "SELF TRIAL",
                "data__poster": "in_trial",
                "count": 9,
            },
        ],
        "snoozed_due": [
            {
                "data__state": "Karnataka",
                "data__support_ticket_type": "paid",
                "data__poster": "paid",
                "count": 2,
            },
            {
                "data__state": "Karnataka",
                "data__support_ticket_type": "SELF TRIAL",
                "data__poster": "in_trial",
                "count": 1,
            },
        ],
    }

    with patch(
        "user_settings.services._fetch_support_tickets_inventory",
        return_value=inventory,
    ) as fetch:
        assert count_available_support_tickets_for_group(tenant, group.group_data) == 5
        assert count_available_fresh_leads_for_group(tenant, group) == 5
        assert fresh_leads_counts_for_groups(tenant, [group]) == {group.id: 5}
        fetch.assert_called_once()
