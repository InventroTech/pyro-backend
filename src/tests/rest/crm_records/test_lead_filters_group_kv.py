"""
Tests for get_lead_filters_for_user: Group + TenantMemberSetting KV only.

Run:
  pytest src/tests/rest/crm_records/test_lead_filters_group_kv.py -v
"""

import uuid

import pytest

from crm_records.lead_filters import get_lead_filters_for_user
from user_settings.models import Group, TenantMemberSetting
from user_settings.services import USER_KV_DAILY_LIMIT_KEY, USER_KV_GROUP_ID_KEY
from tests.factories.authz_factory import TenantMembershipFactory
from tests.factories.core_factory import TenantFactory


@pytest.mark.django_db
def test_lead_filters_from_group_and_kv():
    tenant = TenantFactory()
    user_uuid = uuid.uuid4()
    membership = TenantMembershipFactory(tenant=tenant, user_id=user_uuid)

    group = Group.objects.create(
        tenant=tenant,
        name=f"g-{uuid.uuid4().hex[:8]}",
        group_data={
            "party": ["INC", "BJP"],
            "lead_sources": ["SRC_A"],
            "lead_statuses": ["NEW"],
            "states": ["Karnataka"],
        },
    )

    TenantMemberSetting.objects.create(
        tenant=tenant,
        tenant_membership=membership,
        key=USER_KV_GROUP_ID_KEY,
        value=group.id,
    )
    TenantMemberSetting.objects.create(
        tenant=tenant,
        tenant_membership=membership,
        key=USER_KV_DAILY_LIMIT_KEY,
        value=25,
    )

    filters = get_lead_filters_for_user(tenant, str(user_uuid))

    assert filters.eligible_lead_types == ["INC", "BJP"]
    assert filters.eligible_lead_sources == ["SRC_A"]
    assert filters.eligible_lead_statuses == ["NEW"]
    assert filters.eligible_states == ["Karnataka"]
    assert filters.daily_limit == 25
    assert filters.user_uuid == user_uuid
    assert filters.tenant_membership == membership


@pytest.mark.django_db
def test_lead_filters_no_group_kv_returns_empty_filters():
    tenant = TenantFactory()
    user_uuid = uuid.uuid4()
    TenantMembershipFactory(tenant=tenant, user_id=user_uuid)

    filters = get_lead_filters_for_user(tenant, str(user_uuid))

    assert filters.eligible_lead_types == []
    assert filters.daily_limit is None
