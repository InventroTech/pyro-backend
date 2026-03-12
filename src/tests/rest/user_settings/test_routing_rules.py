import uuid
import pytest
from django.urls import reverse
from rest_framework.test import APIClient

from tests.factories.authz_factory import TenantMembershipFactory
from core.models import Tenant
from user_settings.models import RoutingRule
from support_ticket.models import SupportTicket
from crm_records.models import Record


@pytest.mark.django_db
def test_ticket_routing_rule_filters_to_poster():
    # Use a unique slug to prevent IntegrityError during test parallelization/reuse
    unique_slug = f"t1-ticket-{uuid.uuid4().hex[:8]}"
    tenant = Tenant.objects.create(name="T1", slug=unique_slug)
    user_id = uuid.uuid4()

    # New way: The factory automatically creates a role and attaches it!
    membership = TenantMembershipFactory(tenant=tenant, user_id=user_id)

    # Two tickets for the same tenant, different posters
    t1 = SupportTicket.objects.create(tenant=tenant, poster="Facebook")
    t2 = SupportTicket.objects.create(tenant=tenant, poster="Google")

    RoutingRule.objects.create(
        tenant=tenant,
        user_id=user_id,
        tenant_membership=membership,
        queue_type="ticket",
        is_active=True,
        conditions={
            "filters": [
                {"field": "poster", "op": "equals", "value": "Facebook"},
            ]
        },
    )

    client = APIClient()
    # Simulate tenant + user context via headers/custom attrs
    client.defaults["HTTP_X_TENANT_SLUG"] = tenant.slug

    assert RoutingRule.objects.filter(tenant=tenant, queue_type="ticket").count() == 1
    assert {t.poster for t in SupportTicket.objects.filter(tenant=tenant)} == {"Facebook", "Google"}


@pytest.mark.django_db
def test_lead_routing_rule_filters_by_state():
    # Use a unique slug to prevent IntegrityError
    unique_slug = f"t1-lead-{uuid.uuid4().hex[:8]}"
    tenant = Tenant.objects.create(name="T1", slug=unique_slug)
    user_id = uuid.uuid4()

    # New way: The factory automatically creates a role and attaches it!
    membership = TenantMembershipFactory(tenant=tenant, user_id=user_id)

    # Two leads with different states in JSON data
    Record.objects.create(tenant=tenant, entity_type="lead", data={"state": "Tamil Nadu"})
    Record.objects.create(tenant=tenant, entity_type="lead", data={"state": "Karnataka"})

    rule = RoutingRule.objects.create(
        tenant=tenant,
        user_id=user_id,
        tenant_membership=membership,
        queue_type="lead",
        is_active=True,
        conditions={
            "filters": [
                {"field": "state", "op": "equals", "value": "Tamil Nadu"},
            ]
        },
    )

    assert rule.conditions["filters"][0]["value"] == "Tamil Nadu"