"""
Record list/detail include assigned_to_display (TenantMembership name, else email).
"""

import uuid

import pytest
from django.urls import reverse
from rest_framework.test import APIClient

from crm_records.models import Record
from tests.factories import RoleFactory, TenantFactory, TenantMembershipFactory, UserFactory


@pytest.mark.django_db
def test_record_list_includes_assigned_to_display_name():
    tenant = TenantFactory(slug="assigned-to-display-tenant")
    role = RoleFactory(tenant=tenant, key="GM", name="GM")

    caller_uid = uuid.uuid4()
    user = UserFactory(email="caller@example.com", supabase_uid=str(caller_uid))
    TenantMembershipFactory(
        tenant=tenant,
        user_id=caller_uid,
        email=user.email.lower(),
        role=role,
        name="Caller Membership",
        is_active=True,
    )

    assignee_uid = uuid.uuid4()
    TenantMembershipFactory(
        tenant=tenant,
        user_id=assignee_uid,
        email="assignee@example.com",
        role=role,
        name="RM Visible To GM",
        is_active=True,
    )

    Record.objects.create(
        tenant=tenant,
        entity_type="lead",
        data={"name": "Lead One", "assigned_to": str(assignee_uid)},
    )

    client = APIClient()
    client.force_authenticate(user=user)
    url = reverse("crm_records:record-list")
    response = client.get(url, HTTP_X_TENANT_SLUG=tenant.slug)

    assert response.status_code == 200
    rows = response.data["data"]
    lead_row = next(r for r in rows if r.get("data", {}).get("name") == "Lead One")
    assert lead_row["assigned_to_display"] == "RM Visible To GM"
    assert lead_row["data"]["assigned_to"] == str(assignee_uid)


@pytest.mark.django_db
def test_record_list_assigned_to_display_falls_back_to_email_when_no_name():
    tenant = TenantFactory(slug="assigned-to-display-email-fallback")
    role = RoleFactory(tenant=tenant, key="AGENT", name="Agent")

    caller_uid = uuid.uuid4()
    user = UserFactory(email="gm@example.com", supabase_uid=str(caller_uid))
    TenantMembershipFactory(
        tenant=tenant,
        user_id=caller_uid,
        email=user.email.lower(),
        role=role,
        is_active=True,
    )

    assignee_uid = uuid.uuid4()
    TenantMembershipFactory(
        tenant=tenant,
        user_id=assignee_uid,
        email="onlyemail@example.com",
        role=role,
        name="",
        is_active=True,
    )

    lead = Record.objects.create(
        tenant=tenant,
        entity_type="lead",
        data={"name": "Lead Two", "assigned_to": str(assignee_uid)},
    )

    client = APIClient()
    client.force_authenticate(user=user)
    response = client.get(reverse("crm_records:record-list"), HTTP_X_TENANT_SLUG=tenant.slug)
    assert response.status_code == 200
    row = next(r for r in response.data["data"] if r["id"] == lead.id)
    assert row["assigned_to_display"] == "onlyemail@example.com"


@pytest.mark.django_db
def test_record_detail_includes_assigned_to_display():
    tenant = TenantFactory(slug="assigned-to-display-detail")
    role = RoleFactory(tenant=tenant, key="GM", name="GM")

    caller_uid = uuid.uuid4()
    user = UserFactory(email="detail@example.com", supabase_uid=str(caller_uid))
    TenantMembershipFactory(
        tenant=tenant,
        user_id=caller_uid,
        email=user.email.lower(),
        role=role,
        is_active=True,
    )

    assignee_uid = uuid.uuid4()
    TenantMembershipFactory(
        tenant=tenant,
        user_id=assignee_uid,
        email="detail-rm@example.com",
        role=role,
        name="Detail RM",
        is_active=True,
    )

    lead = Record.objects.create(
        tenant=tenant,
        entity_type="lead",
        data={"name": "Lead Three", "assigned_to": str(assignee_uid)},
    )

    client = APIClient()
    client.force_authenticate(user=user)
    url = reverse("crm_records:record-update", kwargs={"pk": lead.id})
    response = client.get(url, HTTP_X_TENANT_SLUG=tenant.slug)
    assert response.status_code == 200
    assert response.data["assigned_to_display"] == "Detail RM"


@pytest.mark.django_db
def test_assigned_to_supabase_uid_resolves_when_membership_user_id_null():
    """Leads store supabase_uid; membership may only be linked by email (user_id null)."""
    tenant = TenantFactory(slug="assign-display-supa-email")
    role = RoleFactory(tenant=tenant, key="AGENT", name="Agent")

    caller_uid = uuid.uuid4()
    caller = UserFactory(email="caller-supa@example.com", supabase_uid=str(caller_uid))
    TenantMembershipFactory(
        tenant=tenant,
        user_id=caller_uid,
        email=caller.email.lower(),
        role=role,
        is_active=True,
    )

    assignee_supa = str(uuid.uuid4())
    UserFactory(email="assignee.rm@example.com", supabase_uid=assignee_supa)
    TenantMembershipFactory(
        tenant=tenant,
        user_id=None,
        email="assignee.rm@example.com",
        role=role,
        name="Linked By Email Only",
        is_active=True,
    )

    Record.objects.create(
        tenant=tenant,
        entity_type="lead",
        data={"name": "Lead Supa Path", "assigned_to": assignee_supa},
    )

    client = APIClient()
    client.force_authenticate(user=caller)
    response = client.get(reverse("crm_records:record-list"), HTTP_X_TENANT_SLUG=tenant.slug)
    assert response.status_code == 200
    row = next(r for r in response.data["data"] if r.get("data", {}).get("name") == "Lead Supa Path")
    assert row["assigned_to_display"] == "Linked By Email Only"


@pytest.mark.django_db
def test_search_fields_assigned_to_matches_assignee_name():
    """search + search_fields=assigned_to finds leads by RM name (not only raw id)."""
    tenant = TenantFactory(slug="search-assignee-name-tenant")
    role = RoleFactory(tenant=tenant, key="AGENT", name="Agent")

    caller_uid = uuid.uuid4()
    caller = UserFactory(email="caller-search@example.com", supabase_uid=str(caller_uid))
    TenantMembershipFactory(
        tenant=tenant,
        user_id=caller_uid,
        email=caller.email.lower(),
        role=role,
        is_active=True,
    )

    assignee_uid = uuid.uuid4()
    TenantMembershipFactory(
        tenant=tenant,
        user_id=assignee_uid,
        email="rm.search@example.com",
        role=role,
        name="UniqueRmSearchLabel",
        is_active=True,
    )

    Record.objects.create(
        tenant=tenant,
        entity_type="lead",
        data={"name": "Lead For Search", "assigned_to": str(assignee_uid)},
    )
    Record.objects.create(
        tenant=tenant,
        entity_type="lead",
        data={"name": "Other Lead", "assigned_to": str(caller_uid)},
    )

    client = APIClient()
    client.force_authenticate(user=caller)
    url = reverse("crm_records:record-list")
    response = client.get(
        url,
        {"search": "UniqueRmSearch", "search_fields": "assigned_to", "entity_type": "lead"},
        HTTP_X_TENANT_SLUG=tenant.slug,
    )
    assert response.status_code == 200
    names = {r["data"]["name"] for r in response.data["data"]}
    assert "Lead For Search" in names
    assert "Other Lead" not in names
