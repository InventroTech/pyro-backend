"""Tests for CSE daily progress API (overall resolve rate vs goal)."""

from django.urls import reverse
from django.utils import timezone

from authz import service as authz_service
from crm_records.models import Record
from support_ticket.constants import (
    SUPPORT_DEFAULT_RESOLVE_RATE_GOAL_PERCENT,
    SUPPORT_TICKET_ENTITY_TYPE,
)
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RoleFactory
from tests.factories.support_ticket_dump_factory import dump_data
from user_settings.models import TenantMemberSetting
from user_settings.services import USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY


class SupportDailyProgressAPITest(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        authz_service._CACHE.clear()
        self.url = reverse("support_ticket:daily-progress")
        cse_role = RoleFactory(
            tenant=self.tenant,
            key="CSE",
            name="Customer Support Executive",
        )
        self.membership.role = cse_role
        self.membership.save(update_fields=["role"])
        Record.objects.filter(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
        ).delete()

    def test_returns_overall_resolve_rate_and_goal(self):
        TenantMemberSetting.objects.update_or_create(
            tenant=self.tenant,
            tenant_membership=self.membership,
            key=USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY,
            defaults={"value": 85},
        )
        now = timezone.now()
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(
                    user_id="st1",
                    name="ST Resolved",
                    support_ticket_type="Self_Trial",
                ),
                "assigned_to": self.supabase_uid,
                "first_assigned_to": self.supabase_uid,
                "first_assigned_at": now.isoformat(),
                "resolution_status": "Resolved",
            },
        )
        Record.objects.create(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={
                **dump_data(
                    user_id="ot1",
                    name="Other Open",
                    support_ticket_type="in_trial",
                ),
                "assigned_to": self.supabase_uid,
                "first_assigned_to": self.supabase_uid,
                "first_assigned_at": now.isoformat(),
                "resolution_status": "Open",
            },
        )

        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["goal_percent"], 85)
        self.assertEqual(response.data["taken_today"], 2)
        self.assertEqual(response.data["resolved_today"], 1)
        self.assertEqual(response.data["resolve_rate"], 50.0)

    def test_defaults_goal_to_80_when_unset(self):
        response = self.client.get(self.url, **self.auth_headers)
        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(
            response.data["goal_percent"],
            SUPPORT_DEFAULT_RESOLVE_RATE_GOAL_PERCENT,
        )
        self.assertIsNone(response.data["resolve_rate"])
        self.assertEqual(response.data["taken_today"], 0)
