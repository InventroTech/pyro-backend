from unittest.mock import patch
from datetime import date, datetime
from decimal import Decimal
import json

from django.test import SimpleTestCase
from authz.models import TenantMembership
from authz.views_management import (
    INTERNAL_BILLING_EMAIL_ADDRESSES,
    calculate_membership_billing,
    get_membership_monthly_amount,
)
from tests.base.test_setup import BaseAPITestCase
from tests.factories import RoleFactory, TenantFactory, TenantMembershipFactory


class MembershipBillingCalculationTests(SimpleTestCase):
    def test_join_day_ten_gets_twenty_one_days_when_counted_through_day_thirty(self):
        joined_at = datetime(2026, 5, 10, 9, 0, 0)

        days, amount = calculate_membership_billing(
            joined_at,
            date(2026, 5, 1),
            Decimal("3000"),
            30,
        )

        self.assertEqual(days, 21)
        self.assertEqual(amount, Decimal("2100.00"))

    def test_existing_member_gets_full_cycle(self):
        joined_at = datetime(2026, 4, 15, 9, 0, 0)

        days, amount = calculate_membership_billing(
            joined_at,
            date(2026, 5, 1),
            Decimal("3000"),
            30,
        )

        self.assertEqual(days, 30)
        self.assertEqual(amount, Decimal("3000.00"))

    def test_cycle_days_default_to_calendar_days_for_month(self):
        joined_at = datetime(2026, 5, 10, 9, 0, 0)

        days, amount = calculate_membership_billing(
            joined_at,
            date(2026, 5, 1),
            Decimal("3100"),
        )

        self.assertEqual(days, 22)
        self.assertEqual(amount, Decimal("2200.00"))

    def test_current_month_can_be_capped_at_today(self):
        joined_at = datetime(2026, 5, 13, 9, 0, 0)

        days, amount = calculate_membership_billing(
            joined_at,
            date(2026, 5, 1),
            Decimal("1500"),
            31,
            date(2026, 5, 29),
        )

        self.assertEqual(days, 17)
        self.assertEqual(amount, Decimal("822.58"))

    def test_cse_and_rm_role_rates_are_fixed(self):
        cse_membership = type("Membership", (), {
            "role": type("Role", (), {"key": "CSE", "name": "Customer Support Executive"})()
        })()
        rm_membership = type("Membership", (), {
            "role": type("Role", (), {"key": "RM", "name": "Relationship Manager"})()
        })()

        self.assertEqual(get_membership_monthly_amount(cse_membership), ("CSE", Decimal("1500")))
        self.assertEqual(get_membership_monthly_amount(rm_membership), ("RM", Decimal("2000")))

    def test_role_rates_can_be_overridden_for_report(self):
        cse_membership = type("Membership", (), {
            "role": type("Role", (), {"key": "CSE", "name": "Customer Support Executive"})()
        })()

        self.assertEqual(
            get_membership_monthly_amount(cse_membership, {"CSE": Decimal("1750"), "RM": Decimal("2200")}),
            ("CSE", Decimal("1750")),
        )


class TenantMembershipBillingAPITests(BaseAPITestCase):
    def setUp(self):
        super().setUp()
        self.client.force_authenticate(user=self.user)
        self.url = "/membership/billing/"

    @patch("authz.views_management._today", return_value=date(2026, 5, 29))
    def test_returns_prorated_billing_for_current_tenant_memberships_only(self, _mock_today):
        cse_role = RoleFactory(tenant=self.tenant, key="CSE", name="Customer Support Executive")
        TenantMembership.objects.filter(id=self.membership.id).update(
            role_id=cse_role.id,
            created_at=datetime(2026, 5, 13, 9, 0, 0)
        )

        other_tenant = TenantFactory()
        other_role = RoleFactory(tenant=other_tenant, key="staff", name="Staff")
        TenantMembershipFactory(
            tenant=other_tenant,
            role=other_role,
            email="other@example.com",
        )
        internal_domain_membership = TenantMembershipFactory(
            tenant=self.tenant,
            role=cse_role,
            email="internal@thepyro.ai",
        )
        internal_configured_membership = TenantMembershipFactory(
            tenant=self.tenant,
            role=cse_role,
            email="RitamCoding@gmail.com",
        )
        TenantMembership.objects.filter(
            id__in=[internal_domain_membership.id, internal_configured_membership.id]
        ).update(created_at=datetime(2026, 5, 1, 9, 0, 0))

        response = self.client.get(
            self.url,
            {"month": "2026-05"},
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, 200, response.data)
        self.assertEqual(response.data["summary"]["member_count"], 1)
        self.assertEqual(response.data["summary"]["excluded_internal_member_count"], 2)
        self.assertEqual(response.data["summary"]["total_billable_days"], 17)
        self.assertEqual(response.data["summary"]["total_amount"], "822.58")
        self.assertEqual(response.data["cycle_days"], 31)
        self.assertEqual(response.data["period_end"], "2026-05-29")
        self.assertEqual(response.data["excluded_email_domain"], "@thepyro.ai")
        self.assertEqual(
            response.data["excluded_email_addresses_count"],
            len(INTERNAL_BILLING_EMAIL_ADDRESSES),
        )
        cse_billing_role = next(
            role for role in response.data["billing_roles"]
            if role["key"] == "CSE"
        )
        self.assertEqual(cse_billing_role["rate"], "1500.00")
        self.assertEqual(response.data["role_rates"]["CSE"], "1500.00")
        self.assertNotIn("RM", response.data["role_rates"])
        self.assertEqual(response.data["results"][0]["billable_days"], 17)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertNotIn("@thepyro.ai", response.data["results"][0]["email"])
        self.assertEqual(response.data["results"][0]["billing_role_key"], "CSE")
        self.assertEqual(response.data["results"][0]["monthly_amount"], "1500.00")
        self.assertEqual(response.data["results"][0]["billing_amount"], "822.58")

    @patch("authz.views_management._today", return_value=date(2026, 5, 29))
    def test_rate_overrides_are_used_for_report_calculation(self, _mock_today):
        cse_role = RoleFactory(tenant=self.tenant, key="CSE", name="Customer Support Executive")
        TenantMembership.objects.filter(id=self.membership.id).update(
            role_id=cse_role.id,
            created_at=datetime(2026, 5, 13, 9, 0, 0),
        )

        response = self.client.get(
            self.url,
            {
                "month": "2026-05",
                "role_rates": json.dumps({str(cse_role.id): "1800"}),
                "rm_rate": "2100",
            },
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, 200, response.data)
        billing_role = next(
            role for role in response.data["billing_roles"]
            if role["id"] == str(cse_role.id)
        )
        self.assertEqual(billing_role["rate"], "1800.00")
        self.assertEqual(response.data["role_rates"]["CSE"], "1800.00")
        self.assertEqual(response.data["results"][0]["monthly_amount"], "1800.00")
        self.assertEqual(response.data["results"][0]["billing_amount"], "987.10")

    @patch("authz.views_management._today", return_value=date(2026, 5, 29))
    def test_includes_deleted_memberships_that_existed_during_billing_period(self, _mock_today):
        cse_role = RoleFactory(tenant=self.tenant, key="CSE", name="Customer Support Executive")
        deleted_membership = TenantMembershipFactory(
            tenant=self.tenant,
            role=cse_role,
            email="deleted-cse@example.com",
            name="Deleted CSE",
        )
        TenantMembership.all_objects.filter(id=deleted_membership.id).update(
            created_at=datetime(2026, 5, 10, 9, 0, 0),
            is_deleted=True,
            deleted_at=datetime(2026, 5, 20, 18, 0, 0),
        )

        response = self.client.get(
            self.url,
            {"month": "2026-05"},
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, 200, response.data)
        row = next(
            item for item in response.data["results"]
            if item["email"] == "deleted-cse@example.com"
        )
        self.assertTrue(row["is_deleted"])
        self.assertEqual(row["billable_days"], 11)
        self.assertEqual(row["billing_end_date"], "2026-05-20")
        self.assertEqual(row["monthly_amount"], "1500.00")
        self.assertEqual(row["billing_amount"], "532.26")

    def test_invalid_month_returns_400(self):
        response = self.client.get(
            self.url,
            {"month": "05-2026"},
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("YYYY-MM", response.data["error"])

    @patch("authz.views_management._current_billing_month", return_value=date(2026, 5, 1))
    def test_future_month_returns_400(self, _mock_current_month):
        response = self.client.get(
            self.url,
            {"month": "2026-09"},
            **self.auth_headers,
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("future month", response.data["error"])
