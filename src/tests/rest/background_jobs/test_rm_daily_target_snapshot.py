"""
Tests for RmDailyTargetSnapshotJobHandler: once a day, freezes each active
RM's current DAILY_TARGET value into RmDailyTarget for the day that just
ended, so a later edit to the flat DAILY_TARGET setting never rewrites a
day's history once that day is over.
"""
from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from background_jobs.job_handlers import RmDailyTargetSnapshotJobHandler
from background_jobs.models import JobType
from tests.factories import BackgroundJobFactory, TenantFactory, TenantMembershipFactory
from user_settings.models import RmDailyTarget, TenantMemberSetting
from user_settings.services import USER_KV_DAILY_TARGET_KEY


class RmDailyTargetSnapshotJobHandlerTests(TestCase):
    def setUp(self):
        super().setUp()
        self.handler = RmDailyTargetSnapshotJobHandler()
        self.tenant = TenantFactory()
        self.yesterday = timezone.now().date() - timedelta(days=1)

    def _make_job(self):
        return BackgroundJobFactory(
            tenant=self.tenant,
            job_type=JobType.SNAPSHOT_RM_DAILY_TARGETS,
            payload={},
        )

    def test_freezes_yesterday_with_the_rms_current_daily_target(self):
        membership = TenantMembershipFactory(tenant=self.tenant, is_active=True)
        TenantMemberSetting.objects.create(
            tenant=self.tenant, tenant_membership=membership, key=USER_KV_DAILY_TARGET_KEY, value=12,
        )

        self.handler.process(self._make_job())

        row = RmDailyTarget.objects.get(tenant=self.tenant, tenant_membership=membership, date=self.yesterday)
        self.assertEqual(row.target, 12)

    def test_rerunning_the_same_day_converges_to_the_latest_value(self):
        # regression against the update_or_create call reading the WRONG kv
        # snapshot — running twice in the same day should just re-converge
        # to whatever DAILY_TARGET is right now, not error or duplicate rows
        membership = TenantMembershipFactory(tenant=self.tenant, is_active=True)
        setting = TenantMemberSetting.objects.create(
            tenant=self.tenant, tenant_membership=membership, key=USER_KV_DAILY_TARGET_KEY, value=5,
        )

        self.handler.process(self._make_job())
        setting.value = 8
        setting.save()
        self.handler.process(self._make_job())

        rows = RmDailyTarget.objects.filter(tenant=self.tenant, tenant_membership=membership, date=self.yesterday)
        self.assertEqual(rows.count(), 1)
        self.assertEqual(rows.first().target, 8)

    def test_skips_an_rm_with_no_daily_target_configured(self):
        TenantMembershipFactory(tenant=self.tenant, is_active=True)  # no DAILY_TARGET KV row at all

        self.handler.process(self._make_job())

        self.assertEqual(RmDailyTarget.objects.filter(tenant=self.tenant, date=self.yesterday).count(), 0)

    def test_ignores_inactive_memberships(self):
        membership = TenantMembershipFactory(tenant=self.tenant, is_active=False)
        TenantMemberSetting.objects.create(
            tenant=self.tenant, tenant_membership=membership, key=USER_KV_DAILY_TARGET_KEY, value=9,
        )

        self.handler.process(self._make_job())

        self.assertEqual(RmDailyTarget.objects.filter(tenant=self.tenant, date=self.yesterday).count(), 0)

    def test_isolates_by_tenant(self):
        other_tenant = TenantFactory()
        other_membership = TenantMembershipFactory(tenant=other_tenant, is_active=True)
        TenantMemberSetting.objects.create(
            tenant=other_tenant, tenant_membership=other_membership, key=USER_KV_DAILY_TARGET_KEY, value=99,
        )

        # job is scoped to self.tenant, not other_tenant
        self.handler.process(self._make_job())

        self.assertEqual(RmDailyTarget.objects.filter(tenant=other_tenant).count(), 0)
