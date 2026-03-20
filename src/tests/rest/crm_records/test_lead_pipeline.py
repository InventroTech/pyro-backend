"""
Unit and integration tests for bucket-based lead pipeline (feat/bucket-crm).

Covers: lead_assignment_tracking, DailyLimitChecker, CandidateSelector,
PullStrategyApplier, BucketQuerysetBuilder, BucketResolver, CallAttemptMatrixFilter,
LeadAssigner, LeadPipeline.

Run:
  pytest src/tests/rest/crm_records/test_lead_pipeline.py -v

Project settings use ``USE_TZ=False`` (naive ``timezone.now()``), while
``set_first_assignment_today_anchor`` uses ``timezone.localtime()`` (needs an aware
``now``). Tests pass explicit UTC-aware datetimes or patch ``timezone.now`` — no
``override_settings``, which can leave PostgreSQL connections closed under pytest-django.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone as datetime_timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.core.cache import cache
from django.test import TestCase
from django.utils import timezone

from authz.models import TenantMembership
from crm_records.lead_assignment_tracking import (
    FIRST_ASSIGNED_TODAY_AT,
    FIRST_ASSIGNMENT_TODAY_DATE,
    merge_first_assignment_today_anchor,
    set_first_assignment_today_anchor,
    is_assigned_value,
    is_unassigned_value,
)
from crm_records.lead_pipeline.bucket_resolver import BucketAssignmentView, BucketResolver
from crm_records.lead_pipeline.candidate_selector import CandidateSelector
from crm_records.lead_pipeline.daily_limit import DailyLimitChecker
from crm_records.lead_pipeline.lead_assigner import LeadAssigner
from crm_records.lead_pipeline.matrix_filter import CallAttemptMatrixFilter
from crm_records.lead_pipeline.pipeline import LeadPipeline
from crm_records.lead_pipeline.pull_strategy import PullStrategyApplier
from crm_records.lead_pipeline.queryset_builder import BucketQuerysetBuilder
from crm_records.lead_pipeline.user_resolver import ResolvedUser, UserResolver
from crm_records.models import Bucket, CallAttemptMatrix, Record, UserBucketAssignment
from crm_records.rule_engine import action_update_fields

from tests.factories import RecordFactory, TenantFactory, TenantMembershipFactory
from tests.factories.user_factory import UserFactory

# ``localtime()`` requires awareness; project tests run with USE_TZ=False.
_AWARE_UTC = datetime(2024, 6, 15, 10, 30, 0, tzinfo=datetime_timezone.utc)


def _start_of_local_day(dt):
    if timezone.is_naive(dt):
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return timezone.localtime(dt).replace(hour=0, minute=0, second=0, microsecond=0)


class LeadAssignmentTrackingTests(TestCase):
    def test_set_first_assignment_today_anchor_shape(self):
        fixed = _AWARE_UTC
        d = set_first_assignment_today_anchor(now=fixed)
        self.assertEqual(d[FIRST_ASSIGNED_TODAY_AT], fixed.isoformat())
        self.assertEqual(
            d[FIRST_ASSIGNMENT_TODAY_DATE],
            timezone.localtime(fixed).date().isoformat(),
        )

    def test_merge_first_assignment_today_anchor_in_place(self):
        target = {"assigned_to": "u1"}
        t0 = _AWARE_UTC
        merge_first_assignment_today_anchor(target, now=t0)
        self.assertIn(FIRST_ASSIGNED_TODAY_AT, target)
        self.assertIn(FIRST_ASSIGNMENT_TODAY_DATE, target)
        self.assertEqual(target["assigned_to"], "u1")

    def test_is_assigned_and_unassigned(self):
        self.assertFalse(is_assigned_value(None))
        self.assertFalse(is_assigned_value(""))
        self.assertFalse(is_assigned_value("null"))
        self.assertTrue(is_assigned_value("rm-1"))
        self.assertTrue(is_unassigned_value(None))
        self.assertTrue(is_unassigned_value("none"))


class DailyLimitCheckerTests(TestCase):
    def setUp(self):
        super().setUp()
        self.tenant = TenantFactory()
        self.uid = "daily-limit-user"
        self.checker = DailyLimitChecker()
        self.now = timezone.now()

    def test_no_limit_returns_not_reached(self):
        st = self.checker.check(
            tenant=self.tenant,
            user_identifier=self.uid,
            daily_limit=None,
            now=self.now,
            debug=False,
        )
        self.assertFalse(st.is_reached)
        self.assertEqual(st.assigned_today, 0)

    def test_invalid_limit_treated_as_no_enforcement(self):
        st = self.checker.check(
            tenant=self.tenant,
            user_identifier=self.uid,
            daily_limit="x",
            now=self.now,
            debug=False,
        )
        self.assertFalse(st.is_reached)

    def test_negative_limit_not_reached(self):
        st = self.checker.check(
            tenant=self.tenant,
            user_identifier=self.uid,
            daily_limit=-1,
            now=self.now,
            debug=False,
        )
        self.assertFalse(st.is_reached)

    def test_counts_first_assigned_today_and_respects_limit(self):
        start = _start_of_local_day(self.now)
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "first_assigned_to": self.uid,
                "first_assigned_at": (start + timedelta(hours=1)).isoformat(),
                "assigned_to": self.uid,
            },
        )
        st = self.checker.check(
            tenant=self.tenant,
            user_identifier=self.uid,
            daily_limit=1,
            now=self.now,
            debug=False,
        )
        self.assertEqual(st.assigned_today, 1)
        self.assertTrue(st.is_reached)

    def test_debug_bypasses_reached_flag(self):
        start = _start_of_local_day(self.now)
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "first_assigned_to": self.uid,
                "first_assigned_at": (start + timedelta(hours=1)).isoformat(),
            },
        )
        st = self.checker.check(
            tenant=self.tenant,
            user_identifier=self.uid,
            daily_limit=1,
            now=self.now,
            debug=True,
        )
        self.assertEqual(st.assigned_today, 1)
        self.assertFalse(st.is_reached)


class CandidateSelectorTests(TestCase):
    def setUp(self):
        self.sel = CandidateSelector()
        self.now = timezone.now()

    def test_fresh_lead_always_due(self):
        self.assertTrue(self.sel.is_due_for_call({"call_attempts": 0}, self.now))

    def test_non_dict_is_due(self):
        self.assertTrue(self.sel.is_due_for_call(None, self.now))

    def test_follow_up_without_next_call_at_not_due(self):
        past = (self.now - timedelta(hours=1)).isoformat()
        self.assertFalse(
            self.sel.is_due_for_call({"call_attempts": 1, "next_call_at": None}, self.now)
        )
        self.assertTrue(
            self.sel.is_due_for_call({"call_attempts": 1, "next_call_at": past}, self.now)
        )

    def test_future_next_call_at_not_due(self):
        future = (self.now + timedelta(hours=2)).isoformat()
        self.assertFalse(self.sel.is_due_for_call({"call_attempts": 2, "next_call_at": future}, self.now))


class PullStrategyApplierTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()
        self.applier = PullStrategyApplier()

    def test_filters_out_attempted_lead_until_next_call_due(self):
        future = (timezone.now() + timedelta(days=1)).isoformat()
        due = (timezone.now() - timedelta(minutes=5)).isoformat()
        r_skip = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"call_attempts": 1, "next_call_at": future},
        )
        r_ok = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"call_attempts": 1, "next_call_at": due},
        )
        qs = Record.objects.filter(tenant=self.tenant, entity_type="lead", id__in=[r_skip.id, r_ok.id])
        out = self.applier.apply(qs=qs, strategy={}, now_iso=timezone.now().isoformat())
        # .values_list() can drop extra() order annotations; evaluate ORM rows.
        ids = {r.id for r in out}
        self.assertNotIn(r_skip.id, ids)
        self.assertIn(r_ok.id, ids)


class BucketQuerysetBuilderTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()
        self.uid = "scope-user"
        self.user_uuid = uuid.uuid4()
        self.builder = BucketQuerysetBuilder()

    def test_unassigned_scope_excludes_assigned(self):
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"lead_stage": "IN_QUEUE", "assigned_to": "other"},
        )
        un = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"lead_stage": "IN_QUEUE"},
        )
        qs = self.builder.build(
            tenant=self.tenant,
            bucket_filter_conditions={"assigned_scope": "unassigned", "apply_routing_rule": False},
            user_identifier=self.uid,
            user_uuid=None,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
        )
        ids = set(qs.values_list("id", flat=True))
        self.assertIn(un.id, ids)

    def test_me_scope_only_current_user(self):
        mine = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"assigned_to": self.uid, "lead_stage": "ASSIGNED"},
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"assigned_to": "other", "lead_stage": "ASSIGNED"},
        )
        qs = self.builder.build(
            tenant=self.tenant,
            bucket_filter_conditions={"assigned_scope": "me", "apply_routing_rule": False},
            user_identifier=self.uid,
            user_uuid=None,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
        )
        self.assertEqual(list(qs.values_list("id", flat=True)), [mine.id])

    def test_call_attempts_range(self):
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"call_attempts": 1},
        )
        r2 = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"call_attempts": 3},
        )
        qs = self.builder.build(
            tenant=self.tenant,
            bucket_filter_conditions={
                "assigned_scope": "any",
                "call_attempts": {"gte": 2, "lte": 5},
                "apply_routing_rule": False,
            },
            user_identifier=self.uid,
            user_uuid=None,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
        )
        self.assertEqual(list(qs.values_list("id", flat=True)), [r2.id])


class BucketResolverTests(TestCase):
    def setUp(self):
        super().setUp()
        from django.db import connection

        if "crm_records_bucket" not in connection.introspection.table_names():
            self.skipTest(
                "crm_records_bucket missing — apply migrations: python manage.py migrate crm_records"
            )
        self.tenant = TenantFactory()
        self.resolver = BucketResolver()
        cache.clear()

    def tearDown(self):
        cache.clear()
        super().tearDown()

    def test_empty_when_no_membership(self):
        ru = ResolvedUser(
            identifier="x",
            uuid=None,
            membership=None,
            email=None,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
            daily_limit=None,
        )
        self.assertEqual(self.resolver.resolve(self.tenant, ru), [])

    def test_orders_by_priority_and_uses_bucket_filters(self):
        membership = TenantMembershipFactory(tenant=self.tenant)
        b_low = Bucket.objects.create(
            tenant=self.tenant,
            name="Low",
            slug=f"low-{membership.id}",
            filter_conditions={"assigned_scope": "unassigned"},
            is_active=True,
        )
        b_high = Bucket.objects.create(
            tenant=self.tenant,
            name="High",
            slug=f"high-{membership.id}",
            filter_conditions={"assigned_scope": "unassigned", "lead_stage": ["IN_QUEUE"]},
            is_active=True,
        )
        UserBucketAssignment.objects.create(
            tenant=self.tenant,
            user=membership,
            bucket=b_low,
            priority=100,
            pull_strategy={"order_by": "score_desc"},
            is_active=True,
        )
        UserBucketAssignment.objects.create(
            tenant=self.tenant,
            user=membership,
            bucket=b_high,
            priority=10,
            pull_strategy={"order_by": "call_attempts_asc"},
            is_active=True,
        )
        ru = ResolvedUser(
            identifier=str(membership.user_id),
            uuid=membership.user_id,
            membership=membership,
            email=membership.email,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
            daily_limit=None,
        )
        out = self.resolver.resolve(self.tenant, ru)
        self.assertEqual(len(out), 2)
        self.assertIsInstance(out[0], BucketAssignmentView)
        self.assertEqual(out[0].priority, 10)
        self.assertEqual(out[0].bucket_slug, b_high.slug)
        self.assertEqual(out[1].priority, 100)

    def test_cache_returns_same_structure(self):
        membership = TenantMembershipFactory(tenant=self.tenant)
        b = Bucket.objects.create(
            tenant=self.tenant,
            name="C",
            slug=f"cached-{membership.id}",
            filter_conditions={"daily_limit_applies": True},
            is_active=True,
        )
        UserBucketAssignment.objects.create(
            tenant=self.tenant,
            user=membership,
            bucket=b,
            priority=1,
            pull_strategy={},
            is_active=True,
        )
        ru = ResolvedUser(
            identifier=str(membership.user_id),
            uuid=membership.user_id,
            membership=membership,
            email=membership.email,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
            daily_limit=None,
        )
        first = self.resolver.resolve(self.tenant, ru)
        second = self.resolver.resolve(self.tenant, ru)
        self.assertEqual(len(first), len(second))
        self.assertEqual(first[0].bucket_slug, second[0].bucket_slug)


class CallAttemptMatrixFilterTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory()
        self.filt = CallAttemptMatrixFilter()

    def test_excludes_when_max_attempts_reached(self):
        CallAttemptMatrix.objects.create(
            tenant=self.tenant,
            lead_type="PARTY_A",
            max_call_attempts=3,
            sla_days=30,
            min_time_between_calls_hours=1,
        )
        r_ok = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"affiliated_party": "PARTY_A", "call_attempts": 1},
        )
        r_max = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"affiliated_party": "PARTY_A", "call_attempts": 5},
        )
        qs = Record.objects.filter(tenant=self.tenant, entity_type="lead", id__in=[r_ok.id, r_max.id])
        out = self.filt.apply(
            qs=qs,
            tenant=self.tenant,
            eligible_lead_types=["PARTY_A"],
            now=timezone.now(),
        )
        ids = set(out.values_list("id", flat=True))
        self.assertIn(r_ok.id, ids)
        self.assertNotIn(r_max.id, ids)

    def test_no_eligible_types_short_circuits(self):
        qs = Record.objects.filter(tenant=self.tenant, entity_type="lead")
        out = self.filt.apply(qs=qs, tenant=self.tenant, eligible_lead_types=[], now=timezone.now())
        self.assertIs(out, qs)


class LeadAssignerTests(TestCase):
    def setUp(self):
        super().setUp()
        self.tenant = TenantFactory()
        self.user = UserFactory(
            supabase_uid=str(uuid.uuid4()),
            email="assigner@example.com",
            tenant_id=str(self.tenant.id),
        )
        membership = TenantMembershipFactory(tenant=self.tenant, user_id=self.user.supabase_uid)
        self.membership = membership
        self.post = MagicMock()
        self.assigner = LeadAssigner(post_actions=self.post)

    def test_fresh_assignment_sets_first_assigned_and_today_anchor(self):
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "IN_QUEUE",
                "call_attempts": 0,
            },
        )
        now = _AWARE_UTC
        with patch("crm_records.lead_pipeline.lead_assigner.timezone.now", return_value=now):
            result = self.assigner.assign_main_queue(
                candidate_pk=lead.pk,
                tenant=self.tenant,
                user=self.user,
                tenant_membership=self.membership,
                user_identifier=self.user.supabase_uid,
                user_uuid=uuid.UUID(self.user.supabase_uid),
                now=now,
            )
        self.assertIsNotNone(result)
        lead.refresh_from_db()
        self.assertEqual(lead.data.get("assigned_to"), self.user.supabase_uid)
        self.assertEqual(lead.data.get("lead_stage"), LeadAssigner.ASSIGNED_STATUS)
        self.assertIn("first_assigned_at", lead.data)
        self.assertIn(FIRST_ASSIGNED_TODAY_AT, lead.data)
        self.post.run.assert_called_once()

    def test_not_connected_retry_skips_first_assigned_at(self):
        now = _AWARE_UTC
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "lead_stage": "NOT_CONNECTED",
                "call_attempts": 1,
                "next_call_at": (now - timedelta(minutes=1)).isoformat(),
            },
        )
        with patch("crm_records.lead_pipeline.lead_assigner.timezone.now", return_value=now):
            result = self.assigner.assign_main_queue(
                candidate_pk=lead.pk,
                tenant=self.tenant,
                user=self.user,
                tenant_membership=self.membership,
                user_identifier=self.user.supabase_uid,
                user_uuid=uuid.UUID(self.user.supabase_uid),
                now=now,
            )
        self.assertIsNotNone(result)
        lead.refresh_from_db()
        self.assertNotIn("first_assigned_at", lead.data)
        self.assertIn(FIRST_ASSIGNED_TODAY_AT, lead.data)


class LeadPipelineIntegrationTests(TestCase):
    def setUp(self):
        super().setUp()
        from django.db import connection

        if "crm_records_bucket" not in connection.introspection.table_names():
            self.skipTest(
                "crm_records_bucket missing — apply migrations: python manage.py migrate crm_records"
            )
        self.tenant = TenantFactory()
        self.supabase_uid = str(uuid.uuid4())
        self.user = UserFactory(
            supabase_uid=self.supabase_uid,
            email="pipe@example.com",
            tenant_id=str(self.tenant.id),
        )
        self.membership = TenantMembershipFactory(
            tenant=self.tenant,
            user_id=self.supabase_uid,
            email=self.user.email,
        )
        cache.clear()

    def tearDown(self):
        cache.clear()
        super().tearDown()

    @patch("django.utils.timezone.now", return_value=_AWARE_UTC)
    @patch("crm_records.lead_pipeline.post_assignment.get_queue_service")
    def test_get_next_assigns_from_bucket(self, mock_qs, _mock_now):
        mock_qs.return_value.enqueue_job = MagicMock(return_value=MagicMock(id=1))
        bucket = Bucket.objects.create(
            tenant=self.tenant,
            name="Pool",
            slug=f"pool-{self.membership.id}",
            filter_conditions={"assigned_scope": "unassigned", "lead_stage": ["IN_QUEUE"]},
            is_active=True,
        )
        UserBucketAssignment.objects.create(
            tenant=self.tenant,
            user=self.membership,
            bucket=bucket,
            priority=1,
            pull_strategy={"order_by": "score_desc"},
            is_active=True,
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Pipeline Lead",
                "lead_stage": "IN_QUEUE",
                "lead_source": "SALES LEAD",
                "call_attempts": 0,
                "lead_score": 10,
            },
        )
        pipeline = LeadPipeline()
        record = pipeline.get_next(tenant=self.tenant, request_user=self.user, debug=False)
        self.assertIsNotNone(record, msg="Expected a lead from bucket; check bucket migrations and test data.")
        record.refresh_from_db()
        self.assertEqual(record.data.get("assigned_to"), self.supabase_uid)

    def test_get_next_returns_none_without_user_identifier(self):
        u = SimpleNamespace(supabase_uid=None, email=None)
        pipeline = LeadPipeline()
        self.assertIsNone(pipeline.get_next(tenant=self.tenant, request_user=u, debug=False))


class UserResolverTests(TestCase):
    def test_resolve_loads_membership_and_filters(self):
        tenant = TenantFactory()
        uid = str(uuid.uuid4())
        user = UserFactory(supabase_uid=uid, email="ur@example.com", tenant_id=str(tenant.id))
        TenantMembershipFactory(tenant=tenant, user_id=uid, email=user.email)
        resolver = UserResolver()
        ru = resolver.resolve(tenant, user)
        self.assertEqual(ru.identifier, uid)
        self.assertIsInstance(ru.membership, TenantMembership)
        self.assertEqual(ru.uuid, uuid.UUID(uid))


class RuleEngineFirstAssignedTodayTests(TestCase):
    """action_update_fields: anchor for 12h NOT_CONNECTED release; legacy not_connected_unassign_at removed."""

    def test_fresh_assignment_via_rule_sets_first_assigned_today_fields(self):
        record = RecordFactory(
            tenant=TenantFactory(),
            entity_type="lead",
            data={"lead_stage": "IN_QUEUE", "call_attempts": 0},
        )
        ctx = {"record": record, "payload": {}, "event": "lead.updated"}
        with patch("crm_records.rule_engine.timezone.now", return_value=_AWARE_UTC):
            action_update_fields(ctx, {"assigned_to": "rm-rule-1"})
        record.refresh_from_db()
        self.assertIn(FIRST_ASSIGNED_TODAY_AT, record.data)
        self.assertIn(FIRST_ASSIGNMENT_TODAY_DATE, record.data)
        self.assertEqual(record.data.get("assigned_to"), "rm-rule-1")

    def test_not_connected_assignment_does_not_set_not_connected_unassign_at(self):
        record = RecordFactory(
            tenant=TenantFactory(),
            entity_type="lead",
            data={
                "lead_stage": "IN_QUEUE",
                "call_attempts": 1,
                "last_call_outcome": "not_connected",
            },
        )
        ctx = {"record": record, "payload": {}, "event": "lead.not_connected"}
        with patch("crm_records.rule_engine.timezone.now", return_value=_AWARE_UTC):
            action_update_fields(
                ctx,
                {"assigned_to": "rm-rule-2", "lead_stage": "NOT_CONNECTED"},
            )
        record.refresh_from_db()
        self.assertNotIn("not_connected_unassign_at", record.data)
        self.assertIn(FIRST_ASSIGNED_TODAY_AT, record.data)
