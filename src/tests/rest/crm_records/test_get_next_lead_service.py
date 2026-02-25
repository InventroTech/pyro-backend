"""
Unit tests for get_next_lead_service. Uses SimpleTestCase and mocks only—
no database, no migrations. Run with: python manage.py test tests.rest.crm_records.test_get_next_lead_service
"""
from __future__ import annotations

import uuid
from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase
from django.utils import timezone as django_tz
from rest_framework import status

from crm_records.get_next_lead_service import (
    GetNextLeadContext,
    affiliated_party_aliases,
    lead_is_due_for_call,
    should_exclude_lead_by_matrix,
    resolve_context,
    apply_request_overrides,
    daily_limit_retry_response,
    build_main_queue,
    lock_assign_and_respond,
    get_next_lead,
    _build_flattened_response,
)


# --- Helpers (no DB) ---


class TestAffiliatedPartyAliases(SimpleTestCase):
    def test_returns_lead_type_and_case_variants(self):
        result = affiliated_party_aliases("some_party")
        self.assertIn("some_party", result)
        self.assertIn("some_party".lower(), result)
        self.assertIn("some_party".upper(), result)

    def test_in_trial_returns_in_trial_and_in_trail(self):
        result = affiliated_party_aliases("in_trial")
        self.assertIn("in_trial", result)
        self.assertIn("in_trail", result)

    def test_in_trail_returns_same_aliases(self):
        result = affiliated_party_aliases("in_trail")
        self.assertIn("in_trial", result)
        self.assertIn("in_trail", result)


class TestLeadIsDueForCall(SimpleTestCase):
    def test_zero_call_attempts_due(self):
        self.assertTrue(lead_is_due_for_call({"call_attempts": 0}, django_tz.now()))
        self.assertTrue(lead_is_due_for_call({}, django_tz.now()))

    def test_no_next_call_at_not_due(self):
        self.assertFalse(
            lead_is_due_for_call(
                {"call_attempts": 1, "next_call_at": None},
                django_tz.now(),
            )
        )
        self.assertFalse(
            lead_is_due_for_call(
                {"call_attempts": 1, "next_call_at": ""},
                django_tz.now(),
            )
        )

    def test_next_call_at_in_past_due(self):
        past = (django_tz.now() - timedelta(hours=1)).isoformat()
        self.assertTrue(
            lead_is_due_for_call(
                {"call_attempts": 1, "next_call_at": past},
                django_tz.now(),
            )
        )

    def test_next_call_at_in_future_not_due(self):
        future = (django_tz.now() + timedelta(hours=1)).isoformat()
        self.assertFalse(
            lead_is_due_for_call(
                {"call_attempts": 1, "next_call_at": future},
                django_tz.now(),
            )
        )

    def test_non_dict_returns_true(self):
        self.assertTrue(lead_is_due_for_call(None, django_tz.now()))


class TestShouldExcludeLeadByMatrix(SimpleTestCase):
    def test_no_matrix_does_not_exclude(self):
        exclude, reason = should_exclude_lead_by_matrix(
            None, {"call_attempts": 5}, None, django_tz.now()
        )
        self.assertFalse(exclude)
        self.assertIsNone(reason)

    def test_max_call_attempts_excludes(self):
        record = MagicMock()
        record.created_at = django_tz.now()
        matrix = MagicMock()
        matrix.max_call_attempts = 3
        matrix.sla_days = 2
        matrix.min_time_between_calls_hours = 2
        exclude, reason = should_exclude_lead_by_matrix(
            record, {"call_attempts": 3}, matrix, django_tz.now()
        )
        self.assertTrue(exclude)
        self.assertIn("Max call attempts", reason)

    def test_under_max_not_excluded(self):
        record = MagicMock()
        record.created_at = django_tz.now()
        matrix = MagicMock()
        matrix.max_call_attempts = 3
        matrix.sla_days = 2
        matrix.min_time_between_calls_hours = 2
        exclude, _ = should_exclude_lead_by_matrix(
            record, {"call_attempts": 1}, matrix, django_tz.now()
        )
        self.assertFalse(exclude)


class TestBuildFlattenedResponse(SimpleTestCase):
    def test_flattened_contains_expected_keys(self):
        record = MagicMock()
        record.id = 42
        record.data = {"name": "Lead", "lead_stage": "assigned", "praja_id": "P1"}
        serialized = {"created_at": "2025-01-01", "updated_at": "2025-01-02"}
        result = _build_flattened_response(record, serialized, record.data)
        self.assertEqual(result["id"], 42)
        self.assertEqual(result["name"], "Lead")
        self.assertEqual(result["lead_status"], "assigned")
        self.assertEqual(result["praja_id"], "P1")
        self.assertIn("data", result)
        self.assertIn("record", result)


# --- resolve_context (mocked DB) ---


class TestResolveContext(SimpleTestCase):
    def test_no_tenant_returns_none(self):
        request = MagicMock()
        request.tenant = None
        request.user = MagicMock()
        request.user.supabase_uid = "uid"
        request.user.email = "a@b.com"
        request.query_params = {}
        self.assertIsNone(resolve_context(request))

    def test_no_user_identifier_returns_none(self):
        request = MagicMock()
        request.tenant = MagicMock()
        request.user = MagicMock()
        request.user.supabase_uid = None
        request.user.email = None
        request.query_params = {}
        self.assertIsNone(resolve_context(request))

    @patch("authz.models.TenantMembership")
    def test_with_tenant_and_user_returns_context(self, mock_tm):
        mock_tm.objects.filter.return_value.first.return_value = None
        request = MagicMock()
        request.tenant = MagicMock()
        request.user = MagicMock()
        request.user.supabase_uid = str(uuid.uuid4())
        request.user.email = "rm@test.com"
        request.query_params = {}
        ctx = resolve_context(request)
        self.assertIsNotNone(ctx)
        self.assertEqual(ctx.tenant, request.tenant)
        self.assertFalse(ctx.debug_mode)

    @patch("authz.models.TenantMembership")
    def test_debug_mode_from_query_params(self, mock_tm):
        mock_tm.objects.filter.return_value.first.return_value = None
        request = MagicMock()
        request.tenant = MagicMock()
        request.user = MagicMock()
        request.user.supabase_uid = str(uuid.uuid4())
        request.user.email = "rm@test.com"
        request.query_params = {"debug": "1"}
        ctx = resolve_context(request)
        self.assertIsNotNone(ctx)
        self.assertTrue(ctx.debug_mode)


# --- apply_request_overrides ---


class TestApplyRequestOverrides(SimpleTestCase):
    def setUp(self):
        self.ctx = GetNextLeadContext(
            tenant=MagicMock(),
            user=MagicMock(),
            user_identifier="u1",
            user_uuid=uuid.uuid4(),
            tenant_membership=None,
            now=django_tz.now(),
            now_iso=django_tz.now().isoformat(),
            debug_mode=False,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
            daily_limit=None,
        )

    def test_party_param_overrides_eligible_lead_types(self):
        request = MagicMock()
        request.query_params = {"party": "in_trial, sales_lead"}
        apply_request_overrides(self.ctx, request)
        self.assertEqual(self.ctx.eligible_lead_types, ["in_trial", "sales_lead"])

    def test_lead_sources_param_overrides(self):
        request = MagicMock()
        request.query_params = {"lead_sources": "web, api"}
        apply_request_overrides(self.ctx, request)
        self.assertEqual(self.ctx.eligible_lead_sources, ["web", "api"])

    def test_lead_statuses_param_overrides(self):
        request = MagicMock()
        request.query_params = {"lead_statuses": "new, contacted"}
        apply_request_overrides(self.ctx, request)
        self.assertEqual(self.ctx.eligible_lead_statuses, ["new", "contacted"])


# --- daily_limit_retry_response (mocked) ---


class TestDailyLimitRetryResponse(SimpleTestCase):
    def setUp(self):
        self.ctx = GetNextLeadContext(
            tenant=MagicMock(),
            user=MagicMock(),
            user_identifier=str(uuid.uuid4()),
            user_uuid=uuid.uuid4(),
            tenant_membership=None,
            now=django_tz.now(),
            now_iso=django_tz.now().isoformat(),
            debug_mode=False,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
            daily_limit=None,
        )

    def test_no_daily_limit_returns_none(self):
        self.assertIsNone(daily_limit_retry_response(self.ctx))

    def test_daily_limit_not_reached_returns_none(self):
        self.ctx.daily_limit = 10
        with patch("crm_records.get_next_lead_service.Record") as MockRecord:
            mock_extra = MagicMock()
            mock_extra.count.return_value = 5
            mock_qs = MagicMock()
            mock_qs.extra.return_value = mock_extra
            MockRecord.objects.filter.return_value = mock_qs
            self.assertIsNone(daily_limit_retry_response(self.ctx))


# --- get_next_lead (all mocked, no DB) ---


class TestGetNextLeadIntegration(SimpleTestCase):
    def test_no_tenant_returns_200_empty(self):
        request = MagicMock()
        request.tenant = None
        request.user = MagicMock()
        request.user.supabase_uid = "uid"
        request.user.email = "e@e.com"
        request.query_params = {}
        response = get_next_lead(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, {})

    def test_no_user_identifier_returns_200_empty(self):
        request = MagicMock()
        request.tenant = MagicMock()
        request.user = MagicMock()
        request.user.supabase_uid = None
        request.user.email = None
        request.query_params = {}
        response = get_next_lead(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, {})

    @patch("crm_records.get_next_lead_service.resolve_context")
    def test_resolve_context_none_returns_200_empty(self, mock_resolve):
        mock_resolve.return_value = None
        request = MagicMock()
        request.tenant = MagicMock()
        request.user = MagicMock()
        request.query_params = {}
        response = get_next_lead(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, {})

    @patch("crm_records.get_next_lead_service.build_main_queue")
    @patch("crm_records.get_next_lead_service.daily_limit_retry_response")
    @patch("crm_records.get_next_lead_service.apply_request_overrides")
    @patch("crm_records.get_next_lead_service.resolve_context")
    def test_no_unassigned_leads_returns_200_empty(
        self, mock_resolve, mock_apply, mock_retry, mock_build
    ):
        mock_retry.return_value = None
        mock_build.return_value = (MagicMock(), 0, 0)
        ctx = GetNextLeadContext(
            tenant=MagicMock(),
            user=MagicMock(),
            user_identifier="u1",
            user_uuid=uuid.uuid4(),
            tenant_membership=None,
            now=django_tz.now(),
            now_iso=django_tz.now().isoformat(),
            debug_mode=False,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
            daily_limit=None,
        )
        mock_resolve.return_value = ctx
        request = MagicMock()
        request.tenant = MagicMock()
        request.user = MagicMock()
        request.query_params = {}
        response = get_next_lead(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, {})

    @patch("crm_records.get_next_lead_service.build_debug_response")
    @patch("crm_records.get_next_lead_service.build_main_queue")
    @patch("crm_records.get_next_lead_service.daily_limit_retry_response")
    @patch("crm_records.get_next_lead_service.apply_request_overrides")
    @patch("crm_records.get_next_lead_service.resolve_context")
    def test_debug_mode_returns_debug_response(
        self, mock_resolve, mock_apply, mock_retry, mock_build, mock_debug_response
    ):
        mock_retry.return_value = None
        mock_build.return_value = (MagicMock(), 0, 0)
        mock_debug_response.return_value = MagicMock(
            status_code=status.HTTP_200_OK,
            data={"debug": True, "counts": {}},
        )
        ctx = GetNextLeadContext(
            tenant=MagicMock(),
            user=MagicMock(),
            user_identifier="u1",
            user_uuid=uuid.uuid4(),
            tenant_membership=None,
            now=django_tz.now(),
            now_iso=django_tz.now().isoformat(),
            debug_mode=True,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
            daily_limit=None,
        )
        mock_resolve.return_value = ctx
        request = MagicMock()
        request.tenant = MagicMock()
        request.user = MagicMock()
        request.query_params = {"debug": "true"}
        response = get_next_lead(request)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIs(True, response.data.get("debug"))
        self.assertIn("counts", response.data)


# --- lock_assign_and_respond (mocked queryset) ---


class TestLockAssignAndRespond(SimpleTestCase):
    def setUp(self):
        self.ctx = GetNextLeadContext(
            tenant=MagicMock(),
            user=MagicMock(),
            user_identifier=str(uuid.uuid4()),
            user_uuid=uuid.uuid4(),
            tenant_membership=None,
            now=django_tz.now(),
            now_iso=django_tz.now().isoformat(),
            debug_mode=False,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
            daily_limit=None,
        )

    @patch("crm_records.get_next_lead_service.transaction")
    @patch("crm_records.get_next_lead_service.get_queue_service")
    @patch("crm_records.get_next_lead_service.order_by_score")
    def test_empty_queryset_returns_200_empty(self, mock_order, mock_queue, mock_transaction):
        from contextlib import nullcontext
        mock_transaction.atomic.return_value = nullcontext()  # no-op so no DB connection
        mock_ordered = MagicMock()
        mock_ordered.select_for_update.return_value.__getitem__.return_value = []
        mock_order.return_value = mock_ordered
        mock_qs = MagicMock()
        response = lock_assign_and_respond(mock_qs, self.ctx)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, {})
