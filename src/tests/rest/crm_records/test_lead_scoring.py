"""
Tests for lead scoring: no auto-score on Record.save(), score_all_records_for_tenant,
LeadScoringJobHandler, and Praja /entity/ POST + PATCH.

Run with: pytest src/tests/rest/crm_records/test_lead_scoring.py -v
"""

import pytest
from django.test import TestCase, override_settings
from rest_framework.test import APIClient

from background_jobs.job_handlers import LeadScoringJobHandler, LeadScoringChunkJobHandler
from background_jobs.models import BackgroundJob, JobType, JobStatus
from crm_records.models import Record, ScoringRule
from crm_records.scoring import calculate_lead_score, score_all_records_for_tenant
from tests.factories import RecordFactory, TenantFactory


def _process_parent_chunk_jobs(parent_job: BackgroundJob):
    """
    Unit tests call handlers directly (without JobProcessor), so we need to
    explicitly run the enqueued chunk handlers to update lead_score.
    """
    chunk_job_ids = (parent_job.result or {}).get("chunk_job_ids") or []
    chunk_jobs = BackgroundJob.objects.filter(
        id__in=chunk_job_ids,
        job_type=JobType.SCORE_LEADS_CHUNK,
    )
    for chunk_job in chunk_jobs:
        LeadScoringChunkJobHandler().process(chunk_job)


def _poster_free_rule(tenant, weight=42.0):
    return ScoringRule.objects.create(
        tenant=tenant,
        entity_type="lead",
        attribute="data.poster",
        data={"operator": "==", "value": "free"},
        weight=weight,
        order=0,
        is_active=True,
    )


@pytest.mark.django_db
class ScoringNullOperatorTests(TestCase):
    """isNull / isNotNull and == with literal null/none string (not JSON null)."""

    def setUp(self):
        self.tenant = TenantFactory(slug="score-tenant-null-op")

    def test_is_null_matches_missing_field(self):
        ScoringRule.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            attribute="data.optional_tag",
            data={"operator": "isNull", "value": ""},
            weight=3.0,
            order=0,
            is_active=True,
        )
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "A",
                "praja_id": "NULL1",
                "lead_stage": "FRESH",
            },
        )
        self.assertEqual(calculate_lead_score(lead), 3.0)

    def test_is_null_no_match_when_field_present(self):
        ScoringRule.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            attribute="data.poster",
            data={"operator": "isNull", "value": ""},
            weight=9.0,
            order=0,
            is_active=True,
        )
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "poster": "free",
                "praja_id": "NULL2",
                "lead_stage": "FRESH",
            },
        )
        self.assertEqual(calculate_lead_score(lead), 0.0)

    def test_is_not_null_matches_present_field(self):
        ScoringRule.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            attribute="data.poster",
            data={"operator": "isNotNull", "value": ""},
            weight=4.0,
            order=0,
            is_active=True,
        )
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "poster": "x",
                "praja_id": "NULL3",
                "lead_stage": "FRESH",
            },
        )
        self.assertEqual(calculate_lead_score(lead), 4.0)

    def test_equals_null_string_matches_missing_not_literal(self):
        """Typing 'null' in value with == now matches missing/JSON-null, not the word 'null'."""
        ScoringRule.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            attribute="data.optional_tag",
            data={"operator": "==", "value": "null"},
            weight=2.0,
            order=0,
            is_active=True,
        )
        missing = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"name": "B", "praja_id": "NULL4", "lead_stage": "FRESH"},
        )
        literal = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "optional_tag": "null",
                "praja_id": "NULL5",
                "lead_stage": "FRESH",
            },
        )
        self.assertEqual(calculate_lead_score(missing), 2.0)
        self.assertEqual(calculate_lead_score(literal), 0.0)

    def test_sql_batch_is_null_matches_python(self):
        ScoringRule.objects.create(
            tenant=self.tenant,
            entity_type="lead",
            attribute="data.optional_tag",
            data={"operator": "isNull", "value": ""},
            weight=5.0,
            order=0,
            is_active=True,
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"name": "C", "praja_id": "NULL6", "lead_stage": "FRESH"},
        )
        result = score_all_records_for_tenant(
            self.tenant.id, entity_type="lead", batch_size=10
        )
        self.assertEqual(result["total_leads"], 1)
        self.assertEqual(result["total_score_added"], 5.0)
        lead = Record.objects.get(data__praja_id="NULL6")
        self.assertEqual(lead.data.get("lead_score"), 5.0)


@pytest.mark.django_db
class RecordSaveNoAutoLeadScoringTests(TestCase):
    """Leads are not scored on Record.save(); use /entity/ or bulk/job scoring."""

    def setUp(self):
        # Fixed slug: django_get_or_create on slug avoids duplicate key under pytest (see test_entity_api).
        self.tenant = TenantFactory(slug="score-tenant-save")

    def test_new_lead_does_not_get_rule_based_score_on_create(self):
        _poster_free_rule(self.tenant, weight=100.0)
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={
                "name": "Scored Lead",
                "praja_id": "SCORE_SAVE_1",
                "poster": "free",
            },
        )
        lead.refresh_from_db()
        self.assertNotIn("lead_score", lead.data)

    def test_new_lead_without_rules_has_no_auto_lead_score(self):
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"name": "No Rules", "praja_id": "SCORE_SAVE_2"},
        )
        lead.refresh_from_db()
        self.assertNotIn("lead_score", lead.data)

    def test_new_non_lead_record_not_scored(self):
        _poster_free_rule(self.tenant, weight=99.0)
        ticket = RecordFactory(
            tenant=self.tenant,
            entity_type="ticket",
            data={"poster": "free", "praja_id": "TIX1"},
        )
        ticket.refresh_from_db()
        self.assertNotIn("lead_score", ticket.data)

    def test_lead_update_via_save_does_not_apply_scoring_rules(self):
        _poster_free_rule(self.tenant, weight=50.0)
        lead = RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"poster": "free", "praja_id": "SCORE_SAVE_3"},
        )
        lead.refresh_from_db()
        self.assertNotIn("lead_score", lead.data)

        lead.data = {**lead.data, "poster": "paid"}
        lead.save(update_fields=["data", "updated_at"])
        lead.refresh_from_db()
        self.assertNotIn("lead_score", lead.data)
        self.assertEqual(lead.data.get("poster"), "paid")


@pytest.mark.django_db
class ScoreAllRecordsForTenantTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory(slug="score-tenant-bulk")  # fixed slug; see test_entity_api
        _poster_free_rule(self.tenant, weight=10.0)

    def test_scores_all_matching_leads_and_returns_summary(self):
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"poster": "free", "praja_id": "B1"},
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"poster": "paid", "praja_id": "B2"},
        )
        result = score_all_records_for_tenant(
            self.tenant.id, entity_type="lead", batch_size=10
        )
        self.assertEqual(result["total_leads"], 2)
        self.assertEqual(result["processed_leads"], 2)
        self.assertEqual(result["updated_leads"], 1)
        self.assertEqual(result["total_score_added"], 10.0)
        self.assertEqual(result["status"], "completed")

        free = Record.objects.get(data__praja_id="B1")
        paid = Record.objects.get(data__praja_id="B2")
        self.assertEqual(free.data.get("lead_score"), 10.0)
        self.assertEqual(paid.data.get("lead_score"), 0.0)

    def test_batch_size_smaller_than_count_still_processes_all(self):
        for i in range(3):
            RecordFactory(
                tenant=self.tenant,
                entity_type="lead",
                data={"poster": "free", "praja_id": f"BAT{i}"},
            )
        result = score_all_records_for_tenant(
            self.tenant.id, entity_type="lead", batch_size=1
        )
        self.assertEqual(result["total_leads"], 3)
        self.assertEqual(result["processed_leads"], 3)
        self.assertEqual(result["updated_leads"], 3)
        self.assertEqual(result["total_score_added"], 30.0)

    def test_empty_tenant_returns_completed_summary_with_zeros(self):
        empty_tenant = TenantFactory(slug="score-tenant-empty")
        result = score_all_records_for_tenant(
            empty_tenant.id, entity_type="lead", batch_size=50
        )
        self.assertEqual(result["total_leads"], 0)
        self.assertEqual(result["processed_leads"], 0)
        self.assertEqual(result["updated_leads"], 0)
        self.assertEqual(result["total_score_added"], 0.0)
        self.assertEqual(result["progress_percentage"], 100)
        self.assertEqual(result["status"], "completed")

    def test_inactive_rules_not_applied(self):
        # setUp already added an active rule; deactivate every rule for this tenant so none apply.
        ScoringRule.objects.filter(tenant=self.tenant, entity_type="lead").update(is_active=False)
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"poster": "free", "praja_id": "INACT"},
        )
        result = score_all_records_for_tenant(
            self.tenant.id, entity_type="lead", batch_size=10
        )
        self.assertEqual(result["total_leads"], 1)
        self.assertEqual(result["updated_leads"], 0)
        self.assertEqual(result["total_score_added"], 0.0)
        lead = Record.objects.get(data__praja_id="INACT")
        self.assertEqual(lead.data.get("lead_score"), 0.0)


@pytest.mark.django_db
class LeadScoringJobHandlerTests(TestCase):
    def setUp(self):
        self.tenant = TenantFactory(slug="score-tenant-job")  # fixed slug; see test_entity_api
        _poster_free_rule(self.tenant, weight=7.0)
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"poster": "free", "praja_id": "JOB1"},
        )

    def test_process_delegates_to_score_all_records(self):
        job = BackgroundJob.objects.create(
            tenant=self.tenant,
            job_type=JobType.SCORE_LEADS,
            status=JobStatus.PENDING,
            payload={"entity_type": "lead", "batch_size": 50},
        )
        ok = LeadScoringJobHandler().process(job)
        self.assertTrue(ok)
        job.refresh_from_db()
        self.assertEqual(job.result.get("status"), "completed")
        self.assertEqual(job.result.get("total_leads"), 1)

        _process_parent_chunk_jobs(job)
        lead = Record.objects.get(data__praja_id="JOB1")
        self.assertEqual(lead.data.get("lead_score"), 7.0)

    def test_process_raises_value_error_without_tenant(self):
        job = BackgroundJob.objects.create(
            tenant=None,
            job_type=JobType.SCORE_LEADS,
            status=JobStatus.PENDING,
            payload={"entity_type": "lead"},
        )
        with self.assertRaises(ValueError) as ctx:
            LeadScoringJobHandler().process(job)
        self.assertIn("tenant_id", str(ctx.exception).lower())

    def test_process_uses_default_payload_keys(self):
        job = BackgroundJob.objects.create(
            tenant=self.tenant,
            job_type=JobType.SCORE_LEADS,
            status=JobStatus.PENDING,
            payload={},
        )
        self.assertTrue(LeadScoringJobHandler().validate_payload({}))
        ok = LeadScoringJobHandler().process(job)
        self.assertTrue(ok)
        job.refresh_from_db()
        self.assertEqual(job.result.get("status"), "completed")
        self.assertEqual(job.result.get("total_leads"), 1)

        _process_parent_chunk_jobs(job)
        lead = Record.objects.get(data__praja_id="JOB1")
        self.assertEqual(lead.data.get("lead_score"), 7.0)

    def test_process_persists_processing_then_completed_on_job_result(self):
        job = BackgroundJob.objects.create(
            tenant=self.tenant,
            job_type=JobType.SCORE_LEADS,
            status=JobStatus.PENDING,
            payload={"entity_type": "lead", "batch_size": 10},
        )
        statuses_at_save = []
        real_save = job.save

        def save_capture(*args, **kwargs):
            statuses_at_save.append((job.result or {}).get("status"))
            return real_save(*args, **kwargs)

        job.save = save_capture
        LeadScoringJobHandler().process(job)
        self.assertEqual(statuses_at_save[0], "processing")
        self.assertEqual(statuses_at_save[-1], "completed")


@pytest.mark.django_db
@override_settings(
    PYRO_SECRET="test-pyro-secret-scoring",
    DEFAULT_TENANT_SLUG="entity-score-tenant",
)
class EntityApiLeadScoringTests(TestCase):
    """POST and PATCH on /entity/ apply Praja lead scoring from rules."""

    def setUp(self):
        self.tenant = TenantFactory(slug="entity-score-tenant")  # must match DEFAULT_TENANT_SLUG above
        _poster_free_rule(self.tenant, weight=25.0)
        self.client = APIClient()
        self.entity_url = "/entity/"
        self.headers = {"HTTP_X_SECRET_PYRO": "test-pyro-secret-scoring"}

    def test_post_entity_creates_lead_with_rule_based_score(self):
        response = self.client.post(
            self.entity_url,
            {
                "name": "Entity Scored",
                "data": {
                    "praja_id": "ENT_SCORE_1",
                    "poster": "free",
                    "lead_stage": "FRESH",
                },
            },
            format="json",
            **self.headers,
        )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["data"].get("lead_score"), 25.0)

    def test_patch_entity_recalculates_lead_score_when_data_changes(self):
        self.client.post(
            self.entity_url,
            {
                "data": {
                    "praja_id": "ENT_SCORE_2",
                    "poster": "free",
                    "name": "Patch Me",
                },
            },
            format="json",
            **self.headers,
        )
        r1 = self.client.patch(
            f"{self.entity_url}?praja_id=ENT_SCORE_2",
            {"data": {"poster": "paid"}},
            format="json",
            **self.headers,
        )
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r1.data["data"].get("poster"), "paid")
        self.assertEqual(r1.data["data"].get("lead_score"), 0.0)
