"""
Tests for work-item bucket builder and pipeline.
"""

from __future__ import annotations

import uuid

import pytest
from django.core.cache import cache
from django.utils import timezone

from crm_records.lead_pipeline.queryset_builder import BucketQuerysetBuilder
from crm_records.models import Bucket, Record, UserBucketAssignment
from crm_records.work_item_pipeline import (
    WorkItemPipeline,
    is_self_trial_bucket_slug,
    is_work_item_bucket_slug,
)
from tests.factories import RecordFactory, TenantFactory

# Mirrors sql/work_item_seed_buckets.sql (for test fixtures only).
_WORK_ITEM_SEED_SLUGS = (
    "self_trial_attempt_0",
    "support_in_trial_attempt_0",
    "support_paid_attempt_0",
    "self_trial_attempt_1",
    "support_in_trial_attempt_1_terminal",
    "support_paid_attempt_1_terminal",
    "self_trial_attempt_2",
    "self_trial_attempt_3",
    "self_trial_attempt_4_terminal",
    "support_other_pending",
)


def test_work_item_bucket_slug_prefixes():
    assert is_work_item_bucket_slug("support_in_trial_attempt_0")
    assert is_work_item_bucket_slug("self_trial_attempt_4_terminal")
    assert not is_work_item_bucket_slug("fresh_leads")
    assert not is_work_item_bucket_slug("support")  # requires support_ prefix
    assert is_self_trial_bucket_slug("self_trial_attempt_0")
    assert not is_self_trial_bucket_slug("support_other_pending")


@pytest.mark.django_db
class TestBucketQuerysetBuilderWorkItem:
    def setup_method(self):
        self.tenant = TenantFactory()
        self.builder = BucketQuerysetBuilder()

    def test_entity_type_support_ticket(self):
        RecordFactory(
            tenant=self.tenant,
            entity_type="support_ticket",
            data={
                "user_id": "u1",
                "atleast_paid_once": False,
                "call_attempts": 0,
                "resolution_status": None,
                "poster": "Help",
            },
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="support_ticket",
            data={
                "user_id": "u2",
                "atleast_paid_once": True,
                "call_attempts": 0,
                "resolution_status": None,
                "poster": "Help",
            },
        )
        qs = self.builder.build(
            tenant=self.tenant,
            bucket_filter_conditions={
                "entity_type": "support_ticket",
                "assigned_scope": "unassigned",
                "atleast_paid_once": False,
                "call_attempts": {"eq": 0},
                "resolution_status_in": [None, ""],
            },
            user_identifier="rm-1",
            user_uuid=None,
            eligible_lead_types=[],
            eligible_lead_sources=["SELF TRIAL"],
            eligible_lead_statuses=["SELF TRIAL"],
            eligible_states=[],
        )
        assert qs.count() == 1
        assert qs.first().data["user_id"] == "u1"

    def test_call_attempts_eq(self):
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"lead_source": "SELF TRIAL", "lead_stage": "FRESH", "call_attempts": 2},
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"lead_source": "SELF TRIAL", "lead_stage": "FRESH", "call_attempts": 0},
        )
        qs = self.builder.build(
            tenant=self.tenant,
            bucket_filter_conditions={
                "entity_type": "lead",
                "lead_source": ["SELF TRIAL"],
                "call_attempts": {"eq": 0},
                "assigned_scope": "unassigned",
            },
            user_identifier="rm-1",
            user_uuid=None,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
            eligible_states=[],
        )
        assert qs.count() == 1
        assert qs.first().data["call_attempts"] == 0

    def test_generic_field_in_without_new_code_path(self):
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"lead_source": "SELF TRIAL", "campaign_code": "A"},
        )
        RecordFactory(
            tenant=self.tenant,
            entity_type="lead",
            data={"lead_source": "SELF TRIAL", "campaign_code": "B"},
        )
        qs = self.builder.build(
            tenant=self.tenant,
            bucket_filter_conditions={
                "entity_type": "lead",
                "assigned_scope": "unassigned",
                "campaign_code_in": ["A"],
            },
            user_identifier="rm-1",
            user_uuid=None,
            eligible_lead_types=[],
            eligible_lead_sources=[],
            eligible_lead_statuses=[],
            eligible_states=[],
        )
        assert qs.count() == 1
        assert qs.first().data["campaign_code"] == "A"


def _seed_work_item_buckets(tenant):
    cache.clear()
    buckets = {}
    for slug in _WORK_ITEM_SEED_SLUGS:
        fc = {"entity_type": "support_ticket" if slug.startswith("support") else "lead", "assigned_scope": "unassigned"}
        if slug == "support_in_trial_attempt_0":
            fc.update(
                {
                    "atleast_paid_once": False,
                    "call_attempts": {"eq": 0},
                    "resolution_status_in": [None, ""],
                }
            )
        elif slug == "support_paid_attempt_0":
            fc.update(
                {
                    "atleast_paid_once": True,
                    "call_attempts": {"eq": 0},
                    "resolution_status_in": [None, ""],
                }
            )
        elif slug == "self_trial_attempt_0":
            fc.update(
                {
                    "lead_source": ["SELF TRIAL"],
                    "lead_stage": ["FRESH", "IN_QUEUE"],
                    "call_attempts": {"eq": 0},
                }
            )
        buckets[slug] = Bucket.objects.create(
            tenant=tenant,
            name=slug,
            slug=slug,
            filter_conditions=fc,
            is_active=True,
        )
        UserBucketAssignment.objects.create(
            tenant=tenant,
            user=None,
            bucket=buckets[slug],
            priority=_WORK_ITEM_SEED_SLUGS.index(slug) + 1,
            pull_strategy={"order": ["-created_at"], "ignore_score_for_sources": []},
            is_active=True,
        )
    return buckets


@pytest.mark.django_db
class TestWorkItemPipelinePriority:
    def test_support_in_trial_before_support_paid(self):
        from tests.rest.crm_records.test_lead_pipeline import _make_rm_user

        tenant = TenantFactory()
        _seed_work_item_buckets(tenant)
        user, _, uid = _make_rm_user(tenant, lead_statuses=[])

        RecordFactory(
            tenant=tenant,
            entity_type="support_ticket",
            data={
                "user_id": "paid-user",
                "atleast_paid_once": True,
                "call_attempts": 0,
                "resolution_status": None,
            },
        )
        st_lead = RecordFactory(
            tenant=tenant,
            entity_type="support_ticket",
            data={
                "user_id": "trial-user",
                "atleast_paid_once": False,
                "call_attempts": 0,
                "resolution_status": None,
            },
        )

        pipeline = WorkItemPipeline()
        record = pipeline.get_next(tenant=tenant, request_user=user)
        assert record is not None
        assert record.id == st_lead.id
