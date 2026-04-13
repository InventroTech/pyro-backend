"""
Tests for POST /entity/push/ — idempotent lead ingestion endpoint.

Run with: pytest src/tests/rest/crm_records/test_entity_push_api.py -v
"""

import pytest
from django.test import TestCase, override_settings
from rest_framework.test import APIClient

from crm_records.models import Record, ScoringRule
from tests.factories import TenantFactory


PUSH_URL = "/entity/push/"
SECRET = "test-pyro-secret-push"
TENANT_SLUG = "push-test-tenant"


def _make_headers(**extra):
    return {"HTTP_X_SECRET_PYRO": SECRET, **extra}


def _scoring_rule(tenant, weight=30.0):
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
@override_settings(
    PYRO_SECRET=SECRET,
    DEFAULT_TENANT_SLUG=TENANT_SLUG,
)
class PrajaLeadsPushCreateTests(TestCase):
    """Push creates a new record when praja_id is not yet in the DB."""

    def setUp(self):
        self.tenant = TenantFactory(slug=TENANT_SLUG)
        self.client = APIClient()

    def test_push_creates_lead_returns_201_and_created_true(self):
        response = self.client.post(
            PUSH_URL,
            {"data": {"praja_id": "PUSH_001", "phone_number": "+910000000001"}},
            format="json",
            **_make_headers(),
        )

        self.assertEqual(response.status_code, 201)
        self.assertTrue(response.data["created"])
        self.assertEqual(response.data["data"]["praja_id"], "PUSH_001")
        self.assertTrue(
            Record.objects.filter(
                tenant=self.tenant,
                entity_type="lead",
                data__praja_id="PUSH_001",
            ).exists()
        )

    def test_push_defaults_lead_stage_to_fresh(self):
        response = self.client.post(
            PUSH_URL,
            {"data": {"praja_id": "PUSH_002"}},
            format="json",
            **_make_headers(),
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["data"]["lead_stage"], "FRESH")

    def test_push_defaults_call_attempts_to_zero(self):
        response = self.client.post(
            PUSH_URL,
            {"data": {"praja_id": "PUSH_003"}},
            format="json",
            **_make_headers(),
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["data"]["call_attempts"], 0)

    def test_push_respects_explicit_lead_stage(self):
        response = self.client.post(
            PUSH_URL,
            {"data": {"praja_id": "PUSH_004", "lead_stage": "IN_QUEUE"}},
            format="json",
            **_make_headers(),
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["data"]["lead_stage"], "IN_QUEUE")

    def test_push_moves_root_name_into_data(self):
        response = self.client.post(
            PUSH_URL,
            {"name": "Test User", "data": {"praja_id": "PUSH_005"}},
            format="json",
            **_make_headers(),
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["data"]["name"], "Test User")


@pytest.mark.django_db
@override_settings(
    PYRO_SECRET=SECRET,
    DEFAULT_TENANT_SLUG=TENANT_SLUG,
)
class PrajaLeadsPushIdempotencyTests(TestCase):
    """Push returns the existing record without modifying it when praja_id already exists."""

    def setUp(self):
        self.tenant = TenantFactory(slug=TENANT_SLUG)
        self.client = APIClient()

    def _push(self, praja_id, **data_fields):
        payload = {"data": {"praja_id": praja_id, **data_fields}}
        return self.client.post(PUSH_URL, payload, format="json", **_make_headers())

    def test_push_existing_praja_id_returns_200_and_created_false(self):
        self._push("PUSH_DUP_1")
        response = self._push("PUSH_DUP_1")

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.data["created"])

    def test_push_does_not_create_duplicate_records(self):
        self._push("PUSH_DUP_2")
        self._push("PUSH_DUP_2")
        self._push("PUSH_DUP_2")

        count = Record.objects.filter(
            tenant=self.tenant,
            entity_type="lead",
            data__praja_id="PUSH_DUP_2",
        ).count()
        self.assertEqual(count, 1)

    def test_push_existing_lead_data_is_not_overwritten(self):
        # First push sets the name
        first = self._push("PUSH_DUP_3", name="Original Name", phone_number="+910000000003")
        self.assertEqual(first.status_code, 201)

        # Second push with a different name — existing record should be returned unchanged
        second = self._push("PUSH_DUP_3", name="Should Not Overwrite")
        self.assertEqual(second.status_code, 200)
        self.assertFalse(second.data["created"])
        self.assertEqual(second.data["data"]["name"], "Original Name")

    def test_push_returns_same_record_id_on_duplicate(self):
        first = self._push("PUSH_DUP_4")
        second = self._push("PUSH_DUP_4")

        self.assertEqual(first.data["id"], second.data["id"])


@pytest.mark.django_db
@override_settings(
    PYRO_SECRET=SECRET,
    DEFAULT_TENANT_SLUG=TENANT_SLUG,
)
class PrajaLeadsPushValidationTests(TestCase):
    """Push validates required fields and rejects bad requests."""

    def setUp(self):
        self.tenant = TenantFactory(slug=TENANT_SLUG)
        self.client = APIClient()

    def test_push_without_praja_id_returns_400(self):
        response = self.client.post(
            PUSH_URL,
            {"data": {"phone_number": "+910000000099"}},
            format="json",
            **_make_headers(),
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("praja_id", response.data["error"])

    def test_push_without_auth_header_returns_403(self):
        response = self.client.post(
            PUSH_URL,
            {"data": {"praja_id": "PUSH_NOAUTH"}},
            format="json",
        )

        self.assertEqual(response.status_code, 403)

    def test_push_with_wrong_secret_returns_403(self):
        response = self.client.post(
            PUSH_URL,
            {"data": {"praja_id": "PUSH_BADSECRET"}},
            format="json",
            HTTP_X_SECRET_PYRO="wrong-secret",
        )

        self.assertEqual(response.status_code, 403)


@pytest.mark.django_db
@override_settings(
    PYRO_SECRET=SECRET,
    DEFAULT_TENANT_SLUG=TENANT_SLUG,
)
class PrajaLeadsPushScoringTests(TestCase):
    """Push applies lead scoring rules on creation, not on duplicate returns."""

    def setUp(self):
        self.tenant = TenantFactory(slug=TENANT_SLUG)
        _scoring_rule(self.tenant, weight=30.0)
        self.client = APIClient()

    def test_push_applies_scoring_on_create(self):
        response = self.client.post(
            PUSH_URL,
            {"data": {"praja_id": "PUSH_SCORE_1", "poster": "free"}},
            format="json",
            **_make_headers(),
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["data"]["lead_score"], 30.0)

    def test_push_no_scoring_when_rule_does_not_match(self):
        response = self.client.post(
            PUSH_URL,
            {"data": {"praja_id": "PUSH_SCORE_2", "poster": "paid"}},
            format="json",
            **_make_headers(),
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["data"]["lead_score"], 0.0)

    def test_push_existing_lead_score_preserved_on_duplicate(self):
        # First push: scored
        self.client.post(
            PUSH_URL,
            {"data": {"praja_id": "PUSH_SCORE_3", "poster": "free"}},
            format="json",
            **_make_headers(),
        )
        # Second push with different poster — score should remain from first create
        response = self.client.post(
            PUSH_URL,
            {"data": {"praja_id": "PUSH_SCORE_3", "poster": "paid"}},
            format="json",
            **_make_headers(),
        )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.data["created"])
        self.assertEqual(response.data["data"]["lead_score"], 30.0)
