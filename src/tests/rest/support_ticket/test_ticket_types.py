from django.db.models import Q
from django.test import SimpleTestCase, TestCase

from crm_records.models import Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE
from support_ticket.ticket_types import (
    SELF_TRIAL_TICKET_TYPE_KEY,
    canonical_support_ticket_type_key,
    q_record_self_trial,
    q_record_support_ticket_type_key,
    raw_field_values_for_type_key,
)
from tests.factories import TenantFactory
from tests.factories.crm_records_factory import RecordFactory


class SupportTicketTypeCanonicalizationTests(SimpleTestCase):
    def test_selftrail_canonicalizes_to_self_trail(self):
        self.assertEqual(
            canonical_support_ticket_type_key("selftrail"),
            SELF_TRIAL_TICKET_TYPE_KEY,
        )

    def test_unknown_type_canonicalizes_to_rest(self):
        self.assertEqual(canonical_support_ticket_type_key("unknown_type"), "rest")

    def test_raw_field_values_only_include_canonical_self_trail_values(self):
        for value in raw_field_values_for_type_key(SELF_TRIAL_TICKET_TYPE_KEY):
            self.assertEqual(
                canonical_support_ticket_type_key(value),
                SELF_TRIAL_TICKET_TYPE_KEY,
            )

    def test_q_record_self_trial_is_not_empty_q(self):
        q = q_record_self_trial()
        self.assertTrue(q.children)
        self.assertNotEqual(str(q), str(Q()))


class QRecordSelfTrialFilterTests(TestCase):
    def setUp(self):
        super().setUp()
        self.tenant = TenantFactory()

    def test_q_record_self_trial_matches_self_trial_records_only(self):
        self_trial = RecordFactory(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={"support_ticket_type": "SELF TRIAL", "user_id": "self_trial_user"},
        )
        in_trial = RecordFactory(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={"support_ticket_type": "in_trial", "user_id": "in_trial_user"},
        )
        legacy_poster = RecordFactory(
            tenant=self.tenant,
            entity_type=SUPPORT_TICKET_ENTITY_TYPE,
            data={"poster": "self_trial", "user_id": "legacy_poster_user"},
        )

        matched_ids = set(
            Record.objects.filter(q_record_self_trial()).values_list("id", flat=True)
        )
        self.assertEqual(matched_ids, {self_trial.id, legacy_poster.id})
        self.assertNotIn(in_trial.id, matched_ids)

    def test_q_record_support_ticket_type_key_empty_type_matches_nothing(self):
        q = q_record_support_ticket_type_key("__no_such_type_key__")
        self.assertFalse(Record.objects.filter(q).exists())
