from django.test import SimpleTestCase

from support_ticket.ticket_types import (
    SELF_TRIAL_TICKET_TYPE_KEY,
    canonical_support_ticket_type_key,
    raw_field_values_for_type_key,
)


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
