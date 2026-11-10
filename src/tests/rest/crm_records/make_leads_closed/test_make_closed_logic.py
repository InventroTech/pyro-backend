"""apply_closed and parse_praja_ids on their own (no database)."""

from django.test import SimpleTestCase

from crm_records.make_closed import MakeClosedError, apply_closed, parse_praja_ids
from .helpers import HISTORY_KEYS, SCHEDULE_KEYS, worked_lead_data


class ApplyClosedTests(SimpleTestCase):
    def test_sets_stage_and_clears_the_schedule(self):
        data = worked_lead_data()

        self.assertTrue(apply_closed(data))

        self.assertEqual(data["lead_stage"], "CLOSED")
        for key in SCHEDULE_KEYS:
            self.assertNotIn(key, data, key)

    def test_keeps_call_history(self):
        data = worked_lead_data()

        apply_closed(data)

        for key in HISTORY_KEYS:
            self.assertIn(key, data, key)

    def test_reports_no_change_for_a_clean_closed_lead(self):
        data = {"lead_stage": "CLOSED", "call_attempts": 4}

        self.assertFalse(apply_closed(data))
        self.assertEqual(data, {"lead_stage": "CLOSED", "call_attempts": 4})

    def test_treats_lowercase_or_padded_closed_as_closed(self):
        for stage in ("closed", " Closed ", "CLOSED"):
            with self.subTest(stage=stage):
                self.assertFalse(apply_closed({"lead_stage": stage}))

    def test_clears_a_leftover_assignee_on_a_closed_lead(self):
        data = {"lead_stage": "CLOSED", "assigned_to": "rm-1"}

        self.assertTrue(apply_closed(data))
        self.assertNotIn("assigned_to", data)

    def test_clears_a_null_assignee_key(self):
        data = {"lead_stage": "CLOSED", "assigned_to": None}

        self.assertTrue(apply_closed(data))
        self.assertNotIn("assigned_to", data)

    def test_closes_an_empty_lead(self):
        data = {}

        self.assertTrue(apply_closed(data))
        self.assertEqual(data, {"lead_stage": "CLOSED"})

    def test_treats_a_null_stage_as_not_closed(self):
        data = {"lead_stage": None}

        self.assertTrue(apply_closed(data))
        self.assertEqual(data["lead_stage"], "CLOSED")

    def test_does_not_add_call_attempts(self):
        data = {"lead_stage": "FRESH"}

        apply_closed(data)

        self.assertNotIn("call_attempts", data)


class ParsePrajaIdsTests(SimpleTestCase):
    def test_returns_ids_as_text(self):
        self.assertEqual(parse_praja_ids([1793876, "1793877"]), ["1793876", "1793877"])

    def test_trims_dedupes_and_keeps_order(self):
        self.assertEqual(parse_praja_ids([" 12 ", 34, "12", "PRAJA-1"]), ["12", "34", "PRAJA-1"])

    def test_accepts_64_characters_and_rejects_65(self):
        self.assertEqual(parse_praja_ids(["a" * 64]), ["a" * 64])
        with self.assertRaises(MakeClosedError):
            parse_praja_ids(["a" * 65])

    def test_rejects_empty_or_non_list_input(self):
        for raw in ([], None, "12", 12, {"praja_ids": [1]}):
            with self.subTest(raw=raw):
                with self.assertRaises(MakeClosedError):
                    parse_praja_ids(raw)

    def test_rejects_bad_ids(self):
        for bad in ("has space", "", "  ", "90.01", "*", 0, -5, True, False, 12.5, None, ["1"], {"id": 1}):
            with self.subTest(bad=bad):
                with self.assertRaises(MakeClosedError):
                    parse_praja_ids([bad])

    def test_error_names_the_position_of_the_bad_id(self):
        with self.assertRaisesMessage(MakeClosedError, "praja_ids[1]"):
            parse_praja_ids(["1", "bad id"])
