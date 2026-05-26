"""Tests for rule engine simple condition evaluation."""

from django.test import TestCase

from crm_records.rule_engine import _evaluate_condition, _is_simple_condition


class RuleEngineNotEqualConditionTests(TestCase):
    """!= operator and support.take_break seed condition."""

    def test_not_equal_literal(self):
        condition = {"!=": [{"var": "record_data.resolution_status"}, "WIP"]}
        ctx = {"record_data": {"resolution_status": "Resolved"}, "payload": {}}
        self.assertTrue(_evaluate_condition(condition, ctx))

    def test_not_equal_when_equal_returns_false(self):
        condition = {"!=": [{"var": "record_data.resolution_status"}, "WIP"]}
        ctx = {"record_data": {"resolution_status": "WIP"}, "payload": {}}
        self.assertFalse(_evaluate_condition(condition, ctx))

    def test_support_take_break_seed_condition_matches_non_wip(self):
        condition = {
            "and": [
                {"!=": [{"var": "record_data.resolution_status"}, "WIP"]},
                {"!=": [{"var": "payload.resolution_status"}, "WIP"]},
            ]
        }
        ctx = {
            "record_data": {"resolution_status": "Resolved"},
            "payload": {"resolution_status": None},
        }
        self.assertTrue(_evaluate_condition(condition, ctx))
        self.assertTrue(_is_simple_condition(condition))

    def test_support_take_break_seed_condition_rejects_record_wip(self):
        condition = {
            "and": [
                {"!=": [{"var": "record_data.resolution_status"}, "WIP"]},
                {"!=": [{"var": "payload.resolution_status"}, "WIP"]},
            ]
        }
        ctx = {
            "record_data": {"resolution_status": "WIP"},
            "payload": {"resolution_status": None},
        }
        self.assertFalse(_evaluate_condition(condition, ctx))

    def test_support_take_break_seed_condition_rejects_payload_wip(self):
        condition = {
            "and": [
                {"!=": [{"var": "record_data.resolution_status"}, "WIP"]},
                {"!=": [{"var": "payload.resolution_status"}, "WIP"]},
            ]
        }
        ctx = {
            "record_data": {"resolution_status": "New"},
            "payload": {"resolution_status": "WIP"},
        }
        self.assertFalse(_evaluate_condition(condition, ctx))
