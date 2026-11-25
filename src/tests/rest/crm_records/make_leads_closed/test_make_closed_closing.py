"""What make-closed changes on a lead, and what it must leave alone."""

from rest_framework import status

from .helpers import (
    HISTORY_KEYS,
    SCHEDULE_KEYS,
    MakeClosedAPITestCase,
    worked_lead_data,
)


class MakeClosedClosingTests(MakeClosedAPITestCase):
    def test_sets_lead_stage_to_closed(self):
        lead = self.lead()

        response = self.post_ids(["9001"])

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data, {"updated": ["9001"], "skipped": [], "not_found": []})
        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "CLOSED")

    def test_clears_assignee_and_every_scheduled_follow_up(self):
        lead = self.lead()

        self.post_ids(["9001"])

        lead.refresh_from_db()
        for key in SCHEDULE_KEYS:
            self.assertNotIn(key, lead.data, key)

    def test_keeps_call_history(self):
        original = worked_lead_data()
        lead = self.lead()

        self.post_ids(["9001"])

        lead.refresh_from_db()
        for key in HISTORY_KEYS:
            self.assertEqual(lead.data[key], original[key], key)

    def test_keeps_unrelated_lead_details(self):
        lead = self.lead(phone_number="9876543210", tags=["vip"], extra={"district": "Pune"})

        self.post_ids(["9001"])

        lead.refresh_from_db()
        self.assertEqual(lead.data["name"], "Worked Lead")
        self.assertEqual(lead.data["praja_id"], "9001")
        self.assertEqual(lead.data["lead_source"], "referral")
        self.assertEqual(lead.data["phone_number"], "9876543210")
        self.assertEqual(lead.data["tags"], ["vip"])
        self.assertEqual(lead.data["extra"], {"district": "Pune"})

    def test_closes_every_known_stage(self):
        stages = ["FRESH", "IN_QUEUE", "ASSIGNED", "SNOOZED", "NOT_CONNECTED", "NOT_INTERESTED", "TRIAL_ACTIVATED"]
        for index, stage in enumerate(stages, start=1):
            with self.subTest(stage=stage):
                lead = self.lead(praja_id=f"stage-{index}", lead_stage=stage)

                response = self.post_ids([f"stage-{index}"])

                self.assertEqual(response.data["updated"], [f"stage-{index}"])
                lead.refresh_from_db()
                self.assertEqual(lead.data["lead_stage"], "CLOSED")

    def test_closes_an_unknown_custom_stage(self):
        lead = self.lead(lead_stage="CUSTOM_STAGE_FROM_IMPORT")

        self.post_ids(["9001"])

        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "CLOSED")

    def test_closes_a_lead_with_no_lead_stage(self):
        lead = self.raw_record({"name": "No stage", "praja_id": "9001"})

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["updated"], ["9001"])
        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "CLOSED")

    def test_removes_assignee_key_even_when_it_is_null(self):
        lead = self.raw_record({"praja_id": "9001", "lead_stage": "CLOSED", "assigned_to": None})

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["updated"], ["9001"])
        lead.refresh_from_db()
        self.assertNotIn("assigned_to", lead.data)

    def test_does_not_add_call_attempts_when_missing(self):
        lead = self.raw_record({"praja_id": "9001", "lead_stage": "FRESH"})

        self.post_ids(["9001"])

        lead.refresh_from_db()
        self.assertNotIn("call_attempts", lead.data)

    def test_cleans_up_a_closed_lead_that_still_has_an_rm(self):
        lead = self.lead(lead_stage="CLOSED")

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["updated"], ["9001"])
        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "CLOSED")
        for key in SCHEDULE_KEYS:
            self.assertNotIn(key, lead.data, key)

    def test_saves_uppercase_closed_when_cleaning_a_lowercase_closed_lead(self):
        lead = self.raw_record({"praja_id": "9001", "lead_stage": "closed", "assigned_to": "rm-1"})

        response = self.post_ids(["9001"])

        self.assertEqual(response.data["updated"], ["9001"])
        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "CLOSED")
        self.assertNotIn("assigned_to", lead.data)

    def test_bumps_updated_at_on_closed_leads(self):
        lead = self.lead()
        before = lead.updated_at

        self.post_ids(["9001"])

        lead.refresh_from_db()
        self.assertGreater(lead.updated_at, before)

    def test_closes_many_leads_in_one_request(self):
        leads = [self.lead(praja_id=str(n)) for n in range(1, 6)]

        response = self.post_ids([str(n) for n in range(1, 6)])

        self.assertEqual(response.data["updated"], ["1", "2", "3", "4", "5"])
        for lead in leads:
            lead.refresh_from_db()
            self.assertEqual(lead.data["lead_stage"], "CLOSED")
