"""Bad request bodies are rejected with 400 and change nothing; valid id formats are accepted."""

from rest_framework import status

from .helpers import MakeClosedAPITestCase


class MakeClosedInvalidBodyTests(MakeClosedAPITestCase):
    def assert_rejected(self, body):
        response = self.post_body(body)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, body)
        self.assertIn("error", response.data)
        return response

    def test_rejects_a_missing_praja_ids_field(self):
        self.assert_rejected({})

    def test_rejects_record_ids_instead_of_praja_ids(self):
        self.assert_rejected({"record_ids": [1, 2]})

    def test_rejects_an_empty_list(self):
        self.assert_rejected({"praja_ids": []})

    def test_rejects_null(self):
        self.assert_rejected({"praja_ids": None})

    def test_rejects_a_single_string_instead_of_a_list(self):
        self.assert_rejected({"praja_ids": "9001"})

    def test_rejects_a_single_number_instead_of_a_list(self):
        self.assert_rejected({"praja_ids": 9001})

    def test_rejects_an_object_instead_of_a_list(self):
        self.assert_rejected({"praja_ids": {"id": "9001"}})


class MakeClosedInvalidIdTests(MakeClosedAPITestCase):
    def assert_bad_id(self, bad_id):
        response = self.post_ids([bad_id])
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, bad_id)
        self.assertIn("praja_ids[0]", response.data["error"])

    def test_rejects_an_id_with_spaces_inside(self):
        self.assert_bad_id("has space")

    def test_rejects_an_id_with_special_characters(self):
        for bad in ("90'01", "90;01", "90/01", "90.01", "*", "%"):
            with self.subTest(bad=bad):
                self.assert_bad_id(bad)

    def test_rejects_an_id_longer_than_64_characters(self):
        self.assert_bad_id("x" * 65)

    def test_rejects_an_empty_or_blank_string(self):
        for bad in ("", "   "):
            with self.subTest(bad=repr(bad)):
                self.assert_bad_id(bad)

    def test_rejects_zero_and_negative_numbers(self):
        for bad in (0, -5):
            with self.subTest(bad=bad):
                self.assert_bad_id(bad)

    def test_rejects_true_and_false(self):
        for bad in (True, False):
            with self.subTest(bad=bad):
                self.assert_bad_id(bad)

    def test_rejects_decimals(self):
        self.assert_bad_id(12.5)

    def test_rejects_null_inside_the_list(self):
        self.assert_bad_id(None)

    def test_rejects_nested_lists_and_objects(self):
        for bad in (["9001"], {"praja_id": "9001"}):
            with self.subTest(bad=bad):
                self.assert_bad_id(bad)

    def test_names_the_position_of_the_bad_id(self):
        response = self.post_ids(["1", "2", "bad id"])

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("praja_ids[2]", response.data["error"])

    def test_one_bad_id_rejects_the_whole_request_and_closes_nothing(self):
        lead = self.lead()

        response = self.post_ids(["9001", "bad id"])

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        lead.refresh_from_db()
        self.assertEqual(lead.data["lead_stage"], "NOT_CONNECTED")
        self.assertEqual(lead.data["assigned_to"], "rm-123")


class MakeClosedAcceptedIdFormatTests(MakeClosedAPITestCase):
    def test_accepts_a_64_character_id(self):
        long_id = "a" * 64
        self.lead(praja_id=long_id)

        response = self.post_ids([long_id])

        self.assertEqual(response.data["updated"], [long_id])

    def test_trims_spaces_around_ids(self):
        self.lead()

        response = self.post_ids(["  9001  "])

        self.assertEqual(response.data["updated"], ["9001"])

    def test_accepts_letters_dashes_and_underscores(self):
        self.lead(praja_id="PRAJA_1-a")

        response = self.post_ids(["PRAJA_1-a"])

        self.assertEqual(response.data["updated"], ["PRAJA_1-a"])

    def test_matches_ids_stored_as_numbers_or_text(self):
        self.lead(praja_id=445566)
        self.lead(praja_id="778899")

        response = self.post_ids([445566, "778899"])

        self.assertEqual(response.data["updated"], ["445566", "778899"])

    def test_number_and_text_forms_of_the_same_id_count_once(self):
        self.lead(praja_id="445566")

        response = self.post_ids([445566, "445566"])

        self.assertEqual(response.data["updated"], ["445566"])

    def test_matching_is_case_sensitive(self):
        self.lead(praja_id="PRAJA-1")

        response = self.post_ids(["praja-1"])

        self.assertEqual(response.data["not_found"], ["praja-1"])
