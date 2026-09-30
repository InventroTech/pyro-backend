from django.test import SimpleTestCase

from crm_records.mixpanel_properties import (
    apply_lead_mixpanel_attributes,
    lead_mixpanel_properties,
)


class LeadMixpanelPropertiesTest(SimpleTestCase):
    def test_missing_attributes_are_sent_as_null(self):
        props = lead_mixpanel_properties({"name": "A"}, lead_id=1)
        self.assertEqual(props["lead_id"], 1)
        self.assertEqual(props["name"], "A")
        self.assertIsNone(props["referred_through_precheck_logic"])
        self.assertIsNone(props["contact_graph_score"])

    def test_values_are_coerced_to_bool_and_float(self):
        props = lead_mixpanel_properties(
            {"referred_through_precheck_logic": "true", "contact_graph_score": "0.73"}
        )
        self.assertIs(props["referred_through_precheck_logic"], True)
        self.assertEqual(props["contact_graph_score"], 0.73)

        props = lead_mixpanel_properties(
            {"referred_through_precheck_logic": False, "contact_graph_score": 1}
        )
        self.assertIs(props["referred_through_precheck_logic"], False)
        self.assertEqual(props["contact_graph_score"], 1.0)

    def test_unparseable_values_become_null(self):
        props = apply_lead_mixpanel_attributes(
            {"referred_through_precheck_logic": "maybe", "contact_graph_score": "abc"}
        )
        self.assertIsNone(props["referred_through_precheck_logic"])
        self.assertIsNone(props["contact_graph_score"])

    def test_lead_data_overrides_base(self):
        props = lead_mixpanel_properties({"lead_score": 9}, lead_score=None)
        self.assertEqual(props["lead_score"], 9)
