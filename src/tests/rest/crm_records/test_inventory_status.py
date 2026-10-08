"""Tests for the combined procurement / inventory request status."""

from django.test import SimpleTestCase

from crm_records.inventory_status import (
    BUILTIN_STATUS_VALUES,
    DEFAULT_PAGES,
    DEFAULT_STATUSES,
    _is_generated_status_text,
    allowed_status_values,
    get_tenant_status_config,
    legacy_codes_for,
    merge_status_config,
    normalize_request_status,
    normalize_status_code,
    stage_filter_values,
    status_labels,
    statuses_for_page,
    sync_request_status,
    validate_request_status,
)


class NormalizeRequestStatusTests(SimpleTestCase):
    def test_legacy_codes_map_to_new(self):
        self.assertEqual(normalize_request_status("VENDOR_IDENTIFIED"), "APPROVED")
        self.assertEqual(normalize_request_status("in shipping"), "ORDERED")

    def test_new_codes_unchanged(self):
        self.assertEqual(normalize_request_status("in_cart"), "IN_CART")
        self.assertEqual(normalize_request_status(None), "")


class SyncRequestStatusTests(SimpleTestCase):
    def test_legacy_status_rewritten_with_label(self):
        data = sync_request_status(
            {"status": "VENDOR_IDENTIFIED", "status_text": "VENDOR_IDENTIFIED"},
            previous={"status": "NEW_REQUEST"},
        )
        self.assertEqual(data["status"], "APPROVED")
        self.assertEqual(data["status_text"], "Approved")

    def test_custom_status_text_kept(self):
        data = sync_request_status(
            {"status": "APPROVED", "status_text": "Approved by finance"},
            previous={"status": "NEW_REQUEST"},
        )
        self.assertEqual(data["status_text"], "Approved by finance")

    def test_stale_label_replaced_on_status_change(self):
        data = sync_request_status(
            {"status": "IN_CART", "status_text": "Approved"},
            previous={"status": "APPROVED", "status_text": "Approved"},
        )
        self.assertEqual(data["status_text"], "In cart")

    def test_order_sets_shipment_ordered(self):
        data = sync_request_status({"status": "ORDERED"}, previous={"status": "IN_CART"})
        self.assertEqual(data["shipment_status"], "ORDERED")

    def test_order_keeps_in_flight_shipment(self):
        data = sync_request_status(
            {"status": "ORDERED", "shipment_status": "IN_TRANSIT"},
            previous={"status": "IN_CART", "shipment_status": "IN_TRANSIT"},
        )
        self.assertEqual(data["shipment_status"], "IN_TRANSIT")

    def test_manual_delivered_mirrors_shipment(self):
        data = sync_request_status(
            {"status": "DELIVERED", "shipment_status": "IN_TRANSIT"},
            previous={"status": "ORDERED", "shipment_status": "IN_TRANSIT"},
        )
        self.assertEqual(data["shipment_status"], "DELIVERED")

    def test_shipment_delivered_moves_status(self):
        data = sync_request_status(
            {"status": "ORDERED", "shipment_status": "DELIVERED", "status_text": "Ordered"},
            previous={"status": "ORDERED", "shipment_status": "IN_TRANSIT"},
        )
        self.assertEqual(data["status"], "DELIVERED")
        self.assertEqual(data["status_text"], "Delivered")

    def test_legacy_in_shipping_with_exception_moves_status(self):
        data = sync_request_status(
            {"status": "IN_SHIPPING", "shipment_status": "EXCEPTION"},
            previous={"status": "IN_SHIPPING", "shipment_status": "IN_TRANSIT"},
        )
        self.assertEqual(data["status"], "EXCEPTION")

    def test_shipment_back_in_flight_returns_to_ordered(self):
        data = sync_request_status(
            {"status": "DELIVERED", "shipment_status": "IN_TRANSIT"},
            previous={"status": "DELIVERED", "shipment_status": "DELIVERED"},
        )
        self.assertEqual(data["status"], "ORDERED")

    def test_shipment_on_in_cart_means_ordered(self):
        data = sync_request_status(
            {"status": "IN_CART", "shipment_status": "ORDERED"},
            previous={"status": "IN_CART"},
        )
        self.assertEqual(data["status"], "ORDERED")

    def test_shipment_on_pending_request_ignored(self):
        data = sync_request_status(
            {"status": "NEW_REQUEST", "shipment_status": "IN_TRANSIT"},
            previous={"status": "NEW_REQUEST"},
        )
        self.assertEqual(data["status"], "NEW_REQUEST")

    def test_unrelated_save_leaves_status_alone(self):
        data = sync_request_status(
            {"status": "ON_HOLD", "status_text": "Waiting on budget", "notes": "x"},
            previous={"status": "ON_HOLD", "status_text": "Waiting on budget"},
        )
        self.assertEqual(data["status"], "ON_HOLD")
        self.assertEqual(data["status_text"], "Waiting on budget")


class StatusConfigTests(SimpleTestCase):
    def test_defaults(self):
        cfg = merge_status_config(None)
        values = [s["value"] for s in cfg["statuses"]]
        self.assertEqual(
            values,
            ["NEW_REQUEST", "REQ_TO_VERIFY", "APPROVED", "IN_CART", "ON_HOLD",
             "REJECTED", "ORDERED", "DELIVERED", "EXCEPTION"],
        )
        self.assertEqual(cfg["pages"][0]["id"], "pending_approval")

    def test_override_label_page_and_add_status(self):
        cfg = merge_status_config({
            "statuses": [
                {"value": "APPROVED", "label": "Vendor OK", "page": "in_cart"},
                {"value": "PAID", "label": "Paid", "color": "green", "page": "closed"},
                {"value": "ON_HOLD", "active": False},
            ]
        })
        by_value = {s["value"]: s for s in cfg["statuses"]}
        self.assertEqual(by_value["APPROVED"]["label"], "Vendor OK")
        self.assertEqual(by_value["APPROVED"]["page"], "in_cart")
        self.assertFalse(by_value["ON_HOLD"]["active"])
        self.assertEqual(by_value["PAID"]["page"], "closed")
        self.assertFalse(by_value["PAID"]["builtin"])

    def test_legacy_value_in_config_targets_new_status(self):
        cfg = merge_status_config({"statuses": [{"value": "VENDOR_IDENTIFIED", "label": "OK"}]})
        by_value = {s["value"]: s for s in cfg["statuses"]}
        self.assertEqual(by_value["APPROVED"]["label"], "OK")
        self.assertNotIn("VENDOR_IDENTIFIED", by_value)

    def test_unknown_page_cleared(self):
        cfg = merge_status_config({"statuses": [{"value": "APPROVED", "page": "nope"}]})
        by_value = {s["value"]: s for s in cfg["statuses"]}
        self.assertIsNone(by_value["APPROVED"]["page"])

    def test_stage_filter_values_include_legacy(self):
        cfg = merge_status_config(None)
        self.assertEqual(
            stage_filter_values(cfg, ["ordered"]),
            ["ORDERED", "IN_SHIPPING", "EXCEPTION"],
        )
        self.assertIn("VENDOR_IDENTIFIED", stage_filter_values(cfg, ["pending_approval"]))
        self.assertEqual(stage_filter_values(cfg, ["delivered"]), ["DELIVERED"])

    def test_validate_request_status(self):
        cfg = merge_status_config(None)
        self.assertIsNone(validate_request_status("VENDOR_IDENTIFIED", cfg))
        self.assertIsNone(validate_request_status("", cfg))
        self.assertIn("Unknown status", validate_request_status("PAID", cfg))


ALL_STATUSES = [
    "NEW_REQUEST", "REQ_TO_VERIFY", "APPROVED", "IN_CART", "ON_HOLD",
    "REJECTED", "ORDERED", "DELIVERED", "EXCEPTION",
]


class NormalizeStatusCodeTests(SimpleTestCase):
    def test_none_is_empty(self):
        self.assertEqual(normalize_status_code(None), "")

    def test_trims_uppercases_and_snakes_spaces(self):
        self.assertEqual(normalize_status_code("  in cart "), "IN_CART")

    def test_non_string_values(self):
        self.assertEqual(normalize_status_code(123), "123")

    def test_does_not_map_legacy_codes(self):
        self.assertEqual(normalize_status_code("vendor identified"), "VENDOR_IDENTIFIED")


class NormalizeRequestStatusMoreTests(SimpleTestCase):
    def test_every_builtin_status_is_unchanged(self):
        for code in ALL_STATUSES:
            self.assertEqual(normalize_request_status(code), code)
            self.assertEqual(normalize_request_status(code.lower()), code)

    def test_legacy_with_spaces_and_case(self):
        self.assertEqual(normalize_request_status(" vendor identified "), "APPROVED")
        self.assertEqual(normalize_request_status("In_Shipping"), "ORDERED")

    def test_custom_code_passes_through(self):
        self.assertEqual(normalize_request_status("paid"), "PAID")

    def test_empty_string(self):
        self.assertEqual(normalize_request_status(""), "")
        self.assertEqual(normalize_request_status("   "), "")


class LegacyCodesForTests(SimpleTestCase):
    def test_approved_and_ordered_have_legacy_codes(self):
        self.assertEqual(legacy_codes_for("APPROVED"), {"VENDOR_IDENTIFIED"})
        self.assertEqual(legacy_codes_for("ORDERED"), {"IN_SHIPPING"})

    def test_other_statuses_have_none(self):
        for code in ["NEW_REQUEST", "IN_CART", "DELIVERED", "EXCEPTION", "PAID", ""]:
            self.assertEqual(legacy_codes_for(code), set())


class DefaultsTests(SimpleTestCase):
    def test_builtin_values_are_the_nine_statuses(self):
        self.assertEqual(BUILTIN_STATUS_VALUES, frozenset(ALL_STATUSES))

    def test_every_default_status_points_to_a_default_page(self):
        page_ids = {p["id"] for p in DEFAULT_PAGES}
        for status in DEFAULT_STATUSES:
            self.assertIn(status["page"], page_ids, status["value"])

    def test_every_default_page_has_at_least_one_status(self):
        cfg = merge_status_config(None)
        for page in DEFAULT_PAGES:
            self.assertTrue(statuses_for_page(cfg, page["id"]), page["id"])

    def test_default_page_order(self):
        cfg = merge_status_config(None)
        self.assertEqual(
            [p["id"] for p in cfg["pages"]],
            ["pending_approval", "in_cart", "ordered", "delivered", "closed"],
        )

    def test_default_status_page_mapping(self):
        cfg = merge_status_config(None)
        pages = {s["value"]: s["page"] for s in cfg["statuses"]}
        self.assertEqual(pages, {
            "NEW_REQUEST": "pending_approval",
            "REQ_TO_VERIFY": "pending_approval",
            "APPROVED": "pending_approval",
            "ON_HOLD": "pending_approval",
            "IN_CART": "in_cart",
            "ORDERED": "ordered",
            "EXCEPTION": "ordered",
            "DELIVERED": "delivered",
            "REJECTED": "closed",
        })

    def test_default_colours_are_unique(self):
        colours = [s["color"] for s in DEFAULT_STATUSES]
        self.assertEqual(len(colours), len(set(colours)))

    def test_default_colours(self):
        self.assertEqual(
            {s["value"]: (s["background"], s["color"]) for s in DEFAULT_STATUSES},
            {
                "NEW_REQUEST": ("#FFFBEB", "#78350F"),
                "REQ_TO_VERIFY": ("#F5F3FF", "#6D28D9"),
                "APPROVED": ("#DCFCE7", "#16A34A"),
                "IN_CART": ("#E8F1FD", "#1B6FE8"),
                "ON_HOLD": ("#FFF7ED", "#F97316"),
                "REJECTED": ("#FEE2E2", "#DC2626"),
                "ORDERED": ("#EEF2FA", "#1A3673"),
                "DELIVERED": ("#DCFCE7", "#15803D"),
                "EXCEPTION": ("#FEE2E2", "#B91C1C"),
            },
        )

    def test_background_override_and_new_status_default(self):
        cfg = merge_status_config({
            "statuses": [
                {"value": "APPROVED", "background": "#FFFFFF"},
                {"value": "PAID"},
            ]
        })
        by_value = {s["value"]: s for s in cfg["statuses"]}
        self.assertEqual(by_value["APPROVED"]["background"], "#FFFFFF")
        self.assertEqual(by_value["APPROVED"]["color"], "#16A34A")
        self.assertIsNone(by_value["PAID"]["background"])


class MergeStatusConfigMoreTests(SimpleTestCase):
    def _by_value(self, cfg):
        return {s["value"]: s for s in cfg["statuses"]}

    def test_non_mapping_config_returns_defaults(self):
        for raw in (None, "x", [], 42, {}):
            cfg = merge_status_config(raw)
            self.assertEqual([s["value"] for s in cfg["statuses"]], ALL_STATUSES)
            self.assertEqual(len(cfg["pages"]), 5)

    def test_defaults_are_active_builtin_and_ordered(self):
        cfg = merge_status_config(None)
        for index, status in enumerate(cfg["statuses"]):
            self.assertTrue(status["active"])
            self.assertTrue(status["builtin"])
            self.assertEqual(status["order"], index + 1)

    def test_merge_does_not_mutate_module_defaults(self):
        merge_status_config({
            "pages": [{"id": "only", "label": "Only"}],
            "statuses": [{"value": "APPROVED", "label": "Changed", "page": "only"}],
        })
        approved = next(s for s in DEFAULT_STATUSES if s["value"] == "APPROVED")
        self.assertEqual(approved["label"], "Approved")
        self.assertEqual(approved["page"], "pending_approval")
        self.assertEqual(merge_status_config(None)["pages"][0]["id"], "pending_approval")

    def test_tenant_pages_replace_defaults_and_sort_by_order(self):
        cfg = merge_status_config({
            "pages": [
                {"id": "b", "label": "B", "order": 2},
                {"id": "a", "label": "A", "order": 1},
            ]
        })
        self.assertEqual([p["id"] for p in cfg["pages"]], ["a", "b"])

    def test_page_label_and_order_defaults(self):
        cfg = merge_status_config({"pages": [{"id": "x"}, {"id": "y"}]})
        self.assertEqual(cfg["pages"], [
            {"id": "x", "label": "x", "order": 1},
            {"id": "y", "label": "y", "order": 2},
        ])

    def test_invalid_pages_skipped(self):
        cfg = merge_status_config({"pages": [{"label": "No id"}, "bad", {"id": "  "}, {"id": "ok"}]})
        self.assertEqual([p["id"] for p in cfg["pages"]], ["ok"])

    def test_all_invalid_pages_keep_defaults(self):
        cfg = merge_status_config({"pages": [{"label": "No id"}, None]})
        self.assertEqual(len(cfg["pages"]), 5)

    def test_pages_not_a_list_keep_defaults(self):
        cfg = merge_status_config({"pages": {"id": "x"}})
        self.assertEqual(len(cfg["pages"]), 5)

    def test_statuses_on_removed_pages_become_unmapped(self):
        cfg = merge_status_config({"pages": [{"id": "pending_approval", "label": "P"}]})
        by_value = self._by_value(cfg)
        self.assertEqual(by_value["NEW_REQUEST"]["page"], "pending_approval")
        self.assertIsNone(by_value["IN_CART"]["page"])
        self.assertIsNone(by_value["DELIVERED"]["page"])

    def test_new_status_defaults(self):
        cfg = merge_status_config({"statuses": [{"value": "waiting_payment"}]})
        entry = self._by_value(cfg)["WAITING_PAYMENT"]
        self.assertEqual(entry["label"], "Waiting payment")
        self.assertEqual(entry["color"], "gray")
        self.assertIsNone(entry["page"])
        self.assertEqual(entry["order"], 10)
        self.assertTrue(entry["active"])
        self.assertFalse(entry["builtin"])
        self.assertEqual(cfg["statuses"][-1]["value"], "WAITING_PAYMENT")

    def test_builtin_can_be_hidden_but_not_removed(self):
        cfg = merge_status_config({"statuses": [{"value": "REJECTED", "active": False}]})
        by_value = self._by_value(cfg)
        self.assertIn("REJECTED", by_value)
        self.assertFalse(by_value["REJECTED"]["active"])
        self.assertTrue(by_value["REJECTED"]["builtin"])
        self.assertEqual(len(cfg["statuses"]), 9)

    def test_order_override_reorders(self):
        cfg = merge_status_config({"statuses": [{"value": "EXCEPTION", "order": 0}]})
        self.assertEqual(cfg["statuses"][0]["value"], "EXCEPTION")

    def test_blank_overrides_are_ignored(self):
        cfg = merge_status_config({
            "statuses": [{"value": "APPROVED", "label": "", "color": None, "page": ""}]
        })
        entry = self._by_value(cfg)["APPROVED"]
        self.assertEqual(entry["label"], "Approved")
        self.assertEqual(entry["color"], "#16A34A")
        self.assertEqual(entry["page"], "pending_approval")

    def test_label_is_trimmed(self):
        cfg = merge_status_config({"statuses": [{"value": "APPROVED", "label": "  OK  "}]})
        self.assertEqual(self._by_value(cfg)["APPROVED"]["label"], "OK")

    def test_invalid_status_entries_skipped(self):
        cfg = merge_status_config({"statuses": ["PAID", None, {"label": "No value"}, {"value": " "}]})
        self.assertEqual([s["value"] for s in cfg["statuses"]], ALL_STATUSES)

    def test_statuses_not_a_list_ignored(self):
        cfg = merge_status_config({"statuses": {"value": "PAID"}})
        self.assertEqual(len(cfg["statuses"]), 9)

    def test_duplicate_entries_later_wins(self):
        cfg = merge_status_config({
            "statuses": [
                {"value": "PAID", "label": "First"},
                {"value": "paid", "label": "Second"},
            ]
        })
        values = [s["value"] for s in cfg["statuses"]]
        self.assertEqual(values.count("PAID"), 1)
        self.assertEqual(self._by_value(cfg)["PAID"]["label"], "Second")

    def test_custom_status_on_custom_page(self):
        cfg = merge_status_config({
            "pages": [{"id": "pending_approval", "label": "P"}, {"id": "paid", "label": "Paid"}],
            "statuses": [{"value": "PAID", "page": "paid"}],
        })
        self.assertEqual(self._by_value(cfg)["PAID"]["page"], "paid")
        self.assertEqual(statuses_for_page(cfg, "paid"), ["PAID"])

    def test_active_is_coerced_to_bool(self):
        cfg = merge_status_config({"statuses": [{"value": "ON_HOLD", "active": 0}]})
        self.assertIs(self._by_value(cfg)["ON_HOLD"]["active"], False)


class StatusLookupTests(SimpleTestCase):
    def test_status_labels_default(self):
        labels = status_labels()
        self.assertEqual(labels["APPROVED"], "Approved")
        self.assertEqual(labels["IN_CART"], "In cart")
        self.assertEqual(len(labels), 9)

    def test_status_labels_from_config(self):
        cfg = merge_status_config({"statuses": [{"value": "APPROVED", "label": "Vendor OK"}]})
        self.assertEqual(status_labels(cfg)["APPROVED"], "Vendor OK")

    def test_statuses_for_page(self):
        cfg = merge_status_config(None)
        self.assertEqual(
            statuses_for_page(cfg, "pending_approval"),
            ["NEW_REQUEST", "REQ_TO_VERIFY", "APPROVED", "ON_HOLD"],
        )
        self.assertEqual(statuses_for_page(cfg, "closed"), ["REJECTED"])
        self.assertEqual(statuses_for_page(cfg, "nope"), [])

    def test_statuses_for_page_includes_hidden(self):
        cfg = merge_status_config({"statuses": [{"value": "ON_HOLD", "active": False}]})
        self.assertIn("ON_HOLD", statuses_for_page(cfg, "pending_approval"))

    def test_allowed_status_values_include_custom_and_hidden(self):
        cfg = merge_status_config({
            "statuses": [{"value": "PAID"}, {"value": "ON_HOLD", "active": False}]
        })
        allowed = allowed_status_values(cfg)
        self.assertIn("PAID", allowed)
        self.assertIn("ON_HOLD", allowed)
        self.assertEqual(len(allowed), 10)


class StageFilterValuesMoreTests(SimpleTestCase):
    def setUp(self):
        self.cfg = merge_status_config(None)

    def test_pending_approval_exact(self):
        self.assertEqual(
            stage_filter_values(self.cfg, ["pending_approval"]),
            ["NEW_REQUEST", "REQ_TO_VERIFY", "APPROVED", "VENDOR_IDENTIFIED", "ON_HOLD"],
        )

    def test_multiple_pages(self):
        self.assertEqual(
            stage_filter_values(self.cfg, ["in_cart", "delivered"]),
            ["IN_CART", "DELIVERED"],
        )

    def test_page_ids_are_trimmed(self):
        self.assertEqual(stage_filter_values(self.cfg, [" closed "]), ["REJECTED"])

    def test_unknown_page_is_empty(self):
        self.assertEqual(stage_filter_values(self.cfg, ["nope"]), [])
        self.assertEqual(stage_filter_values(self.cfg, []), [])

    def test_repeated_page_has_no_duplicates(self):
        self.assertEqual(stage_filter_values(self.cfg, ["ordered", "ordered"]),
                         ["ORDERED", "IN_SHIPPING", "EXCEPTION"])

    def test_custom_status_on_page(self):
        cfg = merge_status_config({"statuses": [{"value": "PAID", "page": "closed"}]})
        self.assertEqual(stage_filter_values(cfg, ["closed"]), ["REJECTED", "PAID"])

    def test_moved_status_follows_config(self):
        cfg = merge_status_config({"statuses": [{"value": "APPROVED", "page": "in_cart"}]})
        self.assertEqual(stage_filter_values(cfg, ["in_cart"]),
                         ["APPROVED", "VENDOR_IDENTIFIED", "IN_CART"])
        self.assertNotIn("APPROVED", stage_filter_values(cfg, ["pending_approval"]))


class IsGeneratedStatusTextTests(SimpleTestCase):
    def setUp(self):
        self.labels = status_labels()

    def test_generated_values(self):
        for text in ["", None, "   ", "APPROVED", "VENDOR_IDENTIFIED", "IN_SHIPPING",
                     "Approved", "approved", "In cart", "vendor identified", "In shipping"]:
            self.assertTrue(_is_generated_status_text(text, self.labels), repr(text))

    def test_custom_values(self):
        for text in ["Approved by finance", "PAID", "Waiting on budget", "approved!"]:
            self.assertFalse(_is_generated_status_text(text, self.labels), repr(text))

    def test_lowercase_code_is_not_treated_as_code(self):
        self.assertFalse(_is_generated_status_text("in_cart", self.labels))


class SyncRequestStatusMoreTests(SimpleTestCase):
    def test_non_dict_returned_unchanged(self):
        self.assertIsNone(sync_request_status(None))
        self.assertEqual(sync_request_status(["x"]), ["x"])

    def test_mutates_and_returns_same_dict(self):
        data = {"status": "APPROVED"}
        self.assertIs(sync_request_status(data, {"status": "NEW_REQUEST"}), data)

    def test_no_status_leaves_data_alone(self):
        data = sync_request_status({"notes": "x"}, previous={"status": "APPROVED"})
        self.assertEqual(data, {"notes": "x"})

    def test_previous_not_a_mapping(self):
        data = sync_request_status({"status": "APPROVED"}, previous="bad")
        self.assertEqual(data["status_text"], "Approved")

    def test_new_record_gets_label(self):
        data = sync_request_status({"status": "NEW_REQUEST"})
        self.assertEqual(data["status_text"], "New request")

    def test_lowercase_status_is_upper_snaked(self):
        data = sync_request_status({"status": "in cart"}, previous={"status": "APPROVED"})
        self.assertEqual(data["status"], "IN_CART")
        self.assertEqual(data["status_text"], "In cart")

    def test_unchanged_legacy_row_rewritten_on_any_save(self):
        data = sync_request_status(
            {"status": "IN_SHIPPING", "status_text": "IN_SHIPPING", "notes": "x"},
            previous={"status": "IN_SHIPPING", "status_text": "IN_SHIPPING"},
        )
        self.assertEqual(data["status"], "ORDERED")
        self.assertEqual(data["status_text"], "Ordered")

    def test_unchanged_legacy_row_keeps_custom_text(self):
        data = sync_request_status(
            {"status": "VENDOR_IDENTIFIED", "status_text": "Quote received"},
            previous={"status": "VENDOR_IDENTIFIED", "status_text": "Quote received"},
        )
        self.assertEqual(data["status"], "APPROVED")
        self.assertEqual(data["status_text"], "Quote received")

    def test_custom_labels_used(self):
        data = sync_request_status(
            {"status": "APPROVED"}, previous={"status": "NEW_REQUEST"},
            labels={"APPROVED": "Vendor OK"},
        )
        self.assertEqual(data["status_text"], "Vendor OK")

    def test_custom_status_without_label_keeps_text(self):
        data = sync_request_status(
            {"status": "PAID", "status_text": "Paid in full"}, previous={"status": "DELIVERED"},
        )
        self.assertEqual(data["status"], "PAID")
        self.assertEqual(data["status_text"], "Paid in full")

    def test_each_status_change_sets_its_label(self):
        labels = status_labels()
        for code in ALL_STATUSES:
            data = sync_request_status({"status": code, "status_text": ""}, previous={"status": "X"})
            self.assertEqual(data["status_text"], labels[code], code)

    def test_manual_exception_mirrors_shipment(self):
        data = sync_request_status(
            {"status": "EXCEPTION", "shipment_status": "IN_TRANSIT"},
            previous={"status": "ORDERED", "shipment_status": "IN_TRANSIT"},
        )
        self.assertEqual(data["shipment_status"], "EXCEPTION")

    def test_manual_delivered_without_shipment_key_adds_it(self):
        data = sync_request_status({"status": "DELIVERED"}, previous={"status": "ORDERED"})
        self.assertEqual(data["shipment_status"], "DELIVERED")

    def test_order_keeps_out_for_delivery(self):
        data = sync_request_status(
            {"status": "ORDERED", "shipment_status": "OUT_FOR_DELIVERY"},
            previous={"status": "IN_CART"},
        )
        self.assertEqual(data["shipment_status"], "OUT_FOR_DELIVERY")

    def test_order_replaces_not_shipped(self):
        data = sync_request_status(
            {"status": "ORDERED", "shipment_status": "NOT_SHIPPED"},
            previous={"status": "IN_CART", "shipment_status": "NOT_SHIPPED"},
        )
        self.assertEqual(data["shipment_status"], "ORDERED")

    def test_manual_back_to_ordered_from_delivered_resets_shipment(self):
        data = sync_request_status(
            {"status": "ORDERED", "shipment_status": "DELIVERED"},
            previous={"status": "DELIVERED", "shipment_status": "DELIVERED"},
        )
        self.assertEqual(data["shipment_status"], "ORDERED")

    def test_shipment_exception_moves_ordered(self):
        data = sync_request_status(
            {"status": "ORDERED", "shipment_status": "EXCEPTION", "status_text": "Ordered"},
            previous={"status": "ORDERED", "shipment_status": "IN_TRANSIT"},
        )
        self.assertEqual(data["status"], "EXCEPTION")
        self.assertEqual(data["status_text"], "Exception")

    def test_shipment_delivered_after_exception(self):
        data = sync_request_status(
            {"status": "EXCEPTION", "shipment_status": "DELIVERED"},
            previous={"status": "EXCEPTION", "shipment_status": "EXCEPTION"},
        )
        self.assertEqual(data["status"], "DELIVERED")

    def test_shipment_in_flight_keeps_ordered(self):
        for shipment in ("IN_TRANSIT", "OUT_FOR_DELIVERY"):
            data = sync_request_status(
                {"status": "ORDERED", "shipment_status": shipment},
                previous={"status": "ORDERED", "shipment_status": "ORDERED"},
            )
            self.assertEqual(data["status"], "ORDERED")

    def test_exception_back_in_flight_returns_to_ordered(self):
        data = sync_request_status(
            {"status": "EXCEPTION", "shipment_status": "OUT_FOR_DELIVERY"},
            previous={"status": "EXCEPTION", "shipment_status": "EXCEPTION"},
        )
        self.assertEqual(data["status"], "ORDERED")

    def test_shipment_delivered_on_approved_jumps_to_delivered(self):
        data = sync_request_status(
            {"status": "APPROVED", "shipment_status": "DELIVERED"},
            previous={"status": "APPROVED"},
        )
        self.assertEqual(data["status"], "DELIVERED")

    def test_shipment_on_approved_means_ordered(self):
        data = sync_request_status(
            {"status": "APPROVED", "shipment_status": "IN_TRANSIT"},
            previous={"status": "APPROVED"},
        )
        self.assertEqual(data["status"], "ORDERED")

    def test_auto_move_overwrites_custom_text(self):
        data = sync_request_status(
            {"status": "APPROVED", "shipment_status": "IN_TRANSIT", "status_text": "Quote OK"},
            previous={"status": "APPROVED", "status_text": "Quote OK"},
        )
        self.assertEqual(data["status_text"], "Ordered")

    def test_not_shipped_on_in_cart_ignored(self):
        data = sync_request_status(
            {"status": "IN_CART", "shipment_status": "NOT_SHIPPED"},
            previous={"status": "IN_CART"},
        )
        self.assertEqual(data["status"], "IN_CART")

    def test_shipment_ignored_on_hold_and_rejected(self):
        for code in ("ON_HOLD", "REJECTED", "REQ_TO_VERIFY"):
            data = sync_request_status(
                {"status": code, "shipment_status": "DELIVERED"},
                previous={"status": code},
            )
            self.assertEqual(data["status"], code)

    def test_unchanged_shipment_does_not_move_status(self):
        data = sync_request_status(
            {"status": "ORDERED", "shipment_status": "DELIVERED"},
            previous={"status": "ORDERED", "shipment_status": "DELIVERED"},
        )
        self.assertEqual(data["status"], "ORDERED")

    def test_cleared_shipment_does_not_move_status(self):
        data = sync_request_status(
            {"status": "DELIVERED", "shipment_status": ""},
            previous={"status": "DELIVERED", "shipment_status": "DELIVERED"},
        )
        self.assertEqual(data["status"], "DELIVERED")

    def test_shipment_compared_case_insensitively(self):
        data = sync_request_status(
            {"status": "ORDERED", "shipment_status": "delivered"},
            previous={"status": "ORDERED", "shipment_status": "in transit"},
        )
        self.assertEqual(data["status"], "DELIVERED")

    def test_idempotent(self):
        first = sync_request_status(
            {"status": "ORDERED", "shipment_status": "DELIVERED"},
            previous={"status": "ORDERED", "shipment_status": "IN_TRANSIT"},
        )
        snapshot = dict(first)
        second = sync_request_status(dict(first), previous=snapshot)
        self.assertEqual(second, snapshot)


class ValidateRequestStatusMoreTests(SimpleTestCase):
    def test_none_and_blank(self):
        cfg = merge_status_config(None)
        self.assertIsNone(validate_request_status(None, cfg))
        self.assertIsNone(validate_request_status("  ", cfg))

    def test_lowercase_and_legacy_allowed(self):
        cfg = merge_status_config(None)
        self.assertIsNone(validate_request_status("approved", cfg))
        self.assertIsNone(validate_request_status("in shipping", cfg))

    def test_every_builtin_allowed(self):
        cfg = merge_status_config(None)
        for code in ALL_STATUSES:
            self.assertIsNone(validate_request_status(code, cfg))

    def test_hidden_builtin_still_allowed(self):
        cfg = merge_status_config({"statuses": [{"value": "ON_HOLD", "active": False}]})
        self.assertIsNone(validate_request_status("ON_HOLD", cfg))

    def test_custom_status_allowed_when_configured(self):
        cfg = merge_status_config({"statuses": [{"value": "PAID"}]})
        self.assertIsNone(validate_request_status("paid", cfg))

    def test_error_lists_allowed_values(self):
        cfg = merge_status_config(None)
        error = validate_request_status("foo", cfg)
        self.assertIn('"FOO"', error)
        for code in ALL_STATUSES:
            self.assertIn(code, error)


class GetTenantStatusConfigNoDbTests(SimpleTestCase):
    def test_no_tenant_returns_defaults(self):
        cfg = get_tenant_status_config(None, "unmannd_request")
        self.assertEqual(cfg["entity_type"], "unmannd_request")
        self.assertFalse(cfg["is_custom"])
        self.assertEqual(len(cfg["statuses"]), 9)

    def test_no_entity_type_skips_lookup(self):
        cfg = get_tenant_status_config(object(), "")
        self.assertFalse(cfg["is_custom"])
