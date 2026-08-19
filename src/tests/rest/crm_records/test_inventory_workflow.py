"""Tests for inventory cart status side effects."""

from django.test import SimpleTestCase

from crm_records.inventory_workflow import apply_inventory_cart_status_side_effects


class InventoryCartWorkflowTests(SimpleTestCase):
    def test_remove_from_cart_clears_cart_id(self):
        out = apply_inventory_cart_status_side_effects(
            {"status": "VENDOR_IDENTIFIED", "cart_id": "cart-1"},
            previous={"status": "IN_CART", "cart_id": "cart-1"},
        )
        self.assertIsNone(out["cart_id"])
        self.assertEqual(out["status"], "VENDOR_IDENTIFIED")

    def test_approve_into_vendor_identified_keeps_cart_id(self):
        out = apply_inventory_cart_status_side_effects(
            {"status": "VENDOR_IDENTIFIED", "cart_id": "cart-1"},
            previous={"status": "NEW_REQUEST", "cart_id": "cart-1"},
        )
        self.assertEqual(out["cart_id"], "cart-1")

    def test_add_to_cart_does_not_clear_other_fields(self):
        out = apply_inventory_cart_status_side_effects(
            {"status": "IN_CART", "item_name_freeform": "Drone"},
            previous={"status": "VENDOR_IDENTIFIED"},
        )
        self.assertEqual(out["status"], "IN_CART")
        self.assertEqual(out["item_name_freeform"], "Drone")
        self.assertNotIn("cart_id", out)
