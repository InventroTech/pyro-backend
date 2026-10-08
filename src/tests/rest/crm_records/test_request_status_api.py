"""
API / DB tests for the combined procurement + inventory request status:
status-config endpoint, ?stage= and ?status= filters, save-time sync and
validation, EntityTypeSchema.status_config validation, and the 0042 data migration.
"""
import importlib
import uuid
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.db import connection
from django.test import TestCase
from rest_framework.test import APIClient

from authz import service as authz_service
from authz.models import Role, TenantMembership
from core.models import Tenant
from crm_records.inventory_status import get_tenant_status_config
from crm_records.models import EntityTypeSchema, Record
from crm_records.serializers import EntityTypeSchemaSerializer

User = get_user_model()

STATUS_CONFIG_URL = "/crm-records/status-config/"
RECORDS_URL = "/crm-records/records/"


def _make_tenant(name="Tenant"):
    tenant = Tenant.objects.create(
        id=uuid.uuid4(), name=name, slug=f"t-{uuid.uuid4().hex[:8]}"
    )
    cache.delete(f"tenant:slug:{tenant.slug}")
    cache.delete(f"tenant:id:{tenant.id}")
    return tenant


class RequestStatusApiBase(TestCase):
    def setUp(self):
        authz_service._CACHE.clear()
        self.tenant = _make_tenant()
        self.user = User.objects.create_user(
            email=f"tl-{uuid.uuid4().hex[:6]}@example.com",
            password="pass1234",
            supabase_uid=str(uuid.uuid4()),
        )
        role = Role.objects.create(tenant=self.tenant, key="team_lead_unmannd", name="Team Lead")
        self.membership = TenantMembership.objects.create(
            tenant=self.tenant,
            user_id=self.user.supabase_uid,
            email=self.user.email,
            role=role,
            is_active=True,
            name="Team Lead User",
        )
        self.client = APIClient()
        self.client.force_login(self.user)
        self.headers = {"HTTP_X_Tenant_Slug": self.tenant.slug}
        email_patcher = patch("crm_records.views.send_email", return_value=(True, "ok"))
        email_patcher.start()
        self.addCleanup(email_patcher.stop)

    def _record(self, status, entity_type="unmannd_request", tenant=None, **extra):
        data = {"status": status, "item_name_freeform": f"Item {status}", **extra}
        return Record.objects.create(
            tenant=tenant or self.tenant, entity_type=entity_type, data=data
        )

    def _config(self, status_config, entity_type="unmannd_request", tenant=None):
        return EntityTypeSchema.objects.create(
            tenant=tenant or self.tenant,
            entity_type=entity_type,
            status_config=status_config,
        )

    def _list(self, query):
        response = self.client.get(f"{RECORDS_URL}?{query}&page_size=100", **self.headers)
        self.assertEqual(response.status_code, 200, response.data)
        return response

    def _statuses(self, query):
        return sorted(r["data"]["status"] for r in self._list(query).data["data"])

    def _patch(self, record, data):
        return self.client.patch(
            f"{RECORDS_URL}{record.id}/", {"data": data}, format="json", **self.headers
        )

    def _post(self, data, entity_type="unmannd_request"):
        return self.client.post(
            RECORDS_URL, {"entity_type": entity_type, "data": data}, format="json", **self.headers
        )


class StatusConfigEndpointTests(RequestStatusApiBase):
    def test_defaults_for_unmannd_request(self):
        response = self.client.get(f"{STATUS_CONFIG_URL}?entity_type=unmannd_request", **self.headers)
        self.assertEqual(response.status_code, 200)
        body = response.data
        self.assertEqual(body["entity_type"], "unmannd_request")
        self.assertFalse(body["is_custom"])
        self.assertEqual(len(body["statuses"]), 9)
        self.assertEqual([p["id"] for p in body["pages"]],
                         ["pending_approval", "in_cart", "ordered", "delivered", "closed"])
        first = body["statuses"][0]
        for key in ("value", "label", "color", "page", "order", "active", "builtin"):
            self.assertIn(key, first)

    def test_defaults_for_inventory_request(self):
        response = self.client.get(f"{STATUS_CONFIG_URL}?entity_type=inventory_request", **self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["entity_type"], "inventory_request")

    def test_rejects_non_request_entity_types(self):
        for query in ("?entity_type=lead", "", "?entity_type="):
            response = self.client.get(f"{STATUS_CONFIG_URL}{query}", **self.headers)
            self.assertEqual(response.status_code, 400, query)
            self.assertIn("entity_type", response.data["error"])

    def test_entity_type_is_trimmed(self):
        response = self.client.get(f"{STATUS_CONFIG_URL}?entity_type=%20unmannd_request%20", **self.headers)
        self.assertEqual(response.status_code, 200)

    def test_requires_authentication(self):
        anon = APIClient()
        response = anon.get(f"{STATUS_CONFIG_URL}?entity_type=unmannd_request", **self.headers)
        self.assertIn(response.status_code, (401, 403))

    def test_tenant_config_is_merged(self):
        self._config({
            "statuses": [
                {"value": "APPROVED", "label": "Vendor OK"},
                {"value": "PAID", "label": "Paid", "page": "closed"},
            ]
        })
        response = self.client.get(f"{STATUS_CONFIG_URL}?entity_type=unmannd_request", **self.headers)
        body = response.data
        self.assertTrue(body["is_custom"])
        by_value = {s["value"]: s for s in body["statuses"]}
        self.assertEqual(by_value["APPROVED"]["label"], "Vendor OK")
        self.assertEqual(by_value["PAID"]["page"], "closed")
        self.assertEqual(len(body["statuses"]), 10)

    def test_empty_schema_config_is_not_custom(self):
        self._config({})
        response = self.client.get(f"{STATUS_CONFIG_URL}?entity_type=unmannd_request", **self.headers)
        self.assertFalse(response.data["is_custom"])

    def test_modules_have_separate_configs(self):
        self._config({"statuses": [{"value": "APPROVED", "label": "Procurement OK"}]})
        response = self.client.get(f"{STATUS_CONFIG_URL}?entity_type=inventory_request", **self.headers)
        by_value = {s["value"]: s for s in response.data["statuses"]}
        self.assertEqual(by_value["APPROVED"]["label"], "Approved")
        self.assertFalse(response.data["is_custom"])

    def test_other_tenants_config_not_used(self):
        other = _make_tenant("Other")
        self._config({"statuses": [{"value": "APPROVED", "label": "Other tenant"}]}, tenant=other)
        response = self.client.get(f"{STATUS_CONFIG_URL}?entity_type=unmannd_request", **self.headers)
        by_value = {s["value"]: s for s in response.data["statuses"]}
        self.assertEqual(by_value["APPROVED"]["label"], "Approved")

    def test_get_tenant_status_config_reads_schema(self):
        self._config({"pages": [{"id": "only", "label": "Only"}]})
        cfg = get_tenant_status_config(self.tenant, "unmannd_request")
        self.assertTrue(cfg["is_custom"])
        self.assertEqual([p["id"] for p in cfg["pages"]], ["only"])


class StageFilterTests(RequestStatusApiBase):
    def setUp(self):
        super().setUp()
        for status in ["NEW_REQUEST", "REQ_TO_VERIFY", "APPROVED", "VENDOR_IDENTIFIED", "ON_HOLD",
                       "IN_CART", "ORDERED", "IN_SHIPPING", "EXCEPTION", "DELIVERED", "REJECTED"]:
            self._record(status)

    def test_pending_approval(self):
        self.assertEqual(
            self._statuses("entity_type=unmannd_request&stage=pending_approval"),
            sorted(["NEW_REQUEST", "REQ_TO_VERIFY", "APPROVED", "VENDOR_IDENTIFIED", "ON_HOLD"]),
        )

    def test_in_cart(self):
        self.assertEqual(self._statuses("entity_type=unmannd_request&stage=in_cart"), ["IN_CART"])

    def test_ordered_includes_exception_and_legacy(self):
        self.assertEqual(
            self._statuses("entity_type=unmannd_request&stage=ordered"),
            sorted(["ORDERED", "IN_SHIPPING", "EXCEPTION"]),
        )

    def test_delivered(self):
        self.assertEqual(self._statuses("entity_type=unmannd_request&stage=delivered"), ["DELIVERED"])

    def test_closed(self):
        self.assertEqual(self._statuses("entity_type=unmannd_request&stage=closed"), ["REJECTED"])

    def test_multiple_stages(self):
        self.assertEqual(
            self._statuses("entity_type=unmannd_request&stage=in_cart,delivered"),
            ["DELIVERED", "IN_CART"],
        )

    def test_unknown_stage_returns_nothing(self):
        self.assertEqual(self._statuses("entity_type=unmannd_request&stage=nope"), [])

    def test_empty_stage_is_ignored(self):
        self.assertEqual(len(self._statuses("entity_type=unmannd_request&stage=")), 11)

    def test_stage_scoped_to_entity_type(self):
        self._record("IN_CART", entity_type="inventory_request")
        self.assertEqual(self._statuses("entity_type=inventory_request&stage=in_cart"), ["IN_CART"])
        self.assertEqual(self._statuses("entity_type=inventory_request&stage=ordered"), [])

    def test_stage_uses_tenant_config(self):
        self._config({"statuses": [{"value": "APPROVED", "page": "in_cart"}]})
        self.assertEqual(
            self._statuses("entity_type=unmannd_request&stage=in_cart"),
            sorted(["APPROVED", "VENDOR_IDENTIFIED", "IN_CART"]),
        )
        self.assertNotIn("APPROVED", self._statuses("entity_type=unmannd_request&stage=pending_approval"))

    def test_stage_with_custom_page(self):
        self._record("PAID")
        self._config({
            "pages": [{"id": "pending_approval", "label": "P"}, {"id": "paid", "label": "Paid"}],
            "statuses": [{"value": "PAID", "page": "paid"}],
        })
        self.assertEqual(self._statuses("entity_type=unmannd_request&stage=paid"), ["PAID"])

    def test_stage_count_matches(self):
        response = self._list("entity_type=unmannd_request&stage=ordered&include_count=true")
        self.assertEqual(response.data["page_meta"]["total_count"], 3)

    def test_other_tenant_rows_excluded(self):
        other = _make_tenant("Other")
        self._record("IN_CART", tenant=other)
        self.assertEqual(self._statuses("entity_type=unmannd_request&stage=in_cart"), ["IN_CART"])


class StatusFilterLegacyExpansionTests(RequestStatusApiBase):
    def setUp(self):
        super().setUp()
        for status in ["APPROVED", "VENDOR_IDENTIFIED", "ORDERED", "IN_SHIPPING", "IN_CART"]:
            self._record(status)

    def test_approved_matches_legacy(self):
        self.assertEqual(
            self._statuses("entity_type=unmannd_request&status=APPROVED"),
            ["APPROVED", "VENDOR_IDENTIFIED"],
        )

    def test_ordered_matches_legacy(self):
        self.assertEqual(
            self._statuses("entity_type=unmannd_request&status=ORDERED"),
            ["IN_SHIPPING", "ORDERED"],
        )

    def test_legacy_value_matches_new(self):
        self.assertEqual(
            self._statuses("entity_type=unmannd_request&status=VENDOR_IDENTIFIED"),
            ["APPROVED", "VENDOR_IDENTIFIED"],
        )

    def test_comma_list(self):
        self.assertEqual(
            self._statuses("entity_type=unmannd_request&status=IN_CART,ORDERED"),
            ["IN_CART", "IN_SHIPPING", "ORDERED"],
        )

    def test_status_without_legacy_unchanged(self):
        self.assertEqual(self._statuses("entity_type=unmannd_request&status=IN_CART"), ["IN_CART"])

    def test_non_request_entity_not_expanded(self):
        self._record("VENDOR_IDENTIFIED", entity_type="lead")
        self._record("APPROVED", entity_type="lead")
        self.assertEqual(self._statuses("entity_type=lead&status=APPROVED"), ["APPROVED"])


class SaveSyncTests(RequestStatusApiBase):
    def test_post_legacy_status_saved_as_new(self):
        response = self._post({"status": "VENDOR_IDENTIFIED", "item_name_freeform": "Drone"})
        self.assertEqual(response.status_code, 201, response.data)
        record = Record.objects.get(id=response.data["id"])
        self.assertEqual(record.data["status"], "APPROVED")
        self.assertEqual(record.data["status_text"], "Approved")

    def test_post_new_request_gets_label(self):
        response = self._post({"status": "NEW_REQUEST", "item_name_freeform": "Drone"},
                              entity_type="inventory_request")
        self.assertEqual(response.status_code, 201, response.data)
        record = Record.objects.get(id=response.data["id"])
        self.assertEqual(record.data["status_text"], "New request")

    def test_post_lowercase_status_normalized(self):
        response = self._post({"status": "in cart", "item_name_freeform": "Drone"})
        self.assertEqual(response.status_code, 201, response.data)
        self.assertEqual(Record.objects.get(id=response.data["id"]).data["status"], "IN_CART")

    def test_order_sets_shipment_ordered(self):
        record = self._record("IN_CART", status_text="In cart")
        response = self._patch(record, {**record.data, "status": "ORDERED"})
        self.assertEqual(response.status_code, 200, response.data)
        record.refresh_from_db()
        self.assertEqual(record.data["status"], "ORDERED")
        self.assertEqual(record.data["status_text"], "Ordered")
        self.assertEqual(record.data["shipment_status"], "ORDERED")

    def test_manual_delivered_updates_shipment(self):
        record = self._record("ORDERED", shipment_status="IN_TRANSIT")
        response = self._patch(record, {**record.data, "status": "DELIVERED"})
        self.assertEqual(response.status_code, 200, response.data)
        record.refresh_from_db()
        self.assertEqual(record.data["shipment_status"], "DELIVERED")

    def test_shipment_delivered_moves_status(self):
        record = self._record("ORDERED", shipment_status="IN_TRANSIT", status_text="Ordered")
        response = self._patch(record, {**record.data, "shipment_status": "DELIVERED"})
        self.assertEqual(response.status_code, 200, response.data)
        record.refresh_from_db()
        self.assertEqual(record.data["status"], "DELIVERED")
        self.assertEqual(record.data["status_text"], "Delivered")

    def test_legacy_row_normalized_on_unrelated_edit(self):
        record = self._record("IN_SHIPPING", status_text="IN_SHIPPING", shipment_status="IN_TRANSIT")
        response = self._patch(record, {**record.data, "comments": "update"})
        self.assertEqual(response.status_code, 200, response.data)
        record.refresh_from_db()
        self.assertEqual(record.data["status"], "ORDERED")
        self.assertEqual(record.data["status_text"], "Ordered")

    def test_tenant_label_used_for_status_text(self):
        self._config({"statuses": [{"value": "APPROVED", "label": "Vendor OK"}]})
        record = self._record("NEW_REQUEST")
        response = self._patch(record, {**record.data, "status": "APPROVED"})
        self.assertEqual(response.status_code, 200, response.data)
        record.refresh_from_db()
        self.assertEqual(record.data["status_text"], "Vendor OK")

    def test_custom_status_text_kept(self):
        record = self._record("NEW_REQUEST")
        response = self._patch(
            record, {**record.data, "status": "APPROVED", "status_text": "Approved by finance"}
        )
        self.assertEqual(response.status_code, 200, response.data)
        record.refresh_from_db()
        self.assertEqual(record.data["status_text"], "Approved by finance")

    def test_non_request_entity_not_touched(self):
        response = self._post({"status": "VENDOR_IDENTIFIED", "name": "Lead"}, entity_type="lead")
        self.assertEqual(response.status_code, 201, response.data)
        record = Record.objects.get(id=response.data["id"])
        self.assertEqual(record.data["status"], "VENDOR_IDENTIFIED")
        self.assertNotIn("status_text", record.data)


class SaveValidationTests(RequestStatusApiBase):
    def test_unknown_status_allowed_without_custom_config(self):
        record = self._record("DELIVERED")
        response = self._patch(record, {**record.data, "status": "PAID"})
        self.assertEqual(response.status_code, 200, response.data)
        record.refresh_from_db()
        self.assertEqual(record.data["status"], "PAID")

    def test_unknown_status_rejected_with_custom_config(self):
        self._config({"statuses": [{"value": "PAID", "page": "closed"}]})
        record = self._record("DELIVERED")
        response = self._patch(record, {**record.data, "status": "FOO"})
        self.assertEqual(response.status_code, 400)
        self.assertIn("Unknown status", str(response.data))
        record.refresh_from_db()
        self.assertEqual(record.data["status"], "DELIVERED")

    def test_configured_custom_status_allowed(self):
        self._config({"statuses": [{"value": "PAID", "page": "closed"}]})
        record = self._record("DELIVERED")
        response = self._patch(record, {**record.data, "status": "paid"})
        self.assertEqual(response.status_code, 200, response.data)
        record.refresh_from_db()
        self.assertEqual(record.data["status"], "PAID")

    def test_legacy_status_allowed_with_custom_config(self):
        self._config({"statuses": [{"value": "PAID"}]})
        record = self._record("NEW_REQUEST")
        response = self._patch(record, {**record.data, "status": "VENDOR_IDENTIFIED"})
        self.assertEqual(response.status_code, 200, response.data)

    def test_hidden_builtin_still_saves(self):
        self._config({"statuses": [{"value": "ON_HOLD", "active": False}]})
        record = self._record("NEW_REQUEST")
        response = self._patch(record, {**record.data, "status": "ON_HOLD"})
        self.assertEqual(response.status_code, 200, response.data)

    def test_validation_uses_record_entity_type_config(self):
        self._config({"statuses": [{"value": "PAID"}]}, entity_type="inventory_request")
        record = self._record("DELIVERED", entity_type="unmannd_request")
        response = self._patch(record, {**record.data, "status": "FOO"})
        self.assertEqual(response.status_code, 200, response.data)


class EntityTypeSchemaStatusConfigValidationTests(TestCase):
    def _errors(self, status_config):
        serializer = EntityTypeSchemaSerializer(
            data={"entity_type": "unmannd_request", "status_config": status_config}
        )
        serializer.is_valid()
        return serializer.errors.get("status_config")

    def test_valid_config(self):
        self.assertIsNone(self._errors({
            "pages": [{"id": "p", "label": "P"}],
            "statuses": [{"value": "PAID", "page": "p"}],
        }))

    def test_empty_values_become_empty_dict(self):
        serializer = EntityTypeSchemaSerializer(
            data={"entity_type": "unmannd_request", "status_config": {}}
        )
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.validated_data["status_config"], {})

    def test_must_be_object(self):
        self.assertIsNotNone(self._errors(["x"]))

    def test_lists_must_hold_objects(self):
        self.assertIsNotNone(self._errors({"pages": "x"}))
        self.assertIsNotNone(self._errors({"statuses": ["PAID"]}))

    def test_page_needs_id(self):
        self.assertIsNotNone(self._errors({"pages": [{"label": "No id"}]}))

    def test_status_needs_value(self):
        self.assertIsNotNone(self._errors({"statuses": [{"label": "No value"}]}))

    def test_status_page_must_exist_when_pages_given(self):
        errors = self._errors({
            "pages": [{"id": "p"}],
            "statuses": [{"value": "PAID", "page": "missing"}],
        })
        self.assertIn("unknown page", str(errors))

    def test_status_page_not_checked_without_pages(self):
        self.assertIsNone(self._errors({"statuses": [{"value": "PAID", "page": "closed"}]}))


class CombineRequestStatusMigrationTests(TestCase):
    def setUp(self):
        self.tenant = _make_tenant()
        self.migration = importlib.import_module(
            "crm_records.migrations.0042_combine_request_status"
        )

    def _rec(self, data, entity_type="unmannd_request"):
        return Record.objects.create(tenant=self.tenant, entity_type=entity_type, data=data)

    def _run(self, sql):
        with connection.cursor() as cursor:
            cursor.execute(sql)

    def _data(self, record):
        record.refresh_from_db()
        return record.data

    def test_forward_mapping(self):
        vi = self._rec({"status": "VENDOR_IDENTIFIED", "status_text": "VENDOR_IDENTIFIED"})
        delivered = self._rec({"status": "IN_SHIPPING", "shipment_status": "DELIVERED"})
        exception = self._rec({"status": "IN_SHIPPING", "shipment_status": "exception"})
        transit = self._rec({"status": "IN_SHIPPING", "shipment_status": "IN_TRANSIT"})
        no_shipment = self._rec({"status": "IN_SHIPPING"}, entity_type="inventory_request")

        self._run(self.migration.FORWARD_SQL)

        self.assertEqual(self._data(vi)["status"], "APPROVED")
        self.assertEqual(self._data(vi)["status_text"], "Approved")
        self.assertEqual(self._data(delivered)["status"], "DELIVERED")
        self.assertEqual(self._data(delivered)["status_text"], "Delivered")
        self.assertEqual(self._data(exception)["status"], "EXCEPTION")
        self.assertEqual(self._data(transit)["status"], "ORDERED")
        self.assertEqual(self._data(transit)["status_text"], "Ordered")
        self.assertEqual(self._data(no_shipment)["status"], "ORDERED")

    def test_forward_handles_case_and_spaces(self):
        rec = self._rec({"status": " vendor_identified ", "status_text": "in shipping"})
        self._run(self.migration.FORWARD_SQL)
        self.assertEqual(self._data(rec)["status"], "APPROVED")
        self.assertEqual(self._data(rec)["status_text"], "Approved")

    def test_forward_keeps_custom_status_text(self):
        rec = self._rec({"status": "VENDOR_IDENTIFIED", "status_text": "Quote received"})
        self._run(self.migration.FORWARD_SQL)
        self.assertEqual(self._data(rec)["status"], "APPROVED")
        self.assertEqual(self._data(rec)["status_text"], "Quote received")

    def test_forward_keeps_other_fields(self):
        rec = self._rec({"status": "IN_SHIPPING", "tracking_number": "AWB1", "cart_id": "c1"})
        self._run(self.migration.FORWARD_SQL)
        data = self._data(rec)
        self.assertEqual(data["tracking_number"], "AWB1")
        self.assertEqual(data["cart_id"], "c1")

    def test_forward_ignores_other_entities_and_statuses(self):
        lead = self._rec({"status": "VENDOR_IDENTIFIED"}, entity_type="lead")
        in_cart = self._rec({"status": "IN_CART", "status_text": "IN_CART"})
        self._run(self.migration.FORWARD_SQL)
        self.assertEqual(self._data(lead)["status"], "VENDOR_IDENTIFIED")
        self.assertEqual(self._data(in_cart), {"status": "IN_CART", "status_text": "IN_CART"})

    def test_forward_is_idempotent(self):
        rec = self._rec({"status": "IN_SHIPPING", "shipment_status": "DELIVERED"})
        self._run(self.migration.FORWARD_SQL)
        first = self._data(rec)
        self._run(self.migration.FORWARD_SQL)
        self.assertEqual(self._data(rec), first)

    def test_reverse_mapping(self):
        approved = self._rec({"status": "APPROVED", "status_text": "Approved"})
        delivered = self._rec({"status": "DELIVERED", "status_text": "Delivered"})
        custom = self._rec({"status": "ORDERED", "status_text": "Shipped by FedEx"})
        new_request = self._rec({"status": "NEW_REQUEST", "status_text": "New request"})

        self._run(self.migration.REVERSE_SQL)

        self.assertEqual(self._data(approved)["status"], "VENDOR_IDENTIFIED")
        self.assertEqual(self._data(approved)["status_text"], "VENDOR_IDENTIFIED")
        self.assertEqual(self._data(delivered)["status"], "IN_SHIPPING")
        self.assertEqual(self._data(custom)["status"], "IN_SHIPPING")
        self.assertEqual(self._data(custom)["status_text"], "Shipped by FedEx")
        self.assertEqual(self._data(new_request)["status"], "NEW_REQUEST")

    def test_round_trip_for_approved(self):
        rec = self._rec({"status": "VENDOR_IDENTIFIED", "status_text": "VENDOR_IDENTIFIED"})
        self._run(self.migration.FORWARD_SQL)
        self._run(self.migration.REVERSE_SQL)
        self.assertEqual(self._data(rec)["status"], "VENDOR_IDENTIFIED")
