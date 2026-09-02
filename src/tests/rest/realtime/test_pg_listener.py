import json
from unittest.mock import MagicMock, patch

from django.db.utils import InterfaceError, OperationalError
from django.test import TestCase

from realtime.pg_listener import REALTIME_ENTITY_TYPES, _fetch_record, _handle_payload
from tests.factories import RecordFactory


class FetchRecordTests(TestCase):
    @patch("crm_records.models.Record.objects.filter")
    @patch("realtime.pg_listener.close_old_connections")
    def test_retries_after_interface_error(self, close_old_connections, record_filter):
        record = MagicMock()
        record_filter.return_value.first.side_effect = [InterfaceError("connection already closed"), record]

        result = _fetch_record(1864892)

        self.assertIs(result, record)
        self.assertEqual(record_filter.call_count, 2)
        self.assertEqual(close_old_connections.call_count, 2)

    @patch("crm_records.models.Record.objects.filter")
    @patch("realtime.pg_listener.close_old_connections")
    def test_retries_after_operational_error(self, close_old_connections, record_filter):
        record = MagicMock()
        record_filter.return_value.first.side_effect = [
            OperationalError("SSL connection has been closed unexpectedly"),
            record,
        ]

        result = _fetch_record(1864892)

        self.assertIs(result, record)
        self.assertEqual(record_filter.call_count, 2)
        self.assertEqual(close_old_connections.call_count, 2)


class HandlePayloadTests(TestCase):
    @patch("realtime.broadcast.broadcast_record_updated")
    @patch("realtime.pg_listener._fetch_record")
    def test_handle_payload_fetches_record_and_broadcasts(self, fetch_record, broadcast):
        record = RecordFactory(entity_type="lead")
        fetch_record.return_value = record
        payload = json.dumps(
            {
                "id": record.id,
                "entity_type": "lead",
                "tenant_id": str(record.tenant_id),
            }
        )

        _handle_payload(payload)

        fetch_record.assert_called_once_with(record.id)
        broadcast.assert_called_once_with(record)

    @patch("realtime.broadcast.broadcast_record_updated")
    @patch("realtime.pg_listener._fetch_record")
    def test_handle_payload_skips_when_record_missing(self, fetch_record, broadcast):
        fetch_record.return_value = None
        entity_type = next(iter(REALTIME_ENTITY_TYPES))
        payload = json.dumps({"id": 999999, "entity_type": entity_type})

        _handle_payload(payload)

        fetch_record.assert_called_once_with(999999)
        broadcast.assert_not_called()
