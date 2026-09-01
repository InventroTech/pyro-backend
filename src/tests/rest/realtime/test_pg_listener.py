from unittest.mock import MagicMock, patch

from django.db.utils import InterfaceError
from django.test import TestCase

from realtime.pg_listener import _fetch_record


class FetchRecordTests(TestCase):
    @patch("crm_records.models.Record.objects.filter")
    @patch("realtime.pg_listener.close_old_connections")
    def test_retries_after_stale_connection(self, close_old_connections, record_filter):
        record = MagicMock()
        record_filter.return_value.first.side_effect = [InterfaceError("connection already closed"), record]

        result = _fetch_record(1864892)

        self.assertIs(result, record)
        self.assertEqual(record_filter.call_count, 2)
        self.assertEqual(close_old_connections.call_count, 2)
