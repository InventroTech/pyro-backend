import time
from contextlib import contextmanager


class DRFResponseAssertionsMixin:
    """Core HTTP response assertions for DRF tests."""

    def assert_success_response(self, response):
        self.assertIn(
            response.status_code,
            [200, 201],
            f"Expected 200 or 201, got {response.status_code}: {getattr(response, 'data', response.content)}"
        )

    def assert_error_response(self, response, allowed_status=(400, 401, 403, 404, 422, 500)):
        self.assertIn(
            response.status_code,
            allowed_status,
            f"Expected error status in {allowed_status}, got {response.status_code}: {getattr(response, 'data', response.content)}"
        )

    def assert_response_keys(self, data, keys):
        for key in keys:
            self.assertIn(key, data, f"Expected key '{key}' in response {data}")

    def assert_response_list_length(self, data, expected_length):
        self.assertIsInstance(data, list, "Response should be a list")
        self.assertEqual(len(data), expected_length, f"Expected list of length {expected_length}, got {len(data)}")

    def assert_response_contains(self, response, text):
        """Assert response body (data or content) contains a substring."""
        body = str(getattr(response, "data", response.content))
        self.assertIn(text, body, f"Expected '{text}' in response body")

    def assert_response_not_contains(self, response, text):
        body = str(getattr(response, "data", response.content))
        self.assertNotIn(text, body, f"Did not expect '{text}' in response body")


class StatusCodeAssertionsMixin:
    """Granular HTTP status-code assertions that produce readable failure messages."""

    def _status_msg(self, response):
        return getattr(response, "data", response.content)

    def assert_200(self, response):
        self.assertEqual(response.status_code, 200, f"Expected 200, got {response.status_code}: {self._status_msg(response)}")

    def assert_201(self, response):
        self.assertEqual(response.status_code, 201, f"Expected 201, got {response.status_code}: {self._status_msg(response)}")

    def assert_204(self, response):
        self.assertEqual(response.status_code, 204, f"Expected 204, got {response.status_code}: {self._status_msg(response)}")

    def assert_400(self, response):
        self.assertEqual(response.status_code, 400, f"Expected 400, got {response.status_code}: {self._status_msg(response)}")

    def assert_401(self, response):
        self.assertEqual(response.status_code, 401, f"Expected 401, got {response.status_code}: {self._status_msg(response)}")

    def assert_403(self, response):
        self.assertEqual(response.status_code, 403, f"Expected 403, got {response.status_code}: {self._status_msg(response)}")

    def assert_404(self, response):
        self.assertEqual(response.status_code, 404, f"Expected 404, got {response.status_code}: {self._status_msg(response)}")

    def assert_409(self, response):
        self.assertEqual(response.status_code, 409, f"Expected 409, got {response.status_code}: {self._status_msg(response)}")

    def assert_status(self, response, expected):
        self.assertEqual(
            response.status_code, expected,
            f"Expected {expected}, got {response.status_code}: {self._status_msg(response)}"
        )


class PaginationAssertionsMixin:
    """Assertions for paginated API responses."""

    def assert_paginated_response(self, response, expected_count=None, page_key="results", count_key="count"):
        """Verify a standard DRF paginated envelope (count + results)."""
        data = response.data if hasattr(response, "data") else response
        self.assertIn(count_key, data, f"Missing '{count_key}' in paginated response")
        self.assertIn(page_key, data, f"Missing '{page_key}' in paginated response")
        self.assertIsInstance(data[page_key], list, f"'{page_key}' should be a list")
        if expected_count is not None:
            self.assertEqual(
                data[count_key], expected_count,
                f"Expected count {expected_count}, got {data[count_key]}"
            )

    def assert_cursor_paginated_response(self, response, page_key="data", meta_key="page_meta"):
        """Verify a cursor/offset paginated envelope (data + page_meta)."""
        data = response.data if hasattr(response, "data") else response
        self.assertIn(page_key, data, f"Missing '{page_key}' in paginated response")
        self.assertIn(meta_key, data, f"Missing '{meta_key}' in paginated response")
        self.assertIsInstance(data[page_key], list, f"'{page_key}' should be a list")


class TenantIsolationAssertionsMixin:
    """Helpers for asserting tenant-scoped data isolation."""

    def assert_all_belong_to_tenant(self, records, tenant_id, tenant_field="tenant_id"):
        """Assert every record in a queryset/list belongs to the expected tenant."""
        for record in records:
            actual = getattr(record, tenant_field, None) or record.get(tenant_field)
            self.assertEqual(
                str(actual), str(tenant_id),
                f"Record {record} has {tenant_field}={actual}, expected {tenant_id}"
            )

    def assert_no_cross_tenant_leak(self, response, forbidden_values, field="name"):
        """Assert that none of the forbidden values appear in a list response."""
        items = response.data if isinstance(response.data, list) else response.data.get("results", response.data.get("data", []))
        found_values = [item[field] for item in items if field in item]
        for forbidden in forbidden_values:
            self.assertNotIn(
                forbidden, found_values,
                f"Cross-tenant leak: '{forbidden}' should not appear in response"
            )


class PerformanceAssertionsMixin:
    """Timing helpers for lightweight performance guardrails in tests."""

    @contextmanager
    def assert_faster_than(self, max_seconds, label="operation"):
        """Context manager that fails if the block takes longer than max_seconds."""
        start = time.perf_counter()
        yield
        elapsed = time.perf_counter() - start
        self.assertLess(
            elapsed, max_seconds,
            f"{label} took {elapsed:.3f}s, expected < {max_seconds}s"
        )
