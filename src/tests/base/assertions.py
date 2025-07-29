class DRFResponseAssertionsMixin:
    def assert_success_response(self, response):
        """
        Assert that the response is a successful HTTP 200 or 201.
        """
        self.assertIn(
            response.status_code,
            [200, 201],
            f"Expected 200 or 201, got {response.status_code}: {getattr(response, 'data', response.content)}"
        )

    def assert_error_response(self, response, allowed_status=(400, 401, 403, 404, 422, 500)):
        """
        Assert that the response status code is an expected error.
        """
        self.assertIn(
            response.status_code,
            allowed_status,
            f"Expected error status in {allowed_status}, got {response.status_code}: {getattr(response, 'data', response.content)}"
        )

    def assert_response_keys(self, data, keys):
        """
        Assert all required keys exist in a dictionary (for JSON objects).
        """
        for key in keys:
            self.assertIn(key, data, f"Expected key '{key}' in response {data}")

    def assert_response_list_length(self, data, expected_length):
        """
        Assert the response is a list of expected length.
        """
        self.assertIsInstance(data, list, "Response should be a list")
        self.assertEqual(len(data), expected_length, f"Expected list of length {expected_length}, got {len(data)}")
