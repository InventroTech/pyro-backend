"""
Unit tests for core.hash_utils (SHA-256 and HMAC-SHA256).
"""

from django.test import SimpleTestCase

from core.hash_utils import hmac_sha256_hex, sha256_hex, verify_hmac_sha256_signature


class Sha256HexTests(SimpleTestCase):
    def test_sha256_hex_string_returns_64_char_hex(self):
        out = sha256_hex("hello")
        self.assertEqual(len(out), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in out))

    def test_sha256_hex_deterministic(self):
        self.assertEqual(sha256_hex("same"), sha256_hex("same"))

    def test_sha256_hex_bytes(self):
        out = sha256_hex(b"bytes")
        self.assertEqual(len(out), 64)
        self.assertEqual(out, sha256_hex("bytes"))

    def test_sha256_hex_different_input_different_output(self):
        self.assertNotEqual(sha256_hex("a"), sha256_hex("b"))


class HmacSha256HexTests(SimpleTestCase):
    def test_hmac_sha256_hex_returns_64_char_hex(self):
        out = hmac_sha256_hex("secret", "message")
        self.assertEqual(len(out), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in out))

    def test_hmac_sha256_hex_different_secret_different_output(self):
        a = hmac_sha256_hex("secret1", "msg")
        b = hmac_sha256_hex("secret2", "msg")
        self.assertNotEqual(a, b)

    def test_hmac_sha256_hex_different_message_different_output(self):
        a = hmac_sha256_hex("secret", "msg1")
        b = hmac_sha256_hex("secret", "msg2")
        self.assertNotEqual(a, b)


class VerifyHmacSha256SignatureTests(SimpleTestCase):
    def test_verify_valid_signature_with_prefix(self):
        secret = "webhook-secret"
        body = b'{"event":"test"}'
        sig = "sha256=" + hmac_sha256_hex(secret, body)
        self.assertTrue(verify_hmac_sha256_signature(secret, body, sig, prefix="sha256="))

    def test_verify_invalid_signature_returns_false(self):
        self.assertFalse(
            verify_hmac_sha256_signature("secret", b"body", "sha256=invalid", prefix="sha256=")
        )

    def test_verify_tampered_body_returns_false(self):
        secret = "s"
        body = b"original"
        sig = "sha256=" + hmac_sha256_hex(secret, body)
        self.assertFalse(
            verify_hmac_sha256_signature(secret, b"tampered", sig, prefix="sha256=")
        )

    def test_verify_no_prefix(self):
        secret = "s"
        msg = b"m"
        sig = hmac_sha256_hex(secret, msg)
        self.assertTrue(verify_hmac_sha256_signature(secret, msg, sig, prefix=""))
