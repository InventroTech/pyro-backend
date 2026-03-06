"""
SHA-256 and HMAC-SHA256 utilities for cache keys, webhook signatures, and integrity checks.

Use SHA-256 for: cache key derivation, content hashing (not for password storage).
Use HMAC-SHA256 for: webhook signature verification (authenticity + integrity).
"""

import hashlib
import hmac
from typing import Union


def sha256_hex(data: Union[str, bytes], encoding: str = "utf-8") -> str:
    """
    Compute SHA-256 hash of input and return as lowercase hex string.

    Use for: cache keys derived from secrets, content fingerprints.
    Do not use for: password/secret storage (use bcrypt or Argon2).

    Args:
        data: String or bytes to hash. Strings are encoded with `encoding`.
        encoding: Encoding used when `data` is str (default utf-8).

    Returns:
        64-character lowercase hex digest.
    """
    if isinstance(data, str):
        data = data.encode(encoding)
    return hashlib.sha256(data).hexdigest()


def hmac_sha256_hex(secret: Union[str, bytes], message: Union[str, bytes], encoding: str = "utf-8") -> str:
    """
    Compute HMAC-SHA256(secret, message) and return as lowercase hex string.

    Use for: webhook signature generation or verification (compare with timing-safe compare).

    Args:
        secret: Shared secret (string or bytes).
        message: Message to sign (e.g. raw request body, or timestamp + "." + body).
        encoding: Encoding used when secret/message are str (default utf-8).

    Returns:
        64-character lowercase hex digest.
    """
    if isinstance(secret, str):
        secret = secret.encode(encoding)
    if isinstance(message, str):
        message = message.encode(encoding)
    return hmac.new(secret, message, hashlib.sha256).hexdigest()


def verify_hmac_sha256_signature(
    secret: Union[str, bytes],
    message: Union[str, bytes],
    signature_header: str,
    prefix: str = "sha256=",
    encoding: str = "utf-8",
) -> bool:
    """
    Verify that signature_header matches HMAC-SHA256(secret, message) using timing-safe comparison.

    Use for: webhook signature verification (prevents timing attacks).

    Args:
        secret: Shared secret.
        message: Raw message that was signed (e.g. request body as received).
        signature_header: Value from header (e.g. X-Webhook-Signature), often "sha256=<hex>".
        prefix: Prefix used to build expected value (e.g. "sha256="). Set to "" if header is raw hex.
        encoding: Encoding for str inputs.

    Returns:
        True if signature is valid.
    """
    expected_hex = hmac_sha256_hex(secret, message, encoding=encoding)
    expected_header = (prefix + expected_hex) if prefix else expected_hex
    received = signature_header.strip()
    return hmac.compare_digest(expected_header, received)
