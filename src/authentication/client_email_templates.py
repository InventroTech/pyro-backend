"""
Client-supplied password reset email bodies use a fixed placeholder; the server
injects the OTP after generation. Placeholder must match the frontend constant
in bob: `PASSWORD_RESET_OTP_PLACEHOLDER` (%%PYRO_OTP%%).
"""

from __future__ import annotations

PASSWORD_RESET_OTP_PLACEHOLDER = "%%PYRO_OTP%%"


def ensure_single_otp_placeholder(message: str, html_message: str) -> None:
    """Ensures OTP placeholder appears exactly once in plain and HTML parts."""
    if PASSWORD_RESET_OTP_PLACEHOLDER not in message:
        raise ValueError("Plain text body must include OTP placeholder")
    if PASSWORD_RESET_OTP_PLACEHOLDER not in html_message:
        raise ValueError("HTML body must include OTP placeholder")
    if message.count(PASSWORD_RESET_OTP_PLACEHOLDER) != 1:
        raise ValueError("Plain text body must include OTP placeholder exactly once")
    if html_message.count(PASSWORD_RESET_OTP_PLACEHOLDER) != 1:
        raise ValueError("HTML body must include OTP placeholder exactly once")


def apply_otp_to_client_email(
    subject: str,
    message: str,
    html_message: str,
    otp_plain: str,
) -> tuple[str, str, str]:
    ensure_single_otp_placeholder(message, html_message)

    new_subject = (
        subject.replace(PASSWORD_RESET_OTP_PLACEHOLDER, otp_plain)
        if PASSWORD_RESET_OTP_PLACEHOLDER in subject
        else subject
    )
    new_message = message.replace(PASSWORD_RESET_OTP_PLACEHOLDER, otp_plain)
    new_html = html_message.replace(PASSWORD_RESET_OTP_PLACEHOLDER, otp_plain)

    if PASSWORD_RESET_OTP_PLACEHOLDER in new_subject + new_message + new_html:
        raise ValueError("Unexpected placeholder after injection")

    return new_subject, new_message, new_html
