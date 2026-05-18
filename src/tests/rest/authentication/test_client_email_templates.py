from django.test import SimpleTestCase

from authentication.client_email_templates import (
    PASSWORD_RESET_OTP_PLACEHOLDER,
    apply_otp_to_client_email,
    ensure_single_otp_placeholder,
)


class ClientEmailTemplatesTests(SimpleTestCase):
    def test_ensure_placeholder_once(self):
        slot = PASSWORD_RESET_OTP_PLACEHOLDER
        ensure_single_otp_placeholder(f"a {slot} b", f"<span>{slot}</span>")

    def test_ensure_rejects_missing_or_duplicate(self):
        slot = PASSWORD_RESET_OTP_PLACEHOLDER
        with self.assertRaises(ValueError):
            ensure_single_otp_placeholder("no", "<p>nope</p>")
        with self.assertRaises(ValueError):
            ensure_single_otp_placeholder(f"{slot}{slot}", f"<p>{slot}</p>")

    def test_apply_injects_subject_when_present(self):
        slot = PASSWORD_RESET_OTP_PLACEHOLDER
        subj, txt, html = apply_otp_to_client_email(
            f"x {slot}",
            f"code {slot}",
            f"<b>{slot}</b>",
            "999888",
        )
        self.assertIn("999888", subj)
        self.assertEqual(txt.count("999888"), 1)
        self.assertEqual(html.count("999888"), 1)
