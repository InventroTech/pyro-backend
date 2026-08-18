from typing import Any, Dict, Tuple

from .open_request_link import build_open_request_url


def build_request_to_verify_unmannd_email(context: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Returns (subject, text_body, html_body) when a request is sent to the
    requestor to verify details.
    """
    request_id = context.get("request_id", "N/A")
    requester_name = context.get("requester_name", "Requester")
    item_name = context.get("item_name", "N/A")
    status_text = context.get("status_text", "REQ_TO_VERIFY")
    redirect_url = context.get("redirect_url", "#")
    open_request_url = build_open_request_url(redirect_url, request_id)
    tenant_name = context.get("tenant_name", "Pyro")
    approver_name = context.get("approver_name", "Team Lead")

    subject = f"[Pyro] Please verify your request #{request_id}"

    text_body = (
        f"Hi {requester_name},\n\n"
        f"{approver_name} has asked you to verify the details of your request in {tenant_name}.\n\n"
        f"Request ID: #{request_id}\n"
        f"Item: {item_name}\n"
        f"Status: {status_text}\n\n"
        f"Review the item, quantity, vendor, and links. Update anything that is wrong, then click Verify.\n\n"
        f"Open request: {open_request_url}\n"
    )

    html_body = f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{subject}</title>
  </head>
  <body style="margin:0;padding:0;background:#f6f8fb;font-family:Inter,Segoe UI,Arial,sans-serif;color:#111827;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f6f8fb;padding:24px 12px;">
      <tr>
        <td align="center">
          <table role="presentation" width="620" cellspacing="0" cellpadding="0" style="max-width:620px;background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;overflow:hidden;">
            <tr>
              <td style="padding:18px 22px;background:linear-gradient(135deg,#5b21b6 0%,#7c3aed 100%);">
                <div style="font-size:13px;letter-spacing:.08em;text-transform:uppercase;color:#ddd6fe;">Pyro Notifications</div>
                <div style="margin-top:8px;font-size:22px;line-height:1.3;color:#ffffff;font-weight:700;">
                  Please Verify Your Request
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:22px;">
                <p style="margin:0 0 14px;font-size:15px;color:#374151;">
                  Hi {requester_name}, {approver_name} has asked you to verify the details of your request.
                </p>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:separate;border-spacing:0 10px;">
                  <tr><td style="width:170px;color:#6b7280;font-size:13px;">Request ID</td><td style="font-size:14px;font-weight:600;color:#111827;">#{request_id}</td></tr>
                  <tr><td style="color:#6b7280;font-size:13px;">Item</td><td style="font-size:14px;color:#111827;">{item_name}</td></tr>
                  <tr><td style="color:#6b7280;font-size:13px;">Status</td><td style="font-size:14px;color:#111827;">{status_text}</td></tr>
                </table>
                <p style="margin:16px 0 0;font-size:14px;color:#374151;">
                  Review the item, quantity, vendor, and links. Update anything that is wrong, then click Verify.
                </p>
                <div style="margin-top:20px;">
                  <a
                    href="{open_request_url}"
                    style="display:inline-block;background:#7c3aed;color:#ffffff;text-decoration:none;font-size:14px;font-weight:600;padding:11px 16px;border-radius:10px;"
                  >
                    Open Request
                  </a>
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:14px 22px;background:#f9fafb;border-top:1px solid #e5e7eb;color:#6b7280;font-size:12px;">
                This is an automated notification from Pyro.
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
"""
    return subject, text_body, html_body
