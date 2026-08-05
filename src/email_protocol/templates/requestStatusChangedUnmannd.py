from typing import Any, Dict, Tuple


def build_request_status_changed_unmannd_email(context: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Returns (subject, text_body, html_body) when a request status changes.
    Used for any transition that does not have a dedicated email template.
    """
    request_id = context.get("request_id", "N/A")
    requester_name = context.get("requester_name", "Requester")
    item_name = context.get("item_name", "N/A")
    previous_status = context.get("previous_status", "N/A") or "N/A"
    current_status = context.get("current_status") or context.get("status_text") or "UPDATED"
    status_text = context.get("status_text") or current_status
    redirect_url = context.get("redirect_url", "#")
    tenant_name = context.get("tenant_name", "Pyro")

    subject = f"[Pyro] Request #{request_id} status updated to {current_status}"

    text_body = (
        f"Hi {requester_name},\n\n"
        f"Your request in {tenant_name} has a new status.\n\n"
        f"Request ID: #{request_id}\n"
        f"Item: {item_name}\n"
        f"Previous status: {previous_status}\n"
        f"New status: {status_text}\n\n"
        f"Open request: {redirect_url}\n"
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
              <td style="padding:18px 22px;background:linear-gradient(135deg,#1e3a8a 0%,#2563eb 100%);">
                <div style="font-size:13px;letter-spacing:.08em;text-transform:uppercase;color:#bfdbfe;">Pyro Notifications</div>
                <div style="margin-top:8px;font-size:22px;line-height:1.3;color:#ffffff;font-weight:700;">
                  Request Status Updated
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:22px;">
                <p style="margin:0 0 14px;font-size:15px;color:#374151;">
                  Hi {requester_name}, your request status has changed.
                </p>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:separate;border-spacing:0 10px;">
                  <tr><td style="width:170px;color:#6b7280;font-size:13px;">Request ID</td><td style="font-size:14px;font-weight:600;color:#111827;">#{request_id}</td></tr>
                  <tr><td style="color:#6b7280;font-size:13px;">Item</td><td style="font-size:14px;color:#111827;">{item_name}</td></tr>
                  <tr><td style="color:#6b7280;font-size:13px;">Previous status</td><td style="font-size:14px;color:#111827;">{previous_status}</td></tr>
                  <tr><td style="color:#6b7280;font-size:13px;">New status</td><td style="font-size:14px;color:#111827;">{status_text}</td></tr>
                </table>
                <div style="margin-top:20px;">
                  <a
                    href="{redirect_url}"
                    style="display:inline-block;background:#2563eb;color:#ffffff;text-decoration:none;font-size:14px;font-weight:600;padding:11px 16px;border-radius:10px;"
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
