from typing import Any, Dict, Tuple

from .open_request_link import build_open_request_url


def build_request_ordered_unmannd_email(context: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Returns (subject, text_body, html_body) when a procurement manager orders a request.
    """
    request_id = context.get("request_id", "N/A")
    recipient_name = context.get("recipient_name", "there")
    requester_name = context.get("requester_name", "Requester")
    item_name = context.get("item_name", "N/A")
    status_text = context.get("status_text", "IN_SHIPPING")
    redirect_url = context.get("redirect_url", "#")
    open_request_url = build_open_request_url(redirect_url, request_id)
    tenant_name = context.get("tenant_name", "Pyro")
    ordered_by_name = context.get("ordered_by_name", "Procurement Manager")

    subject = f"[Pyro] Request #{request_id} ordered by procurement manager"

    text_body = (
        f"Hi {recipient_name},\n\n"
        f"A request in {tenant_name} has been ordered by {ordered_by_name}.\n\n"
        f"Request ID: #{request_id}\n"
        f"Requester: {requester_name}\n"
        f"Item: {item_name}\n"
        f"Status: {status_text}\n\n"
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
              <td style="padding:18px 22px;background:linear-gradient(135deg,#0f766e 0%,#14b8a6 100%);">
                <div style="font-size:13px;letter-spacing:.08em;text-transform:uppercase;color:#99f6e4;">Pyro Notifications</div>
                <div style="margin-top:8px;font-size:22px;line-height:1.3;color:#ffffff;font-weight:700;">
                  Request Ordered by Procurement Manager
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:22px;">
                <p style="margin:0 0 14px;font-size:15px;color:#374151;">
                  Hi {recipient_name}, a request has been ordered by {ordered_by_name}.
                </p>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:separate;border-spacing:0 10px;">
                  <tr><td style="width:170px;color:#6b7280;font-size:13px;">Request ID</td><td style="font-size:14px;font-weight:600;color:#111827;">#{request_id}</td></tr>
                  <tr><td style="color:#6b7280;font-size:13px;">Requester</td><td style="font-size:14px;color:#111827;">{requester_name}</td></tr>
                  <tr><td style="color:#6b7280;font-size:13px;">Item</td><td style="font-size:14px;color:#111827;">{item_name}</td></tr>
                  <tr><td style="color:#6b7280;font-size:13px;">Status</td><td style="font-size:14px;color:#111827;">{status_text}</td></tr>
                </table>
                <div style="margin-top:20px;">
                  <a
                    href="{open_request_url}"
                    style="display:inline-block;background:#0f766e;color:#ffffff;text-decoration:none;font-size:14px;font-weight:600;padding:11px 16px;border-radius:10px;"
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
