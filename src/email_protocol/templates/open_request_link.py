"""Build Open Request deep links for UNMANND email templates."""

from typing import Any


def build_open_request_url(redirect_url: Any, request_id: Any) -> str:
    """
    Turn the tenant app base URL into a deep link for a specific request.

    Emails previously used only /app/{tenant}. The frontend uses ?record_id=
    to open My Request and the matching request modal.
    """
    base = str(redirect_url or "").strip() or "#"
    if base == "#" or request_id in (None, "", "N/A"):
        return base

    joiner = "&" if "?" in base else "?"
    return f"{base}{joiner}record_id={request_id}"
