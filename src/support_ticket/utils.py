
import logging
from django.conf import settings
import re
from typing import Optional

import requests
logger = logging.getLogger(__name__)



def send_to_mixpanel(user_id: str, event_name: str, properties: dict) -> None:
    """
    Non-blocking Mixpanel call.
    Uses the same contract as Edge function proxy:
      - POST JSON to MIXPANEL_API_URL
      - Bearer MIXPANEL_TOKEN
    """
    token = getattr(settings, "MIXPANEL_TOKEN", None)
    url = getattr(settings, "MIXPANEL_API_URL", None)
    if not token or not url:
        logger.warning("MIXPANEL env not configured; skipping Mixpanel event")
        return

    payload = {
        "user_id": int(user_id) if str(user_id).isdigit() else user_id,
        "event_name": event_name,
        "properties": properties or {},
    }
    try:
        r = requests.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=5,
        )
        if r.status_code >= 400:
            logger.error("Mixpanel API error %s %s: %s", r.status_code, r.reason, r.text[:500])
        else:
            logger.info("Mixpanel event sent: %s", event_name)
    except Exception as e:
        logger.exception("Error sending Mixpanel event: %s", e)




