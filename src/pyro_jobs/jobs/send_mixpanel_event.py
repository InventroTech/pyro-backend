import logging
import os
import time

from django.utils import timezone

from support_ticket.services import MixpanelService

logger = logging.getLogger(__name__)


def run_send_mixpanel_event(payload: dict) -> dict:
    user_id = payload.get("user_id")
    event_name = payload.get("event_name")
    properties = payload.get("properties", {})

    if not user_id or not event_name:
        raise ValueError(
            f"Invalid payload: missing user_id or event_name "
            f"(user_id={user_id}, event_name={event_name})"
        )

    start_time = time.time()
    service = MixpanelService()
    success = service.send_to_mixpanel_sync(str(user_id), str(event_name), properties)
    execution_time = time.time() - start_time

    if not success:
        error_msg = (
            "MIXPANEL_TOKEN not configured"
            if not os.environ.get("MIXPANEL_TOKEN")
            else "Mixpanel API call returned unsuccessful response"
        )
        logger.warning(
            "[SendMixpanelEvent] event=%s user_id=%s failed: %s",
            event_name, user_id, error_msg,
        )
        raise Exception(error_msg)

    logger.info(
        "[SendMixpanelEvent] sent event=%s user_id=%s in %.3fs",
        event_name, user_id, execution_time,
    )
    return {
        "success": True,
        "event_name": event_name,
        "user_id": str(user_id),
        "execution_time_seconds": round(execution_time, 3),
        "timestamp": timezone.now().isoformat(),
    }
