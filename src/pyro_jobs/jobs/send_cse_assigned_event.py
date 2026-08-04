import logging
import time

from django.utils import timezone

from support_ticket.services import CSEAssignedMixpanelService

logger = logging.getLogger(__name__)


def run_send_cse_assigned_event(payload: dict) -> dict:
    user_id = payload.get("user_id")
    cse_email = payload.get("cse_email")

    if user_id is None or not cse_email:
        raise ValueError(
            f"Invalid payload: missing user_id or cse_email "
            f"(user_id={user_id}, cse_email={bool(cse_email)})"
        )

    start_time = time.time()
    user_id_int = int(user_id)
    service = CSEAssignedMixpanelService()
    outcome = service.send_to_mixpanel_sync(user_id_int, cse_email)
    execution_time = time.time() - start_time
    ts = timezone.now().isoformat()

    if outcome == "success":
        logger.info(
            "[SendCSEAssignedEvent] sent user_id=%s cse_email=%s in %.3fs",
            user_id_int, cse_email, execution_time,
        )
        return {
            "success": True,
            "user_id": user_id_int,
            "cse_email": cse_email,
            "execution_time_seconds": round(execution_time, 3),
            "timestamp": ts,
        }

    if outcome == "skipped_not_found":
        logger.warning(
            "[SendCSEAssignedEvent] skipped user_id=%s (user not found)",
            user_id_int,
        )
        return {
            "success": True,
            "skipped": True,
            "reason": "user_not_found_404",
            "user_id": user_id_int,
            "cse_email": cse_email,
            "execution_time_seconds": round(execution_time, 3),
            "timestamp": ts,
        }

    raise Exception("CSEAssignedMixpanelService failed")
