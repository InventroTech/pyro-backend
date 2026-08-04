import logging
import time

from django.utils import timezone

from support_ticket.services import RMAssignedMixpanelService

logger = logging.getLogger(__name__)


def run_send_rm_assigned_event(payload: dict) -> dict:
    praja_id = payload.get("praja_id")
    rm_email = payload.get("rm_email")

    if praja_id is None or not rm_email:
        raise ValueError(
            f"Invalid payload: missing praja_id or rm_email "
            f"(praja_id={praja_id}, rm_email={bool(rm_email)})"
        )

    start_time = time.time()
    praja_id_int = int(praja_id)
    service = RMAssignedMixpanelService()
    outcome = service.send_to_mixpanel_sync(praja_id_int, rm_email)
    execution_time = time.time() - start_time
    ts = timezone.now().isoformat()

    if outcome == "success":
        logger.info(
            "[SendRMAssignedEvent] sent praja_id=%s rm_email=%s in %.3fs",
            praja_id_int, rm_email, execution_time,
        )
        return {
            "success": True,
            "praja_id": praja_id_int,
            "rm_email": rm_email,
            "execution_time_seconds": round(execution_time, 3),
            "timestamp": ts,
        }

    if outcome == "skipped_not_found":
        logger.warning(
            "[SendRMAssignedEvent] skipped praja_id=%s (user not found in Praja)",
            praja_id_int,
        )
        return {
            "success": True,
            "skipped": True,
            "reason": "praja_user_not_found_404",
            "praja_id": praja_id_int,
            "rm_email": rm_email,
            "execution_time_seconds": round(execution_time, 3),
            "timestamp": ts,
        }

    raise Exception("RMAssignedMixpanelService failed")
