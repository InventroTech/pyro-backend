from __future__ import annotations

import logging
from typing import Any

from authentication.models import User
from authz.models import TenantMembership
from realtime.broadcast import broadcast_to_user, realtime_broadcast_skipped

from .models import InAppNotification

logger = logging.getLogger(__name__)

CALL_RECEIVED_FIELD = "wati_chatbot_call_received"
NOTIFICATION_TYPE_LEAD_CALLED_BACK = "lead_called_back"


def _is_truthy(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def was_call_received(data: dict | None) -> bool:
    if not isinstance(data, dict):
        return False
    return _is_truthy(data.get(CALL_RECEIVED_FIELD))


def should_notify_lead_called_back(old_data: dict | None, new_data: dict | None) -> bool:
    return not was_call_received(old_data) and was_call_received(new_data)


def resolve_user_pk_for_assigned_to(tenant, assigned_to) -> int | None:
    """Map lead.data.assigned_to (supabase uid, email, or membership ref) to User.pk."""
    if assigned_to in (None, "", "null", "None"):
        return None

    ref = str(assigned_to).strip()
    if not ref:
        return None

    user_pk = User.objects.filter(supabase_uid=ref).values_list("pk", flat=True).first()
    if user_pk:
        return user_pk

    user_pk = User.objects.filter(email__iexact=ref).values_list("pk", flat=True).first()
    if user_pk:
        return user_pk

    membership = None
    try:
        membership_id = int(ref)
    except (TypeError, ValueError):
        membership_id = None

    if membership_id is not None:
        membership = (
            TenantMembership.objects.filter(
                tenant=tenant,
                id=membership_id,
                is_active=True,
            )
            .only("user_id")
            .first()
        )

    if membership is None:
        membership = (
            TenantMembership.objects.filter(
                tenant=tenant,
                user_id=ref,
                is_active=True,
            )
            .only("user_id")
            .first()
        )

    if membership is None or not membership.user_id:
        return None

    return User.objects.filter(supabase_uid=str(membership.user_id)).values_list("pk", flat=True).first()


def resolve_supabase_uid_for_user_pk(user_pk: int) -> str | None:
    return User.objects.filter(pk=user_pk).values_list("supabase_uid", flat=True).first()


def build_lead_called_back_payload(record, *, notification_id: int | None = None) -> dict[str, Any]:
    data = record.data if isinstance(getattr(record, "data", None), dict) else {}
    assigned_to = data.get("assigned_to")
    payload = {
        "event": "lead_called_back",
        "record_id": str(record.id),
        "entity_type": record.entity_type,
        "lead_name": data.get("name"),
        "phone_number": data.get("phone_number"),
        "praja_id": data.get("praja_id"),
        "assigned_to": str(assigned_to) if assigned_to is not None else None,
        CALL_RECEIVED_FIELD: True,
    }
    if notification_id is not None:
        payload["notification_id"] = notification_id
    return payload


def _notification_title_message(record) -> tuple[str, str]:
    data = record.data if isinstance(getattr(record, "data", None), dict) else {}
    name = str(data.get("name") or "").strip() or "Lead"
    phone = str(data.get("phone_number") or "").strip()
    praja_id = str(data.get("praja_id") or "").strip()
    title = "WhatsApp call back"
    if phone:
        message = f"{name} called back ({phone})"
    else:
        message = f"{name} called back"
    if praja_id:
        message = f"{message} · Praja ID: {praja_id}"
    return title, message


def create_lead_called_back_notification(*, record, recipient_supabase_uid: str) -> InAppNotification | None:
    title, message = _notification_title_message(record)
    tenant_id = getattr(record, "tenant_id", None)
    try:
        return InAppNotification.objects.create(
            user_id=str(recipient_supabase_uid),
            notification_type=NOTIFICATION_TYPE_LEAD_CALLED_BACK,
            title=title,
            message=message,
            record_id=int(record.id) if record.id is not None else None,
            tenant_id=tenant_id,
        )
    except Exception:
        logger.exception(
            "Failed to persist in_app_notification for record=%s user=%s",
            getattr(record, "id", None),
            recipient_supabase_uid,
        )
        return None


def notify_lead_called_back(record) -> None:
    if realtime_broadcast_skipped():
        return

    if record.entity_type != "lead":
        return

    data = record.data if isinstance(getattr(record, "data", None), dict) else {}
    assigned_to = data.get("assigned_to")
    if assigned_to in (None, "", "null", "None"):
        logger.debug(
            "Skipping lead_called_back for record=%s — no assigned_to",
            record.id,
        )
        return

    user_pk = resolve_user_pk_for_assigned_to(record.tenant, assigned_to)
    if not user_pk:
        logger.warning(
            "Skipping lead_called_back for record=%s — could not resolve assigned_to=%s",
            record.id,
            assigned_to,
        )
        return

    supabase_uid = resolve_supabase_uid_for_user_pk(user_pk)
    if not supabase_uid:
        logger.warning(
            "Skipping lead_called_back for record=%s — user_pk=%s has no supabase_uid",
            record.id,
            user_pk,
        )
        return

    notification = create_lead_called_back_notification(
        record=record,
        recipient_supabase_uid=supabase_uid,
    )
    payload = build_lead_called_back_payload(
        record,
        notification_id=notification.id if notification else None,
    )
    logger.info(
        "Broadcasting lead_called_back record=%s user_pk=%s notification_id=%s praja_id=%s",
        record.id,
        user_pk,
        payload.get("notification_id"),
        payload.get("praja_id"),
    )
    broadcast_to_user(user_pk, payload)
