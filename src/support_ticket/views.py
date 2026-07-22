import logging
import json
import base64
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from uuid import UUID
from rest_framework.permissions import AllowAny
from django.utils import timezone
from django.db import connection, models, transaction
from django.db.models import Q, QuerySet
from django.db import IntegrityError
from django.utils.dateparse import parse_datetime
from django.conf import settings
from config.supabase_auth import SupabaseJWTAuthentication
from authz.permissions import IsTenantAuthenticated
import os
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from .models import SupportTicketDump
from .serializers import SaveAndContinueSerializer, GetNextTicketResponseSerializer, SupportTicketUpdateSerializer, TakeBreakSerializer,UpdateCallStatusRequestSerializer
from .services import MixpanelService, TicketTimeService
from background_jobs.queue_service import get_queue_service
from background_jobs.models import BackgroundJob, JobStatus, JobType
from core.models import Tenant
from crm_records.models import Record
from crm_records.permissions import HasAPISecret
from crm_records.serializers import RecordSerializer
from user_settings.models import Group, TenantMemberSetting
from user_settings.services import (
    USER_KV_GROUP_ID_KEY,
    USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY,
    coerce_kv_int,
)
from authz.permissions import IsTenantAuthenticated
from authz.models import TenantMembership
from accounts.models import SupabaseAuthUser
from datetime import date, datetime, timezone as dt_timezone
from .records import (
    apply_record_data_updates,
    q_record_open_or_snoozed_resolution,
    q_record_pending_resolution,
    record_to_ticket_dict,
    records_to_ticket_dicts,
    support_ticket_records_qs,
)
from .constants import (
    SAVE_AND_CONTINUE_RESOLUTION_EVENTS,
    SUPPORT_DEFAULT_RESOLVE_RATE_GOAL_PERCENT,
    SUPPORT_EVENT_NOT_CONNECTED,
    SUPPORT_EVENT_TAKE_BREAK,
    SUPPORT_RESOLUTION_STATUS_OPEN,
    SUPPORT_RESOLVE_RATE_SUCCESS_STATUSES,
    SUPPORT_TERMINAL_RESOLUTION_STATUSES,
    SUPPORT_TICKET_ENTITY_TYPE,
)
from .ticket_types import (
    SELF_TRIAL_TICKET_TYPE_KEY as _SELF_TRIAL_TICKET_TYPE_KEY,
    canonical_support_ticket_type_key as _canonical_support_ticket_type_key,
    q_record_self_trial,
)
from .events import log_and_dispatch_support_ticket_event, resolve_support_ticket_record
from .mixpanel_properties import support_ticket_mixpanel_properties
from .buckets import seed_cse_support_buckets
from .pipeline import SupportTicketPipeline

logger = logging.getLogger(__name__)
DUMP_BATCH_LIMIT = 5000
# Per-tenant advisory lock so only one process_dumped_tickets job is enqueued at a time.
_PROCESS_DUMPED_TICKETS_LOCK_BASE = 874216000
_INCOMPLETE_DUMP_JOB_STATUSES = (
    JobStatus.PENDING,
    JobStatus.PROCESSING,
    JobStatus.RETRYING,
)


def _process_dumped_tickets_lock_key(tenant_id: Union[str, UUID]) -> int:
    uid = UUID(str(tenant_id))
    return _PROCESS_DUMPED_TICKETS_LOCK_BASE + (uid.int % 1_000_000)

_DUMP_RESERVED_KEYS = frozenset({
    "tenant_id",
    "is_processed",
    "is_deleted",
    "deleted_at",
    "id",
    "created_at",
    "data",
})
_RECORD_DATA_SKIP_FROM_DUMP = frozenset({
    "id",
    "tenant",
    "tenant_id",
    "assigned_to",
    "is_deleted",
    "deleted_at",
    "created_at",
    "is_processed",
    "data",
})
_RECORD_DATETIME_FIELDS = frozenset({
    "created_at",
    "ticket_date",
    "completed_at",
    "snooze_until",
    "dumped_at",
})


def _parse_dump_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if timezone.is_aware(value) else timezone.make_aware(value)
    if isinstance(value, date):
        return timezone.make_aware(datetime.combine(value, datetime.min.time()))
    if isinstance(value, str):
        parsed = parse_datetime(value)
        if parsed is None:
            return None
        return parsed if timezone.is_aware(parsed) else timezone.make_aware(parsed)
    return None


def _serialize_dump_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        elif isinstance(value, date):
            out[key] = datetime.combine(value, datetime.min.time()).isoformat()
        else:
            out[key] = value
    return out


def _extract_dump_payload(payload: Mapping[str, Any]) -> Dict[str, Any]:
    cleaned = {
        key: value
        for key, value in payload.items()
        if key not in _DUMP_RESERVED_KEYS and value is not None
    }
    return _serialize_dump_payload(cleaned)


def _coerce_webhook_api_id(value: Any) -> Any:
    if value is None:
        return None
    return int(value) if str(value).isdigit() else value


def _support_ticket_id_from_dump_data(data: Mapping[str, Any]) -> Any:
    return data.get("support_ticket_id") or data.get("ticket_id")


def _resolve_support_ticket_record_id(
    *,
    tenant_id: Any,
    dump_payload: Mapping[str, Any],
) -> Optional[int]:
    """Find CRM record id after dump ingest for webhook response."""
    base_qs = Record.objects.filter(
        tenant_id=tenant_id,
        entity_type=SUPPORT_TICKET_ENTITY_TYPE,
    )
    ticket_id = _support_ticket_id_from_dump_data(dump_payload)
    if ticket_id is not None:
        ticket_id_str = str(ticket_id)
        record = (
            base_qs.filter(data__support_ticket_id=ticket_id_str).first()
            or base_qs.filter(data__support_ticket_id=ticket_id).first()
            or base_qs.filter(data__ticket_id=ticket_id_str).first()
            or base_qs.filter(data__ticket_id=ticket_id).first()
        )
        if record:
            return record.id

    normalized_user_id = _normalize_dump_user_id(dump_payload.get("user_id"))
    if normalized_user_id:
        record = base_qs.filter(data__user_id=normalized_user_id).order_by("-id").first()
        if record:
            return record.id
    return None


def _enqueue_mixpanel_event(
    *,
    user_id: Any,
    event_name: str,
    properties: Dict[str, Any],
    tenant_id: Any = None,
) -> None:
    if not user_id:
        logger.warning("Skipping Mixpanel enqueue for event=%s due to missing user_id", event_name)
        return
    try:
        queue_service = get_queue_service()
        queue_service.enqueue_job(
            job_type=JobType.SEND_MIXPANEL_EVENT,
            payload={
                "user_id": str(user_id),
                "event_name": event_name,
                "properties": properties or {},
            },
            tenant_id=str(tenant_id) if tenant_id else None,
            priority=0,
            max_attempts=3,
        )
    except Exception as e:
        logger.error("Failed to enqueue Mixpanel event=%s user_id=%s error=%s", event_name, user_id, e, exc_info=True)


def _enqueue_cse_assigned_event(
    *,
    user_id: Any,
    cse_email: str,
    tenant_id: Any = None,
) -> None:
    if not user_id or not cse_email:
        logger.warning(
            "Skipping cse_assigned enqueue due to missing user_id or cse_email "
            "(user_id=%s, cse_email=%s)",
            user_id,
            bool(cse_email),
        )
        return
    try:
        user_id_int = int(user_id)
    except (ValueError, TypeError):
        logger.error(
            "[GetNextTicket] Could not enqueue cse_assigned event - user_id=%s is not numeric",
            user_id,
        )
        return
    try:
        queue_service = get_queue_service()
        queue_service.enqueue_job(
            job_type=JobType.SEND_CSE_ASSIGNED_EVENT,
            payload={"user_id": user_id_int, "cse_email": cse_email},
            tenant_id=str(tenant_id) if tenant_id else None,
            priority=0,
            max_attempts=3,
        )
    except Exception as e:
        logger.error(
            "Failed to enqueue cse_assigned event user_id=%s cse_email=%s error=%s",
            user_id,
            cse_email,
            e,
            exc_info=True,
        )


def _normalize_dump_user_id(user_id: Any) -> Optional[str]:
    if user_id is None:
        return None
    normalized = str(user_id).strip()
    return normalized or None


def _dedupe_dumps_latest_wins(
    dumped_tickets: Sequence[SupportTicketDump],
) -> List[SupportTicketDump]:
    """One row per user_id; SELF TRIAL wins over other types, else latest row wins."""
    by_user: Dict[str, List[SupportTicketDump]] = {}
    user_order: List[str] = []
    for dump in dumped_tickets:
        user_id = _normalize_dump_user_id((dump.data or {}).get("user_id"))
        if not user_id:
            continue
        if user_id not in by_user:
            by_user[user_id] = []
            user_order.append(user_id)
        by_user[user_id].append(dump)

    unique: List[SupportTicketDump] = []
    for user_id in user_order:
        dumps = by_user[user_id]
        self_trial_dumps = [candidate for candidate in dumps if _dump_is_self_trial(candidate)]
        unique.append(self_trial_dumps[-1] if self_trial_dumps else dumps[-1])
    return unique


def _serialize_record_extra_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).isoformat()
    return value


def _build_support_record_data_from_dump(
    dump_ticket: SupportTicketDump,
) -> Dict[str, Any]:
    now = timezone.now()
    data: Dict[str, Any] = {
        "tenant_id": str(dump_ticket.tenant_id) if dump_ticket.tenant_id else None,
        "dumped_at": now.isoformat(),
        "call_status": "Call Waiting",
        "call_attempts": 0,
        "other_reasons": [],
        "review_requested": False,
    }
    raw = dump_ticket.data or {}
    for key, value in raw.items():
        if key in _RECORD_DATA_SKIP_FROM_DUMP or value is None:
            continue
        if key in _RECORD_DATETIME_FIELDS:
            parsed = _parse_dump_datetime(value)
            data[key] = parsed.isoformat() if parsed else None
        elif key == "user_id":
            data[key] = _normalize_dump_user_id(value)
        elif key == "other_reasons":
            data[key] = list(value) if value is not None else []
        elif key == "review_requested":
            data[key] = bool(value)
        elif key == "call_attempts":
            try:
                data[key] = int(value) if value is not None else 0
            except (TypeError, ValueError):
                data[key] = 0
        else:
            data[key] = _serialize_record_extra_value(value)

    data.setdefault("call_status", "Call Waiting")
    data.setdefault("call_attempts", 0)
    data.setdefault("other_reasons", [])
    data.setdefault("review_requested", False)
    # Align with Praja open-ticket sync: new dump→record rows are Open unless dump set one.
    data.setdefault("resolution_status", SUPPORT_RESOLUTION_STATUS_OPEN)

    snooze_until = data.get("snooze_until")
    if snooze_until:
        data["next_call_at"] = snooze_until
    return data


def _delete_open_support_records_for_user(
    *,
    user_id: Any,
    tenant_id: Optional[Any] = None,
) -> int:
    normalized = _normalize_dump_user_id(user_id)
    if not normalized:
        return 0
    qs = Record.objects.filter(
        entity_type=SUPPORT_TICKET_ENTITY_TYPE,
        data__user_id=normalized,
    ).filter(q_record_pending_resolution())
    if tenant_id:
        qs = qs.filter(tenant_id=tenant_id)
    count, _ = qs.delete()
    return count


def enqueue_ticket_created_mixpanel(
    record: Record,
    dump_data: Optional[Mapping[str, Any]] = None,
) -> None:
    record_data = record.data or {}
    user_id = record_data.get("user_id") or str(record.id)
    properties = support_ticket_mixpanel_properties(record)
    get_queue_service().enqueue_job(
        job_type=JobType.SEND_MIXPANEL_EVENT,
        payload={
            "user_id": str(user_id),
            "event_name": "pyro_st_ticket_created",
            "properties": properties,
        },
        tenant_id=str(record.tenant_id) if record.tenant_id else None,
        priority=0,
    )


def enqueue_ticket_created_praja(
    record: Record,
    dump_data: Optional[Mapping[str, Any]] = None,
) -> None:
    from support_ticket.events import enqueue_praja_for_open_ticket

    enqueue_praja_for_open_ticket(record, dump_data=dump_data)


def on_ticket_created_after_dump(
    record: Record,
    dump_data: Optional[Mapping[str, Any]] = None,
) -> None:
    """Side effects after a dumped ticket is inserted as a ``records`` row."""
    enqueue_ticket_created_mixpanel(record, dump_data)
    enqueue_ticket_created_praja(record, dump_data)


@transaction.atomic
def enqueue_process_dumped_tickets_job(
    tenant_id: Union[str, UUID],
    *,
    priority: int = 0,
) -> Optional[BackgroundJob]:
    tid = str(tenant_id)
    if connection.vendor == "postgresql":
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                [_process_dumped_tickets_lock_key(tid)],
            )

    if BackgroundJob.objects.filter(
        job_type=JobType.PROCESS_DUMPED_TICKETS,
        tenant_id=tid,
        status__in=_INCOMPLETE_DUMP_JOB_STATUSES,
    ).exists():
        logger.info(
            "enqueue_process_dumped_tickets_job: skipped — incomplete job exists for tenant=%s",
            tid,
        )
        return None

    job = get_queue_service().enqueue_job(
        job_type=JobType.PROCESS_DUMPED_TICKETS,
        payload={},
        tenant_id=tid,
        priority=priority,
    )
    logger.info(
        "enqueue_process_dumped_tickets_job: enqueued job_id=%s tenant=%s",
        job.id,
        tid,
    )
    return job


def enqueue_process_dumped_tickets_for_pending_dumps() -> Dict[str, Any]:
    """
    Enqueue one ``process_dumped_tickets`` job per tenant with unprocessed dump rows.
    Called by the background worker on a DB-backed 5-minute scheduler tick.
    """
    tenant_ids = (
        SupportTicketDump.objects.filter(
            Q(is_processed__isnull=True) | Q(is_processed=False)
        )
        .values_list("tenant_id", flat=True)
        .distinct()
    )
    enqueued = []
    skipped = []
    for tid in tenant_ids:
        job = enqueue_process_dumped_tickets_job(tid)
        if job:
            enqueued.append({"tenant_id": str(tid), "job_id": job.id})
        else:
            skipped.append(str(tid))
    return {"enqueued": enqueued, "skipped_active_job": skipped}


@dataclass
class ProcessDumpedTicketsResult:
    total_dumped_tickets: int
    unique_tickets: int
    inserted_tickets: int
    mirrored_records: int
    skipped_tickets: int
    marked_processed: int


def process_dumped_tickets(
    *,
    tenant_id: Optional[Union[str, UUID]] = None,
    on_ticket_created: Optional[
        Callable[[Record, Optional[Mapping[str, Any]]], None]
    ] = None,
    batch_limit: int = DUMP_BATCH_LIMIT,
) -> ProcessDumpedTicketsResult:
    inserted_records: List[Record] = []
    dump_data_by_user_id: Dict[str, Dict[str, Any]] = {}

    with transaction.atomic():
        # SELECT FOR UPDATE SKIP LOCKED: concurrent callers (webhook + background job)
        # each lock a disjoint set of rows. A second caller racing on the same rows
        # skips them and returns an empty list, preventing duplicate record creation.
        dumped_qs = SupportTicketDump.objects.select_for_update(skip_locked=True).filter(
            Q(is_processed__isnull=True) | Q(is_processed=False)
        )
        if tenant_id is not None:
            dumped_qs = dumped_qs.filter(tenant_id=tenant_id)
        dumped_tickets_list = list(dumped_qs.order_by("id")[:batch_limit])

        if not dumped_tickets_list:
            logger.info("process_dumped_tickets: No new tickets in dump table to process.")
            return ProcessDumpedTicketsResult(0, 0, 0, 0, 0, 0)

        skipped = sum(
            1
            for dump in dumped_tickets_list
            if not _normalize_dump_user_id((dump.data or {}).get("user_id"))
        )
        unique_tickets = _dedupe_dumps_latest_wins(dumped_tickets_list)
        candidates: List[SupportTicketDump] = []

        for dump_ticket in unique_tickets:
            if not dump_ticket.tenant_id:
                skipped += 1
                continue
            if not Tenant.objects.filter(id=dump_ticket.tenant_id).exists():
                skipped += 1
                continue
            candidates.append(dump_ticket)

        dump_ids = [t.id for t in dumped_tickets_list]

        if not candidates:
            marked = SupportTicketDump.objects.filter(id__in=dump_ids).update(
                is_processed=True
            )
            return ProcessDumpedTicketsResult(
                total_dumped_tickets=len(dumped_tickets_list),
                unique_tickets=len(unique_tickets),
                inserted_tickets=0,
                mirrored_records=0,
                skipped_tickets=skipped,
                marked_processed=marked,
            )

        dumps_to_insert: List[SupportTicketDump] = []
        for dump_ticket in candidates:
            user_id = _normalize_dump_user_id((dump_ticket.data or {}).get("user_id"))
            tenant_id = dump_ticket.tenant_id
            dump_is_self_trial = _dump_is_self_trial(dump_ticket)
            has_open_self_trial = _has_open_self_trial_record_for_user(
                user_id,
                tenant_id=tenant_id,
            )

            if has_open_self_trial:
                _delete_open_non_self_trial_records_for_user(
                    user_id=user_id,
                    tenant_id=tenant_id,
                )
                continue

            if dump_is_self_trial:
                _delete_open_non_self_trial_records_for_user(
                    user_id=user_id,
                    tenant_id=tenant_id,
                )
            else:
                _delete_open_support_records_for_user(
                    user_id=user_id,
                    tenant_id=tenant_id,
                )
            dumps_to_insert.append(dump_ticket)
            if user_id:
                dump_data_by_user_id[user_id] = dict(dump_ticket.data or {})

        for dump_ticket in dumps_to_insert:
            inserted_records.append(
                Record.objects.create(
                    tenant_id=dump_ticket.tenant_id,
                    entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                    data=_build_support_record_data_from_dump(dump_ticket),
                )
            )
        marked = SupportTicketDump.objects.filter(id__in=dump_ids).update(
            is_processed=True
        )

    if on_ticket_created:
        for record in inserted_records:
            try:
                user_id = _normalize_dump_user_id((record.data or {}).get("user_id"))
                dump_data = dump_data_by_user_id.get(user_id) if user_id else None
                on_ticket_created(record, dump_data)
            except Exception as exc:
                logger.error(
                    "process_dumped_tickets: on_ticket_created failed for record %s: %s",
                    record.id,
                    exc,
                    exc_info=True,
                )
    inserted_count = len(inserted_records)
    return ProcessDumpedTicketsResult(
        total_dumped_tickets=len(dumped_tickets_list),
        unique_tickets=len(unique_tickets),
        inserted_tickets=inserted_count,
        mirrored_records=inserted_count,
        skipped_tickets=skipped,
        marked_processed=marked,
    )


def process_dumped_tickets_job_result(result: ProcessDumpedTicketsResult) -> Dict[str, Any]:
    return asdict(result)


_EXPIRED_SUPPORT_TICKET_TYPES = frozenset({
    "Trial Expired",
    "Premium Expired",
    "trial_expired",
    "premium_expired",
})


def _record_support_ticket_type_raw(record: Record) -> Any:
    data = record.data or {}
    ticket_type = data.get("support_ticket_type")
    if ticket_type is not None and str(ticket_type).strip():
        return ticket_type
    return data.get("poster")


def _record_ticket_type_key(record: Record) -> str:
    return _canonical_support_ticket_type_key(_record_support_ticket_type_raw(record))


def _raw_ticket_type_from_mapping(data: Mapping[str, Any]) -> Any:
    ticket_type = data.get("support_ticket_type")
    if ticket_type is not None and str(ticket_type).strip():
        return ticket_type
    return data.get("poster")


def _dump_is_self_trial(dump: SupportTicketDump) -> bool:
    return (
        _canonical_support_ticket_type_key(
            _raw_ticket_type_from_mapping(dump.data or {})
        )
        == _SELF_TRIAL_TICKET_TYPE_KEY
    )


def _record_is_self_trial(record: Record) -> bool:
    return _record_ticket_type_key(record) == _SELF_TRIAL_TICKET_TYPE_KEY


def _open_support_records_for_user(
    user_id: Any,
    *,
    tenant_id: Optional[Any] = None,
) -> QuerySet[Record]:
    normalized = _normalize_dump_user_id(user_id)
    if not normalized:
        return support_ticket_records_qs(tenant_id=tenant_id).none()
    return (
        support_ticket_records_qs(tenant_id=tenant_id)
        .filter(data__user_id=normalized)
        .filter(q_record_open_or_snoozed_resolution())
    )


def _has_open_self_trial_record_for_user(
    user_id: Any,
    *,
    tenant_id: Optional[Any] = None,
) -> bool:
    return any(
        _record_is_self_trial(record)
        for record in _open_support_records_for_user(user_id, tenant_id=tenant_id)
    )


def _delete_open_non_self_trial_records_for_user(
    *,
    user_id: Any,
    tenant_id: Optional[Any] = None,
) -> int:
    record_ids = [
        record.id
        for record in _open_support_records_for_user(user_id, tenant_id=tenant_id)
        if not _record_is_self_trial(record)
    ]
    if not record_ids:
        return 0
    count, _ = Record.objects.filter(id__in=record_ids).delete()
    return count


def _support_ticket_records_qs(tenant: Tenant):
    return Record.objects.filter(
        tenant=tenant,
        entity_type=SUPPORT_TICKET_ENTITY_TYPE,
    )


def _exclude_expired_support_ticket_types(qs):
    expired = list(_EXPIRED_SUPPORT_TICKET_TYPES)
    return qs.exclude(
        Q(data__support_ticket_type__in=expired) | Q(data__poster__in=expired)
    )


def _parse_record_data_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        parsed = parse_datetime(value)
        if parsed is None:
            return None
        dt = parsed
    else:
        return None
    if timezone.is_aware(dt):
        return dt
    # Rule engine stores UTC via timezone.now(); treat naive ISO as UTC.
    return timezone.make_aware(dt, dt_timezone.utc)


def _iso_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    return str(value)


def _support_ticket_payload_from_record(record: Record) -> Dict[str, Any]:
    return record_to_ticket_dict(record)


def _assign_support_record(
    record: Record,
    *,
    user_uuid: UUID,
    user_email: str,
) -> Record:
    payload = dict(record.data or {})
    previous = payload.get("assigned_to")
    is_fresh = previous in (None, "", "null", "None")
    payload["assigned_to"] = str(user_uuid)
    payload["cse_name"] = user_email
    if is_fresh and not payload.get("first_assigned_at"):
        now = timezone.now()
        payload["first_assigned_at"] = now.isoformat()
        payload["first_assigned_to"] = str(user_uuid)
    record.data = payload
    record.save(update_fields=["data", "updated_at"])
    return record


def _enqueue_support_assignment_mixpanel(
    record: Record,
    *,
    user_uuid: UUID,
    user_email: str,
) -> None:
    data = record.data or {}
    customer_user_id = data.get("user_id")
    if not customer_user_id:
        return
    mixpanel_properties = support_ticket_mixpanel_properties(
        record,
        assigned_to=str(user_uuid),
        cse_name=user_email,
        cse_email=user_email,
    )
    _enqueue_mixpanel_event(
        user_id=customer_user_id,
        event_name="pyro_st_assigned",
        properties=mixpanel_properties,
        tenant_id=record.tenant_id,
    )

    _enqueue_cse_assigned_event(
        user_id=customer_user_id,
        cse_email=user_email,
        tenant_id=record.tenant_id,
    )


def _apply_support_record_group_filters(qs, *, tenant, request_user):
    membership = TenantMembership.objects.filter(
        tenant=tenant,
        user_id=request_user.supabase_uid,
    ).first()
    if not membership:
        return qs

    group_row = TenantMemberSetting.objects.filter(
        tenant=tenant,
        tenant_membership=membership,
        key=USER_KV_GROUP_ID_KEY,
    ).first()
    group_id = group_row.value if group_row else None
    if not isinstance(group_id, int):
        return qs

    group = Group.objects.filter(tenant=tenant, id=group_id).first()
    group_data = group.group_data if group and isinstance(group.group_data, dict) else {}
    states = group_data.get("states") if isinstance(group_data.get("states"), list) else []
    ticket_types = group_data.get("support_ticket_types")
    if not isinstance(ticket_types, list):
        ticket_types = group_data.get("posters") if isinstance(group_data.get("posters"), list) else []

    if states:
        qs = qs.filter(data__state__in=states)
    if ticket_types:
        qs = qs.filter(
            Q(data__support_ticket_type__in=ticket_types)
            | Q(data__poster__in=ticket_types)
        )
    return qs


class GetWIPTicketsView(APIView):
    """
    Django equivalent of the Supabase get-wip-tickets edge function.
    Fetches tickets assigned to the authenticated user with 'WIP' status.
    """
    permission_classes = [IsTenantAuthenticated]
    
    def get(self, request):
        """
        Get WIP tickets assigned to the authenticated user.
        Returns tickets sorted by creation date (newest first).
        """
        try:
            # Get user from authentication middleware
            user = request.user
            user_id = user.supabase_uid
            
            logger.info(f'Querying for user ID: {user_id}')
            
            wip_records = (
                support_ticket_records_qs(tenant=request.tenant)
                .filter(
                    data__assigned_to=str(user_id),
                    data__resolution_status="WIP",
                )
                .order_by("-created_at")
            )
            return Response(records_to_ticket_dicts(wip_records), status=status.HTTP_200_OK)
            
        except Exception as error:
            logger.error(f'Unexpected error in get-wip-tickets: {error}')
            return Response({
                'error': 'An unexpected error occurred.',
                'details': str(error)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class GetNotConnectedTicketsView(APIView):
    """
    List Not Connected tickets assigned to the authenticated CSE.

    NC outcomes store ``resolution_status=Snoozed`` + ``call_status=Not Connected``
    (same pattern as get-wip-tickets filtering on ``WIP``).
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        try:
            user_id = str(request.user.supabase_uid)
            logger.info("Querying not-connected tickets for user ID: %s", user_id)

            nc_records = (
                support_ticket_records_qs(tenant=request.tenant)
                .filter(
                    data__assigned_to=user_id,
                    data__resolution_status="Snoozed",
                    data__call_status__iexact="Not Connected",
                )
                .order_by("-created_at")
            )
            return Response(
                records_to_ticket_dicts(nc_records),
                status=status.HTTP_200_OK,
            )
        except Exception as error:
            logger.error("Unexpected error in get-not-connected-tickets: %s", error)
            return Response(
                {"error": "An unexpected error occurred.", "details": str(error)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


def _resolve_rate_goal_percent(raw: Optional[int]) -> int:
    if raw is None:
        return SUPPORT_DEFAULT_RESOLVE_RATE_GOAL_PERCENT
    return max(0, min(100, int(raw)))


class SupportDailyProgressView(APIView):
    """
    Today's overall CSE resolve rate vs a single goal.

    ``taken_today`` uses first-assigned-today (same rules as daily limit counting).
    ``resolved_today`` is the subset with success resolution statuses
    (Resolved / Already Resolved / No Issue).
    ``goal_percent`` comes from CSE KV (default 80 when unset).
    """

    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        try:
            from crm_records.lead_pipeline.daily_limit import DailyLimitChecker

            user_id = str(request.user.supabase_uid)
            tenant = request.tenant
            now = timezone.now()
            membership = TenantMembership.objects.filter(
                tenant=tenant, user_id=request.user.supabase_uid
            ).first()

            goal_raw = None
            if membership:
                row = TenantMemberSetting.objects.filter(
                    tenant=tenant,
                    tenant_membership=membership,
                    key=USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY,
                ).first()
                if row is not None:
                    goal_raw = coerce_kv_int(row.value)

            success = list(SUPPORT_RESOLVE_RATE_SUCCESS_STATUSES)
            checker = DailyLimitChecker()
            taken = checker.count_assigned_today(
                tenant=tenant,
                user_identifier=user_id,
                now=now,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                type_q=None,
            )
            resolved = checker.count_assigned_today(
                tenant=tenant,
                user_identifier=user_id,
                now=now,
                entity_type=SUPPORT_TICKET_ENTITY_TYPE,
                type_q=None,
                resolution_statuses=success,
            )
            goal_percent = _resolve_rate_goal_percent(goal_raw)
            resolve_rate = (
                round((resolved / taken) * 100, 1) if taken > 0 else None
            )

            return Response(
                {
                    "taken_today": taken,
                    "resolved_today": resolved,
                    "resolve_rate": resolve_rate,
                    "goal_percent": goal_percent,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as error:
            logger.error("Unexpected error in support daily-progress: %s", error)
            return Response(
                {"error": "An unexpected error occurred.", "details": str(error)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@method_decorator(csrf_exempt, name='dispatch')
class DumpTicketWebhookView(APIView):
    """
    Django equivalent of the Supabase edge function dump-ticket-webhook.
    Does exactly what the edge function does - nothing more, nothing less.
    Authentication relies only on x-webhook-secret header, no bearer token required.
    """
    authentication_classes = []  # No bearer token authentication required
    permission_classes = [AllowAny]
    
    def options(self, request):
        """Handle CORS preflight requests"""
        response = Response('ok', status=status.HTTP_200_OK)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Headers'] = 'authorization, x-client-info, apikey, content-type, x-webhook-secret'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        return response
    
    def post(self, request):
        """Main webhook handler - exactly like edge function"""
        try:
            # 1. Validate webhook secret for security
            webhook_secret = request.headers.get('x-webhook-secret')
            stored_secret = os.environ.get('WEBHOOK_SECRET')
            
            if not webhook_secret or webhook_secret != stored_secret:
                logger.warning('Unauthorized webhook attempt.')
                return Response({
                    'error': 'Unauthorized: Invalid or missing webhook secret'
                }, status=status.HTTP_401_UNAUTHORIZED)
            
            # 2. Parse the incoming JSON payload
            payload = request.data
            if not payload or not isinstance(payload, dict):
                raise Exception("Invalid or empty JSON payload.")
            
            if not payload.get('tenant_id'):
                raise Exception("Missing required field: tenant_id")

            dump_payload = _extract_dump_payload(payload)
            if not dump_payload.get("ticket_date"):
                dump_payload = _serialize_dump_payload(
                    {**dump_payload, "ticket_date": timezone.now()}
                )

            SupportTicketDump.objects.create(
                tenant_id=payload['tenant_id'],
                data=dump_payload,
                is_processed=False,
            )

            process_dumped_tickets(
                tenant_id=payload['tenant_id'],
                on_ticket_created=on_ticket_created_after_dump,
            )

            record_id = _resolve_support_ticket_record_id(
                tenant_id=payload['tenant_id'],
                dump_payload=dump_payload,
            )

            return Response({
                'message': 'Ticket created successfully in dump table',
                'ticket_id': _coerce_webhook_api_id(
                    _support_ticket_id_from_dump_data(dump_payload)
                ),
                'record_id': record_id,
            }, status=status.HTTP_200_OK)
            
        except Exception as error:
            logger.error(f'Critical error: {error}')
            return Response({
                'error': str(error)
            }, status=status.HTTP_400_BAD_REQUEST)


class SaveAndContinueView(APIView):
    """
    Django equivalent of the Supabase save-and-continue edge function.
    Updates support tickets with resolution status and sends Mixpanel events.
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated] # <--- ADD THIS LINE!
    
    def options(self, request):
        """Handle CORS preflight requests"""
        response = Response('ok', status=status.HTTP_200_OK)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        return response
    
    def post(self, request):
        """Dispatch a button action as a CRM event on the support ticket record."""
        try:
            user = request.user
            user_id = user.supabase_uid
            user_email = user.email

            if not user_id:
                return Response({'error': 'No user id in JWT'}, status=status.HTTP_400_BAD_REQUEST)

            serializer = SaveAndContinueSerializer(
                data=request.data,
                context={'request': request},
            )
            if not serializer.is_valid():
                return Response({
                    'error': 'Invalid request data',
                    'details': serializer.errors,
                }, status=status.HTTP_400_BAD_REQUEST)

            validated_data = serializer.validated_data
            ticket_id = validated_data['ticketId']
            resolution_status = validated_data.get('resolutionStatus')
            event_name = SAVE_AND_CONTINUE_RESOLUTION_EVENTS.get(resolution_status or '')
            if not event_name:
                return Response({
                    'error': f'Unsupported resolution status: {resolution_status}',
                }, status=status.HTTP_400_BAD_REQUEST)

            record = resolve_support_ticket_record(tenant=request.tenant, ticket_id=ticket_id)
            if not record:
                return Response({'error': 'Ticket not found'}, status=status.HTTP_404_NOT_FOUND)

            ticket_assigned_to = (record.data or {}).get("assigned_to")
            if ticket_assigned_to and str(ticket_assigned_to).lower() not in ("", "null", "none"):
                try:
                    if str(UUID(str(ticket_assigned_to))) != str(UUID(str(user_id))):
                        logger.warning(
                            "[SaveAndContinueView] Ownership conflict: user=%s attempted action on ticket=%s assigned_to=%s",
                            user_id, ticket_id, ticket_assigned_to,
                        )
                        return Response(
                            {'error': 'This ticket is no longer assigned to you. Please refresh and get a new ticket.'},
                            status=status.HTTP_409_CONFLICT,
                        )
                except (ValueError, AttributeError, TypeError):
                    pass  # Non-UUID assigned_to value — skip ownership check

            payload = {
                'resolutionStatus': resolution_status,
                'callStatus': validated_data.get('callStatus'),
                'cseRemarks': validated_data.get('cseRemarks'),
                'resolutionTime': validated_data.get('resolutionTime'),
                'otherReasons': validated_data.get('otherReasons', []),
                'reviewRequested': validated_data.get('reviewRequested'),
            }
            record = log_and_dispatch_support_ticket_event(
                record=record,
                tenant=request.tenant,
                event_name=event_name,
                payload=payload,
                actor_user_id=str(user_id),
                actor_email=user_email,
            )

            updated_ticket = _support_ticket_payload_from_record(record)
            response_data = {
                'success': True,
                'message': 'Ticket updated successfully',
                'updatedTicket': updated_ticket,
                'userId': str(user_id) if user_id is not None else None,
                'userEmail': user_email,
                'totalResolutionTime': updated_ticket.get('resolution_time') or '0:00',
            }

            response = Response(response_data, status=status.HTTP_200_OK)
            response['Access-Control-Allow-Origin'] = '*'
            return response

        except Exception as error:
            logger.error('Error in save-and-continue function: %s', error, exc_info=True)
            response = Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            response['Access-Control-Allow-Origin'] = '*'
            return response


class GetNextTicketView(APIView):
    """
    Django equivalent of the Supabase get-next-ticket edge function.

    Reads and assigns support tickets from ``records`` (``entity_type=support_ticket``).
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    def options(self, request):
        """Handle CORS preflight requests"""
        response = Response('ok', status=status.HTTP_200_OK)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        response['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        return response
    
    def get(self, request):
        """Main get-next-ticket handler - exactly like edge function"""
        try:
            # Get user from authentication middleware (IsTenantAuthenticated already handles auth)
            user = request.user
            user_id = user.supabase_uid
            user_email = user.email

            logger.info("=" * 80)
            logger.info("🎫 [GetNextTicketView] GET TICKETS BUTTON CLICKED")
            logger.info("=" * 80)
            logger.info(f"=== TICKET ORDERING VALIDATION ===")
            logger.info(f"Current time: {timezone.now()}")
            logger.info(f"User ID: {user_id}")
            logger.info(f"User Email: {user_email}")

            # Ensure current user exists in auth.users (FK target for assigned_to) before assigning
            try:
                user_uuid = UUID(str(user_id))
            except (ValueError, AttributeError, TypeError):
                logger.warning(
                    "[GetNextTicketView] Invalid user supabase_uid; cannot assign ticket",
                    extra={"user_id": user_id, "user_email": user_email},
                )
                response = Response(
                    {
                        "error": "Your account could not be verified. Please sign out and sign in again, or contact support.",
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )
                response["Access-Control-Allow-Origin"] = "*"
                return response

            if not SupabaseAuthUser.objects.filter(id=user_uuid).exists():
                logger.warning(
                    "[GetNextTicketView] User not found in auth.users (assignee would violate FK); refusing to assign",
                    extra={
                        "user_id": str(user_uuid),
                        "user_email": user_email,
                        "assignee_in_auth_users": False,
                    },
                )
                try:
                    import sentry_sdk
                    sentry_sdk.set_user({"id": str(user_uuid), "email": user_email or ""})
                    sentry_sdk.set_tag("get_next_ticket_assigned_to_fk", "assignee_not_in_auth_users")
                except Exception:
                    pass
                response = Response(
                    {
                        "error": "Your account is not found in the auth system. Please sign out and sign in again, or contact support.",
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )
                response["Access-Control-Allow-Origin"] = "*"
                return response

            # Get the next ticket
            logger.info(f"[GetNextTicketView] Calling _get_and_assign_ticket to find and assign ticket...")
            with transaction.atomic():
                next_record = self._get_and_assign_ticket(request, user, user_email)

            # If no tickets available, return empty object
            if not next_record:
                logger.info("[GetNextTicketView] ⚠️ No tickets available - returning empty response")
                response = Response({}, status=status.HTTP_200_OK)
                response['Access-Control-Allow-Origin'] = '*'
                return response
            
            # Return the ticket
            logger.info(f"[GetNextTicketView] ✅ Ticket found and assigned - Record ID: {next_record.id}")
            logger.info(
                "[GetNextTicketView] Ticket user_id (customer): %s",
                (next_record.data or {}).get("user_id"),
            )
            logger.info(f"[GetNextTicketView] Assigned to CSE: {user_email} ({user_id})")
            response_data = {
                "ticket": _support_ticket_payload_from_record(next_record),
            }
            serializer = GetNextTicketResponseSerializer(response_data)
            
            response = Response(serializer.data, status=status.HTTP_200_OK)
            response['Access-Control-Allow-Origin'] = '*'
            logger.info("=" * 80)
            return response
            
        except IntegrityError as error:
            user_id_ctx = getattr(request.user, "supabase_uid", None)
            user_email_ctx = getattr(request.user, "email", None)
            logger.error(
                "get-next-ticket: database constraint violation (e.g. assigned_to FK); assignee may not exist in auth.users",
                exc_info=True,
                extra={
                    "user_id": str(user_id_ctx) if user_id_ctx else None,
                    "user_email": user_email_ctx,
                    "error": str(error),
                },
            )
            try:
                import sentry_sdk
                sentry_sdk.set_user({"id": str(user_id_ctx), "email": user_email_ctx or ""})
                sentry_sdk.set_tag("get_next_ticket_assigned_to_fk", "integrity_error")
                sentry_sdk.capture_exception(error)
            except Exception:
                pass
            response = Response(
                {"error": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
            response["Access-Control-Allow-Origin"] = "*"
            return response
        except Exception as error:
            user_id_ctx = getattr(request.user, "supabase_uid", None)
            user_email_ctx = getattr(request.user, "email", None)
            logger.error(f"Error in get-next-ticket function: {error}", exc_info=True)
            try:
                import sentry_sdk
                sentry_sdk.set_user({"id": str(user_id_ctx), "email": user_email_ctx or ""})
            except Exception:
                pass
            response = Response(
                {"error": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
            response["Access-Control-Allow-Origin"] = "*"
            return response

    def _get_and_assign_ticket(self, request, user, user_email):
        """
        Assign the next support ticket via ``SupportTicketPipeline`` (bucket architecture).

        Bucket order: fresh Open (ST/other equal) → NC today → WIP today → NC yesterday → WIP yesterday.
        """
        tenant = getattr(request, "tenant", None)
        if not tenant:
            logger.warning("[_get_and_assign_ticket] Missing tenant on request")
            return None

        try:
            user_uuid_obj = UUID(str(user.supabase_uid))
        except (ValueError, AttributeError, TypeError) as e:
            logger.error("[_get_and_assign_ticket] Invalid user supabase_uid: %s", e)
            return None

        # Ensure CSE buckets exist (idempotent).
        seed_cse_support_buckets(tenant)

        with transaction.atomic():
            record = SupportTicketPipeline().get_next(
                tenant=tenant,
                request_user=request.user,
                user_email=user_email,
            )
        if not record:
            return None

        _enqueue_support_assignment_mixpanel(
            record,
            user_uuid=user_uuid_obj,
            user_email=user_email,
        )
        return record

class UpdateCallStatusView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        try:
            ser = UpdateCallStatusRequestSerializer(
                data=request.data,
                context={"request": request},
            )
            if not ser.is_valid():
                return Response({"error": ser.errors}, status=status.HTTP_400_BAD_REQUEST)

            validated = ser.validated_data
            ticket_id = validated["ticketId"]
            call_status = validated["callStatus"]

            if call_status != "Not Connected":
                return Response(
                    {"error": "Only Not Connected is supported; use save-and-continue for other outcomes"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            record = resolve_support_ticket_record(tenant=request.tenant, ticket_id=ticket_id)
            if not record:
                return Response({"error": "Ticket not found"}, status=status.HTTP_404_NOT_FOUND)

            actor_uid = str(request.user.supabase_uid)
            ticket_assigned_to = (record.data or {}).get("assigned_to")
            if ticket_assigned_to and str(ticket_assigned_to).lower() not in ("", "null", "none"):
                try:
                    if str(UUID(str(ticket_assigned_to))) != str(UUID(actor_uid)):
                        logger.warning(
                            "[UpdateCallStatusView] Ownership conflict: user=%s attempted NC on ticket=%s assigned_to=%s",
                            actor_uid, ticket_id, ticket_assigned_to,
                        )
                        return Response(
                            {"error": "This ticket is no longer assigned to you. Please refresh and get a new ticket."},
                            status=status.HTTP_409_CONFLICT,
                        )
                except (ValueError, AttributeError, TypeError):
                    pass  # Non-UUID assigned_to value — skip ownership check

            event_payload = {
                "callStatus": call_status,
                "resolutionStatus": validated.get("resolutionStatus"),
                "cseRemarks": validated.get("cseRemarks"),
                "resolutionTime": validated.get("resolutionTime"),
                "otherReasons": validated.get("otherReasons"),
            }
            record = log_and_dispatch_support_ticket_event(
                record=record,
                tenant=request.tenant,
                event_name=SUPPORT_EVENT_NOT_CONNECTED,
                payload=event_payload,
                actor_user_id=actor_uid,
                actor_email=getattr(request.user, "email", None),
            )
            return Response(_support_ticket_payload_from_record(record), status=200)

        except Exception:
            return Response({"error": "Internal server error"}, status=500)

class SupportTicketUpdateView(APIView):
    """
    API endpoint for admins to update support tickets, specifically for assigning tickets to CSEs
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]
    
    def patch(self, request):
        """Update support ticket fields - primarily for admin assignment"""
        try:
            # Get user from authentication middleware (IsTenantAuthenticated already handles auth)
            user = request.user
            user_id = user.supabase_uid
            user_email = user.email
            
            logger.info(f'Admin updating ticket - Admin ID: {user_id}, Admin Email: {user_email}')
            
            serializer = SupportTicketUpdateSerializer(
                data=request.data,
                context={"request": request},
            )
            if not serializer.is_valid():
                return Response({
                    'error': 'Invalid request data',
                    'details': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            validated_data = serializer.validated_data
            ticket_id = validated_data["ticket_id"]

            record = resolve_support_ticket_record(tenant=request.tenant, ticket_id=ticket_id)
            if not record:
                return Response({"error": "Ticket not found"}, status=status.HTTP_404_NOT_FOUND)

            update_data: Dict[str, Any] = {}
            for key, value in validated_data.items():
                if key == "ticket_id":
                    continue
                if key == "assigned_to" and value is not None:
                    update_data["assigned_to"] = str(value)
                else:
                    update_data[key] = value

            with transaction.atomic():
                record = apply_record_data_updates(record, update_data)

            logger.info("Ticket record %s updated by admin %s", ticket_id, user_email)
            response_data = {
                "success": True,
                "message": "Ticket updated successfully",
                "updated_ticket": record_to_ticket_dict(record),
                "updated_by": user_email,
                "updated_fields": list(update_data.keys()),
            }
            
            response = Response(response_data, status=status.HTTP_200_OK)
            response['Access-Control-Allow-Origin'] = '*'
            return response
            
        except Exception as error:
            logger.error(f'Error in support ticket update: {error}')
            response = Response({
                'error': 'Internal server error'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            response['Access-Control-Allow-Origin'] = '*'
            return response

#
def _call_status_is_not_connected(call_status: Any) -> bool:
    normalized = str(call_status or "").strip().lower().replace("_", " ")
    return normalized in {"not connected", "notconnected"}


def _is_fresh_take_break_ticket(
    data: Mapping[str, Any],
    *,
    payload_resolution_status: Optional[str] = None,
) -> bool:
    """
    Fresh tickets may be released on take-break (frees daily-limit quota).

    WIP and Snoozed / Not Connected stay locked to the CSE.
    """
    resolution = (data or {}).get("resolution_status")
    if resolution == "WIP" or payload_resolution_status == "WIP":
        return False
    if resolution == "Snoozed" or payload_resolution_status == "Snoozed":
        return False
    if _call_status_is_not_connected((data or {}).get("call_status")):
        return False
    return True


def _clear_take_break_assignment_fields(record: Record) -> bool:
    """
    Unassign + clear first_assigned_* so daily-limit / taken-today counts drop.

    Returns True if ``record.data`` changed.
    """
    payload = dict(record.data or {})
    changed = False
    for key in ("assigned_to", "cse_name", "first_assigned_at", "first_assigned_to"):
        if payload.get(key) is not None:
            payload[key] = None
            changed = True
    if changed:
        record.data = payload
        record.save(update_fields=["data", "updated_at"])
    return changed


class TakeBreakView(APIView):
    """
    Take a break on the current ticket.

    For **fresh** tickets: unassign + clear ``first_assigned_*`` so today's
    daily-limit count decreases. WIP / Snoozed (NC) stay locked to the CSE.
    """
    authentication_classes = [SupabaseJWTAuthentication]
    permission_classes = [IsTenantAuthenticated]

    def options(self, request):
        """Handle CORS preflight requests"""
        response = Response('ok', status=status.HTTP_200_OK)
        response['Access-Control-Allow-Origin'] = '*'
        response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        response['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        return response

    def post(self, request):
        """Main take-break handler - exactly like edge function"""
        try:
            # Get user from authentication middleware
            user = request.user
            user_id = user.supabase_uid
            user_email = user.email
            
            if not user_id:
                return Response({
                    'error': 'No user id in JWT'
                }, status=status.HTTP_400_BAD_REQUEST)

            serializer = TakeBreakSerializer(
                data=request.data,
                context={"request": request},
            )
            if not serializer.is_valid():
                return Response({
                    'error': 'Invalid request data',
                    'details': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)

            validated_data = serializer.validated_data
            ticket_id = validated_data['ticketId']
            resolution_status_payload = validated_data.get('resolutionStatus')

            record = resolve_support_ticket_record(tenant=request.tenant, ticket_id=ticket_id)
            if not record:
                return Response({'error': 'Ticket not found'}, status=status.HTTP_404_NOT_FOUND)

            data_before = dict(record.data or {})
            is_fresh = _is_fresh_take_break_ticket(
                data_before,
                payload_resolution_status=resolution_status_payload,
            )

            log_and_dispatch_support_ticket_event(
                record=record,
                tenant=request.tenant,
                event_name=SUPPORT_EVENT_TAKE_BREAK,
                payload={'resolutionStatus': resolution_status_payload},
                actor_user_id=str(user_id),
                actor_email=user_email,
            )
            record.refresh_from_db()
            # Guarantee quota release for fresh tickets even if RuleSet is stale.
            if is_fresh:
                _clear_take_break_assignment_fields(record)
                record.refresh_from_db()

            assigned = str((record.data or {}).get("assigned_to") or "").strip()
            still_assigned = assigned not in ("", "null", "None")
            should_unassign = not still_assigned
            if still_assigned:
                message = (
                    "Ticket kept assigned (WIP or Snoozed / Not Connected). Taking a break."
                )
            else:
                message = "Ticket unassigned. Taking a break."

            response_data = {
                'success': True,
                'message': message,
                'ticketUnassigned': should_unassign,
                'userId': user_id,
                'userEmail': user_email
            }

            response = Response(response_data, status=status.HTTP_200_OK)
            response['Access-Control-Allow-Origin'] = '*'
            return response

        except Exception as error:
            logger.error(f'Error in take-break function: {error}')
            response = Response({
                'error': 'Internal server error'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            response['Access-Control-Allow-Origin'] = '*'
            return response


class ProcessDumpedTicketsView(APIView):
    """
    Manual / ops trigger: enqueue ``process_dumped_tickets`` background job(s).

    Normal flow: background worker enqueues every 5 minutes for tenants with
    unprocessed dumps. POST body may include ``tenant_id`` for a single tenant.
    """
    permission_classes = [AllowAny]

    def post(self, request):
        try:
            tenant_id = (request.data or {}).get('tenant_id')
            if tenant_id:
                job = enqueue_process_dumped_tickets_job(tenant_id)
                if not job:
                    return Response({
                        'message': 'Job already queued or running for tenant',
                        'tenant_id': str(tenant_id),
                    }, status=status.HTTP_200_OK)
                return Response({
                    'message': 'Job enqueued',
                    'job_id': job.id,
                    'tenant_id': str(tenant_id),
                }, status=status.HTTP_202_ACCEPTED)

            result = enqueue_process_dumped_tickets_for_pending_dumps()
            return Response({
                'message': 'Jobs enqueued for tenants with unprocessed dumps',
                **result,
            }, status=status.HTTP_202_ACCEPTED)

        except Exception as error:
            logger.error(
                'ProcessDumpedTicketsView: Failed to enqueue jobs: %s',
                error,
                exc_info=True,
            )
            return Response({
                'error': str(error),
                'message': 'Failed to enqueue process dumped tickets jobs',
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def resolve_entity_api_tenant(request) -> Tuple[Optional[Tenant], Optional[Response]]:
    """Resolve tenant for ``/entity/``-style external APIs."""
    tenant_id = request.query_params.get("tenant_id") or request.data.get("tenant_id")
    if tenant_id:
        try:
            return Tenant.objects.get(id=tenant_id), None
        except Tenant.DoesNotExist:
            return None, Response(
                {"error": f"Tenant with id {tenant_id} not found"},
                status=status.HTTP_404_NOT_FOUND,
            )
        except (ValueError, TypeError):
            return None, Response(
                {"error": f"Invalid tenant_id format: {tenant_id}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

    api_secret_obj = getattr(request, "api_secret_obj", None)
    if api_secret_obj and api_secret_obj.tenant:
        return api_secret_obj.tenant, None

    default_slug = getattr(settings, "DEFAULT_TENANT_SLUG", "bibhab-thepyro-ai")
    try:
        return Tenant.objects.get(slug=default_slug), None
    except Tenant.DoesNotExist:
        tenant = Tenant.objects.first()
        if tenant:
            return tenant, None
        return None, Response(
            {"error": "No tenant found in database"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )


def _entity_request_body(request) -> Dict[str, Any]:
    body = request.data
    return body if isinstance(body, dict) else {}


def _parse_entity_ticket_id(request) -> Tuple[Optional[int], Optional[Response]]:
    body = _entity_request_body(request)
    raw = request.query_params.get("ticket_id") or body.get("ticket_id")
    if raw is None or raw == "":
        return None, Response(
            {"error": "ticket_id is required (in query param or request body)"},
            status=status.HTTP_400_BAD_REQUEST,
        )
    try:
        return int(raw), None
    except (TypeError, ValueError):
        return None, Response(
            {"error": "ticket_id must be an integer"},
            status=status.HTTP_400_BAD_REQUEST,
        )


def extract_support_ticket_entity_updates(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """Read update fields from the ``data`` object only."""
    nested = request_data.get("data")
    if not isinstance(nested, dict):
        return {}
    updates = dict(nested)
    updates.pop("ticket_id", None)
    updates.pop("tenant_id", None)
    return updates


def _entity_task_label(task_item: Dict[str, Any]) -> Optional[str]:
    label = task_item.get("task") or task_item.get("task_name")
    if label is None:
        return None
    normalized = str(label).strip()
    return normalized or None


def merge_entity_tasks_partial(existing: Any, incoming: Any) -> List[Dict[str, Any]]:
    """
    Merge ``incoming`` tasks into ``existing`` by task name.

    Only tasks included in the payload are updated; others are unchanged.
    """
    merged: List[Dict[str, Any]] = [
        dict(task) for task in existing if isinstance(task, dict)
    ] if isinstance(existing, list) else []

    if not isinstance(incoming, list) or not incoming:
        return merged

    index_by_name: Dict[str, int] = {}
    for index, task in enumerate(merged):
        label = _entity_task_label(task)
        if label:
            index_by_name[label] = index

    for item in incoming:
        if not isinstance(item, dict):
            continue
        label = _entity_task_label(item)
        if not label:
            continue
        normalized = {**item, "task": label}
        normalized.pop("task_name", None)

        if label in index_by_name:
            idx = index_by_name[label]
            merged[idx] = {**merged[idx], **normalized, "task": label}
        else:
            merged.append(normalized)
            index_by_name[label] = len(merged) - 1

    return merged


class SupportTicketEntityAPIView(APIView):
    """
    PATCH support ticket by ``ticket_id`` (``records.id``) at ``/entity/support_ticket/``.

    Send ``ticket_id`` at the request root (or as a query param). Put all update
    fields inside a ``data`` object. Tasks are merged by name — only tasks included
    in ``data.tasks`` are updated::

        {
            "ticket_id": 12345,
            "data": {
                "tasks": [{"task": "App Installation", "status": "Yes"}],
                "resolution_status": "In Progress",
                "cse_remarks": "Demo done"
            }
        }

    Auth: ``X-Secret-Pyro`` header (same as ``/entity/``).
    """

    authentication_classes = []
    permission_classes = [HasAPISecret]

    def patch(self, request):
        tenant, error_response = resolve_entity_api_tenant(request)
        if error_response:
            return error_response

        ticket_id, error_response = _parse_entity_ticket_id(request)
        if error_response:
            return error_response

        record = resolve_support_ticket_record(tenant=tenant, ticket_id=ticket_id)
        if not record:
            return Response(
                {"error": f"Support ticket with ticket_id {ticket_id} not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        body = _entity_request_body(request)
        if not isinstance(body.get("data"), dict):
            return Response(
                {"error": "data object is required in request body"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        updates = extract_support_ticket_entity_updates(body)
        incoming_tasks = updates.pop("tasks", None)
        if not updates and incoming_tasks is None:
            return Response(
                {"error": "At least one field must be provided for update"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if "assigned_to" in updates and updates["assigned_to"] is not None:
            updates["assigned_to"] = str(updates["assigned_to"])

        with transaction.atomic():
            if incoming_tasks is not None:
                payload = dict(record.data or {})
                payload["tasks"] = merge_entity_tasks_partial(payload.get("tasks"), incoming_tasks)
                record.data = payload
                record.save(update_fields=["data", "updated_at"])
            if updates:
                record = apply_record_data_updates(record, updates)

        logger.info(
            "[SupportTicketEntityAPI] Updated record id=%s ticket_id=%s tenant=%s fields=%s tasks=%s",
            record.id,
            ticket_id,
            tenant.slug,
            list(updates.keys()),
            len(incoming_tasks) if isinstance(incoming_tasks, list) else 0,
        )

        return Response(RecordSerializer(record).data, status=status.HTTP_200_OK)
