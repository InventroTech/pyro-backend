"""
Support ticket data on ``crm_records.Record`` (``entity_type=support_ticket``).

Operational reads/writes for queue, analytics, WIP, and admin updates use records.
``process_dumped_tickets`` ingests ``support_ticket_dump`` rows directly into records.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Union

from django.db.models import DateTimeField, F, Q, QuerySet
from django.db.models.fields.json import KeyTextTransform
from django.db.models.functions import Cast, TruncDate
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from crm_records.models import Record
from support_ticket.constants import SUPPORT_TICKET_ENTITY_TYPE

# JSON ``data`` keys mirroring legacy ``support_ticket`` columns.
TICKET_DATA_SEARCH_FIELDS = (
    "name",
    "phone",
    "user_id",
    "reason",
    "poster",
    "support_ticket_type",
    "resolution_status",
    "cse_name",
    "cse_remarks",
    "badge",
    "source",
    "subscription_status",
    "layout_status",
    "state",
    "call_status",
    "rm_name",
)

TICKET_DATA_FILTER_FIELDS = TICKET_DATA_SEARCH_FIELDS


def data_field(name: str) -> str:
    return f"data__{name}"


def q_data_unset(field: str) -> Q:
    """
    Match missing key, JSON ``null``, or SQL-null text extraction.

    - Missing key: ``data__field__isnull=True``
    - JSON null value: ``data__field=None`` (``__isnull`` alone does not match)
    """
    path = f"data__{field}"
    return Q(**{path: None}) | Q(**{f"{path}__isnull": True})


def q_data_json_null(field: str) -> Q:
    """Alias for unset/null/absent JSON values (kept for call-site clarity)."""
    return q_data_unset(field)


def q_data_json_null_or_blank(field: str) -> Q:
    return q_data_unset(field) | Q(**{f"data__{field}": ""})


def q_record_unassigned() -> Q:
    return (
        q_data_unset("assigned_to")
        | Q(data__assigned_to="")
        | Q(data__assigned_to="null")
        | Q(data__assigned_to="None")
    )


def q_record_open_or_snoozed_resolution() -> Q:
    return (
        q_data_unset("resolution_status")
        | Q(data__resolution_status="")
        | Q(data__resolution_status="Snoozed")
    )


def q_record_pending_resolution() -> Q:
    return q_data_unset("resolution_status") | Q(data__resolution_status="")


def q_data_json_has_value(field: str) -> Q:
    return ~q_data_unset(field) & ~Q(**{f"data__{field}": ""})


def support_ticket_records_qs(
    *,
    tenant=None,
    tenant_id: Optional[Any] = None,
) -> QuerySet[Record]:
    qs = Record.objects.filter(entity_type=SUPPORT_TICKET_ENTITY_TYPE)
    if tenant is not None:
        qs = qs.filter(tenant=tenant)
    elif tenant_id is not None:
        qs = qs.filter(tenant_id=tenant_id)
    return qs


def _cast_data_timestamp(field: str) -> Cast:
    """Cast a JSON ISO timestamp string (``data->>'field'``) to timestamptz."""
    return Cast(KeyTextTransform(field, "data"), DateTimeField())


def annotate_ticket_datetimes(qs: QuerySet[Record]) -> QuerySet[Record]:
    """Cast ISO timestamps in ``data`` to datetimes for analytics aggregations."""
    return qs.annotate(
        ticket_completed_at=_cast_data_timestamp("completed_at"),
        ticket_dumped_at=_cast_data_timestamp("dumped_at"),
        ticket_snooze_until=_cast_data_timestamp("snooze_until"),
        ticket_date=_cast_data_timestamp("ticket_date"),
    )


def filter_records_callback_due(
    qs: QuerySet[Record],
    *,
    at: Optional[datetime] = None,
) -> QuerySet[Record]:
    """
    Records whose callback time has passed.

    Matches when ``data.snooze_until`` or ``data.next_call_at`` (ISO strings) is
    set and ``<= at``. Used by get-next-ticket so due retries are not buried
    behind a large batch of not-yet-due snoozed rows.
    """
    at = at or timezone.now()
    return annotate_ticket_datetimes(qs).annotate(
        ticket_next_call_at=_cast_data_timestamp("next_call_at"),
    ).filter(
        Q(ticket_snooze_until__lte=at) | Q(ticket_next_call_at__lte=at),
    )


def _iso_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    return str(value)


def _serialize_extra_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).isoformat()
    return value


def _parse_ticket_tasks_raw(raw: Any) -> List[Dict[str, Any]]:
    """Normalise ``data.tasks`` to a list of ``{task, status}`` dicts."""
    if not raw:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return []
    if not isinstance(raw, list):
        return []

    parsed: List[Dict[str, Any]] = []
    for index, item in enumerate(raw):
        if isinstance(item, str):
            parsed.append({"task": item, "status": ""})
            continue
        if not isinstance(item, dict):
            continue
        label = item.get("task") or item.get("title") or item.get("name")
        if not label:
            continue
        status = item.get("status")
        if status is None:
            status = item.get("rawStatus")
        if status is None or status == "Null":
            status = ""
        else:
            status = str(status)
        parsed.append(
            {
                "task": str(label),
                "status": status,
                "id": str(item.get("id") or label or f"task-{index}"),
            }
        )
    return parsed


def build_ticket_task_progress(raw_tasks: Any) -> List[Dict[str, str]]:
    """
    Build UI stepper steps from ticket tasks.

    Returns ``[{id, label, status}]`` where ``status`` is
    ``completed``, ``current``, or ``pending``.
    """
    source = _parse_ticket_tasks_raw(raw_tasks)
    if not source:
        return []

    steps: List[Dict[str, str]] = []
    for index, item in enumerate(source):
        status_text = str(item.get("status") or "").lower().strip()
        label = str(item["task"])
        step_id = str(item.get("id") or label or f"task-{index}")

        if (
            "yes" in status_text
            or "done" in status_text
            or "complete" in status_text
        ):
            step_status = "completed"
        elif (
            "current" in status_text
            or "progress" in status_text
            or "ongoing" in status_text
        ):
            step_status = "current"
        else:
            step_status = "pending"

        steps.append({"id": step_id, "label": label, "status": step_status})

    if not any(step["status"] == "current" for step in steps):
        for step in steps:
            if step["status"] == "pending":
                step["status"] = "current"
                break

    return steps


def all_support_ticket_tasks_completed(data: Optional[Dict[str, Any]]) -> bool:
    """True when every task in ``data["tasks"]`` is marked complete."""
    tasks = _parse_ticket_tasks_raw((data or {}).get("tasks"))
    if not tasks:
        return False
    for item in tasks:
        status_text = str(item.get("status") or "").lower().strip()
        if not (
            "yes" in status_text
            or "done" in status_text
            or "complete" in status_text
        ):
            return False
    return True


def resolution_status_from_latest_object_history(record: Record) -> Optional[str]:
    """
    Read ``resolution_status`` from the latest ``object_history`` row for this record.

    Used for Praja sync at button-click time — the history row is written when rules
    call ``record.save()`` after the CSE action, without reading ``records.data`` directly.
    """
    from django.contrib.contenttypes.models import ContentType
    from object_history.models import ObjectHistory

    content_type = ContentType.objects.get_for_model(Record)
    entry = (
        ObjectHistory.objects.filter(
            content_type=content_type,
            object_id=str(record.id),
        )
        .order_by("-version")
        .first()
    )
    if not entry:
        return None

    resolution_change = (entry.changes or {}).get("resolution_status")
    if isinstance(resolution_change, dict):
        new_value = resolution_change.get("to")
        if new_value is not None and str(new_value).strip() != "":
            return str(new_value)

    after_data = (entry.after_state or {}).get("data")
    if isinstance(after_data, dict):
        status = after_data.get("resolution_status")
        if status is not None and str(status).strip() != "":
            return str(status)
    return None


def record_to_ticket_dict(record: Record) -> Dict[str, Any]:
    """Flatten a support ticket record for API/analytics responses."""
    data = record.data or {}
    return {
        "id": record.id,
        "record_id": record.id,
        "support_ticket_id": data.get("support_ticket_id") or data.get("ticket_id"),
        "created_at": _iso_or_none(record.created_at),
        "ticket_date": _iso_or_none(data.get("ticket_date")),
        "user_id": data.get("user_id"),
        "name": data.get("name"),
        "phone": data.get("phone"),
        "source": data.get("source"),
        "subscription_status": data.get("subscription_status"),
        "atleast_paid_once": data.get("atleast_paid_once"),
        "reason": data.get("reason"),
        "other_reasons": data.get("other_reasons") or [],
        "badge": data.get("badge"),
        "poster": data.get("poster"),
        "support_ticket_type": data.get("support_ticket_type") or data.get("poster"),
        "tenant_id": str(record.tenant_id) if record.tenant_id else data.get("tenant_id"),
        "assigned_to": data.get("assigned_to"),
        "layout_status": data.get("layout_status"),
        "state": data.get("state"),
        "resolution_status": data.get("resolution_status").upper(),
        "resolution_time": data.get("resolution_time"),
        "cse_name": data.get("cse_name"),
        "cse_remarks": data.get("cse_remarks"),
        "call_status": data.get("call_status") or "Call Waiting",
        "call_attempts": data.get("call_attempts")
        if data.get("call_attempts") is not None
        else 0,
        "completed_at": _iso_or_none(data.get("completed_at")),
        "dumped_at": _iso_or_none(data.get("dumped_at")),
        "snooze_until": _iso_or_none(data.get("snooze_until")),
        "review_requested": bool(data.get("review_requested")),
        "praja_dashboard_user_link": data.get("praja_dashboard_user_link"),
        "Jatra_link": data.get("Jatra_link") or data.get("jatra_link"),
        "display_pic_url": data.get("display_pic_url"),
        "rm_name": data.get("rm_name"),
        "tasks": _parse_ticket_tasks_raw(data.get("tasks")),
        "task_progress": build_ticket_task_progress(data.get("tasks")),
    }


def apply_record_data_updates(
    record: Record,
    updates: Dict[str, Any],
) -> Record:
    payload = dict(record.data or {})
    for key, value in updates.items():
        if key in {"ticket_id", "support_ticket_id", "record_id"}:
            continue
        if value is None:
            payload.pop(key, None)
        else:
            payload[key] = _serialize_extra_value(value)
    record.data = payload
    record.save(update_fields=["data", "updated_at"])
    return record


def distinct_data_values(qs: QuerySet[Record], field: str) -> List[Any]:
    path = data_field(field)
    vals = (
        qs.exclude(q_data_unset(field))
        .exclude(**{path: ""})
        .values_list(path, flat=True)
        .order_by(path)
        .distinct()
    )
    return list(vals)


def extract_date_range_from_ticket_data(
    qs: QuerySet[Record],
    request,
    *,
    data_field_name: str = "completed_at",
) -> tuple:
    """Date range for analytics; reads min/max from a JSON datetime field."""
    from analytics.utils import safe_strptime

    annotated = annotate_ticket_datetimes(qs)
    dt_attr = {
        "completed_at": "ticket_completed_at",
        "dumped_at": "ticket_dumped_at",
    }.get(data_field_name, f"ticket_{data_field_name}")

    ordered = annotated.exclude(**{f"{dt_attr}__isnull": True}).order_by(dt_attr)
    min_rec = ordered.first()
    max_rec = annotated.exclude(**{f"{dt_attr}__isnull": True}).order_by(f"-{dt_attr}").first()
    min_date = getattr(min_rec, dt_attr).date() if min_rec else datetime.today().date()
    max_date = getattr(max_rec, dt_attr).date() if max_rec else datetime.today().date()

    start = request.query_params.get("start")
    end = request.query_params.get("end")
    start_date = safe_strptime(start) or min_date
    end_date = safe_strptime(end) or max_date
    return start_date, end_date


def filter_records_by_tenant_param(qs: QuerySet[Record], request) -> QuerySet[Record]:
    """
    Scope *qs* to ``request.tenant`` (from tenant middleware).

    Does not trust ``tenant_id`` query params — returns an empty queryset when
    tenant context is missing so analytics cannot leak cross-tenant rows.
    """
    tenant = getattr(request, "tenant", None)
    if tenant is not None:
        return qs.filter(tenant=tenant)
    return qs.none()


def records_to_ticket_dicts(records: Iterable[Record]) -> List[Dict[str, Any]]:
    return [record_to_ticket_dict(record) for record in records]


def parse_record_data_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if timezone.is_aware(value) else timezone.make_aware(value)
    if isinstance(value, str):
        parsed = parse_datetime(value)
        if parsed is None:
            return None
        return parsed if timezone.is_aware(parsed) else timezone.make_aware(parsed)
    return None


def trunc_ticket_date(qs: QuerySet[Record], *, source: str = "completed_at") -> QuerySet[Record]:
    """Annotate ``resolved_date`` / ``date`` style trunc from ticket JSON datetimes."""
    annotated = annotate_ticket_datetimes(qs)
    dt_field = {
        "completed_at": "ticket_completed_at",
        "dumped_at": "ticket_dumped_at",
    }[source]
    return annotated.annotate(_trunc=TruncDate(dt_field))
