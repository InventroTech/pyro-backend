"""Tenant-scoped CRM + ERP tools the chat LLM can call."""

from __future__ import annotations

import json
import re
from typing import Any, Optional

from django.db.models import Avg, Count, DurationField, ExpressionWrapper, F, Func, IntegerField, Q
from django.db.models.fields.json import KeyTextTransform

from chatbot.constants import (
    ALLOWED_ENTITY_TYPES,
    CRM_ENTITY_TYPES,
    ERP_ENTITY_TYPES,
    SEARCH_RESULT_LIMIT,
)
from chatbot.services.action_tools import ACTION_TOOL_DEFINITIONS, ACTION_TOOL_HANDLERS
from crm_records.models import Record

# JSON keys allowed for breakdown aggregations (prevents arbitrary path abuse).
_BREAKDOWN_FIELD_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "count_records",
            "description": (
                "Count CRM or ERP records for the current tenant by entity_type. "
                "CRM: lead, support_ticket, job, application. "
                "ERP: inventory_item, inventory_request."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_type": {
                        "type": "string",
                        "enum": list(ALLOWED_ENTITY_TYPES),
                    },
                    "status": {
                        "type": "string",
                        "description": "Optional status filter against data.status or data.lead_status / resolution_status.",
                    },
                },
                "required": ["entity_type"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_records",
            "description": (
                "Search tenant CRM/ERP records by entity_type and free-text query "
                "matched against common JSON fields (name, email, sku, part_number, title)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_type": {
                        "type": "string",
                        "enum": list(ALLOWED_ENTITY_TYPES),
                    },
                    "query": {"type": "string"},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 25,
                        "default": SEARCH_RESULT_LIMIT,
                    },
                },
                "required": ["entity_type"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "summarize_entity_breakdown",
            "description": (
                "Return full-tenant counts grouped by a status-like JSON field "
                "for a CRM/ERP entity_type (e.g. lead_stage, lead_status, "
                "resolution_status, status). Uses SQL aggregation over all matching "
                "records — not a sample."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_type": {
                        "type": "string",
                        "enum": list(ALLOWED_ENTITY_TYPES),
                    },
                    "field": {
                        "type": "string",
                        "description": "JSONB key under data, e.g. lead_status, status, resolution_status",
                    },
                },
                "required": ["entity_type"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "inventory_stock_summary",
            "description": (
                "ERP helper: summarize inventory_item stock levels "
                "(available / allocated / total) for the tenant, optionally filtered by sku."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sku": {
                        "type": "string",
                        "description": "Optional part_number_or_sku / SKU filter",
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 25, "default": 10},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "domain_overview",
            "description": (
                "High-level counts across CRM entities (leads, tickets, jobs) "
                "and ERP entities (inventory items/requests) for the current tenant."
            ),
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "analytics_overview",
            "description": (
                "Show analytics snapshot for the current tenant: lead counts, "
                "support ticket open/resolved breakdown, inventory request counts, "
                "and top status breakdowns. Use when the user asks for analytics, "
                "dashboard stats, reports, SLA overview, or CSE/RM metrics summary. "
                "For trial-activated totals or who activated the most trials, "
                "prefer lead_trial_activation_stats."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "focus": {
                        "type": "string",
                        "enum": ["all", "leads", "tickets", "inventory"],
                        "description": "Optional focus area; default all",
                    }
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "lead_trial_activation_stats",
            "description": (
                "Lead trial-activation analytics for the current tenant: "
                "how many leads are currently TRIAL_ACTIVATED, how many "
                "lead.trial_activated events were logged, and which users "
                "activated the most trials (ranked). Event stats default to "
                "period=this_month (IST). Pass period=today|this_week|this_month|"
                "last_month|all or explicit start_date/end_date. Use for "
                "'how many trial activated', 'who activated the most trials this week'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "period": {
                        "type": "string",
                        "enum": [
                            "today",
                            "yesterday",
                            "this_week",
                            "last_week",
                            "this_month",
                            "last_month",
                            "all",
                        ],
                        "description": "Relative date window (IST). Default this_month.",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Optional ISO date YYYY-MM-DD (overrides period start)",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Optional ISO date YYYY-MM-DD (overrides period end)",
                    },
                    "top_n": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 10,
                        "description": "How many top activators to return",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "support_ticket_resolution_stats",
            "description": (
                "Support-ticket resolver + SLA analytics for the current tenant. "
                "Returns who resolved the most tickets (by cse_name), each CSE's "
                "average resolution_time (first-contact style MM:SS field) and "
                "average SLA (completed_at - created_at), plus the same metrics "
                "grouped by rm_name. Defaults to period=this_month (IST). "
                "Use for: 'who resolved the most tickets this week', 'CSE SLA today', "
                "'RM SLA this month'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "period": {
                        "type": "string",
                        "enum": [
                            "today",
                            "yesterday",
                            "this_week",
                            "last_week",
                            "this_month",
                            "last_month",
                            "all",
                        ],
                        "description": "Relative date window (IST). Default this_month.",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Optional ISO date YYYY-MM-DD filter on completed_at",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "Optional ISO date YYYY-MM-DD filter on completed_at",
                    },
                    "top_n": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 10,
                    },
                    "unit": {
                        "type": "string",
                        "enum": ["seconds", "minutes", "hours"],
                        "default": "minutes",
                    },
                },
            },
        },
    },
] + ACTION_TOOL_DEFINITIONS


def _base_qs(tenant, entity_type: str):
    return Record.objects.filter(tenant=tenant, entity_type=entity_type)


def _validate_entity_type(entity_type: str) -> Optional[str]:
    if entity_type not in ALLOWED_ENTITY_TYPES:
        return f"entity_type must be one of {ALLOWED_ENTITY_TYPES}"
    return None


def _status_q(status: str) -> Q:
    status = (status or "").strip()
    if not status:
        return Q()
    return (
        Q(data__status__iexact=status)
        | Q(data__lead_status__iexact=status)
        | Q(data__resolution_status__iexact=status)
    )


def _record_summary(record: Record) -> dict[str, Any]:
    data = record.data if isinstance(record.data, dict) else {}
    return {
        "id": str(record.id),
        "entity_type": record.entity_type,
        "name": data.get("name") or data.get("title") or data.get("part_number_or_sku") or "",
        "status": (
            data.get("status")
            or data.get("lead_status")
            or data.get("resolution_status")
            or ""
        ),
        "sku": data.get("part_number_or_sku") or data.get("sku") or "",
        "available_quantity": data.get("available_quantity"),
        "created_at": record.created_at.isoformat() if record.created_at else None,
    }


def tool_count_records(tenant, entity_type: str, status: str = "") -> dict[str, Any]:
    err = _validate_entity_type(entity_type)
    if err:
        return {"error": err}
    qs = _base_qs(tenant, entity_type)
    if status:
        qs = qs.filter(_status_q(status))
    return {
        "entity_type": entity_type,
        "status_filter": status or None,
        "count": qs.count(),
        "domain": "erp" if entity_type in ERP_ENTITY_TYPES else "crm",
    }


def tool_search_records(
    tenant,
    entity_type: str,
    query: str = "",
    limit: int = SEARCH_RESULT_LIMIT,
) -> dict[str, Any]:
    err = _validate_entity_type(entity_type)
    if err:
        return {"error": err}
    limit = max(1, min(int(limit or SEARCH_RESULT_LIMIT), 25))
    qs = _base_qs(tenant, entity_type).order_by("-created_at")
    q = (query or "").strip()
    if q:
        qs = qs.filter(
            Q(data__name__icontains=q)
            | Q(data__title__icontains=q)
            | Q(data__email__icontains=q)
            | Q(data__part_number_or_sku__icontains=q)
            | Q(data__sku__icontains=q)
        )
    rows = [_record_summary(r) for r in qs[:limit]]
    return {
        "entity_type": entity_type,
        "query": q or None,
        "count": len(rows),
        "results": rows,
        "domain": "erp" if entity_type in ERP_ENTITY_TYPES else "crm",
    }


def _safe_breakdown_field(field: str) -> Optional[str]:
    raw = (field or "").strip()
    if not raw or not _BREAKDOWN_FIELD_RE.match(raw):
        return None
    return raw


def _aggregate_field_breakdown(
    tenant,
    entity_type: str,
    field: str,
    *,
    top_n: int = 30,
) -> dict[str, Any]:
    """
    Full-table GROUP BY on records.data->>field (not a Python sample).
    """
    safe = _safe_breakdown_field(field)
    if not safe:
        return {"error": f"Invalid breakdown field: {field}"}

    qs = _base_qs(tenant, entity_type)
    total = qs.count()
    rows = (
        qs.annotate(_bv=KeyTextTransform(safe, "data"))
        .values("_bv")
        .annotate(count=Count("id"))
        .order_by("-count")[: max(1, int(top_n))]
    )
    breakdown = [
        {
            "value": "(empty)" if r["_bv"] in (None, "") else str(r["_bv"]),
            "count": int(r["count"]),
        }
        for r in rows
    ]
    return {
        "field": safe,
        "total": total,
        "total_in_breakdown": sum(item["count"] for item in breakdown),
        "complete": True,
        "breakdown": breakdown,
    }


def tool_summarize_entity_breakdown(
    tenant,
    entity_type: str,
    field: str = "",
) -> dict[str, Any]:
    err = _validate_entity_type(entity_type)
    if err:
        return {"error": err}

    defaults = {
        "lead": "lead_status",
        "support_ticket": "resolution_status",
        "inventory_request": "status",
        "inventory_item": "status",
        "job": "status",
        "application": "status",
    }
    field = (field or defaults.get(entity_type, "status")).strip()
    agg = _aggregate_field_breakdown(tenant, entity_type, field, top_n=30)
    if agg.get("error"):
        return agg
    return {
        "entity_type": entity_type,
        "field": agg["field"],
        "total": agg["total"],
        "total_in_breakdown": agg["total_in_breakdown"],
        "complete": True,
        "breakdown": agg["breakdown"],
        "domain": "erp" if entity_type in ERP_ENTITY_TYPES else "crm",
    }


def tool_inventory_stock_summary(
    tenant,
    sku: str = "",
    limit: int = 10,
) -> dict[str, Any]:
    limit = max(1, min(int(limit or 10), 25))
    qs = _base_qs(tenant, "inventory_item").order_by("-updated_at")
    sku = (sku or "").strip()
    if sku:
        qs = qs.filter(
            Q(data__part_number_or_sku__icontains=sku) | Q(data__sku__icontains=sku) | Q(data__name__icontains=sku)
        )
    items = []
    avail_sum = 0
    alloc_sum = 0
    for rec in qs[:limit]:
        data = rec.data if isinstance(rec.data, dict) else {}
        avail = data.get("available_quantity") or 0
        alloc = data.get("allocated_quantity") or 0
        try:
            avail_sum += float(avail)
            alloc_sum += float(alloc)
        except (TypeError, ValueError):
            pass
        items.append(_record_summary(rec))
    return {
        "domain": "erp",
        "sku_filter": sku or None,
        "items": items,
        "available_quantity_sum": avail_sum,
        "allocated_quantity_sum": alloc_sum,
        "item_count_returned": len(items),
        "total_inventory_items": _base_qs(tenant, "inventory_item").count(),
    }


def tool_domain_overview(tenant) -> dict[str, Any]:
    crm = {}
    for et in CRM_ENTITY_TYPES:
        crm[et] = _base_qs(tenant, et).count()
    erp = {}
    for et in ERP_ENTITY_TYPES:
        erp[et] = _base_qs(tenant, et).count()
    return {"domain": "both", "crm": crm, "erp": erp}


def _status_breakdown(
    tenant, entity_type: str, field: str, top_n: int = 15
) -> list[dict[str, Any]]:
    agg = _aggregate_field_breakdown(tenant, entity_type, field, top_n=top_n)
    if agg.get("error"):
        return []
    return agg["breakdown"]


def tool_analytics_overview(tenant, focus: str = "all") -> dict[str, Any]:
    """Tenant-scoped analytics snapshot (ORM GROUP BY, full table)."""
    focus = (focus or "all").strip().lower()
    if focus not in {"all", "leads", "tickets", "inventory"}:
        focus = "all"

    out: dict[str, Any] = {"domain": "analytics", "focus": focus}

    if focus in {"all", "leads"}:
        by_status = _aggregate_field_breakdown(tenant, "lead", "lead_status", top_n=15)
        by_stage = _aggregate_field_breakdown(tenant, "lead", "lead_stage", top_n=15)
        out["leads"] = {
            "total": by_stage.get("total") or by_status.get("total") or 0,
            "by_lead_status": by_status.get("breakdown") or [],
            "by_stage": by_stage.get("breakdown") or [],
            "complete": True,
        }

    if focus in {"all", "tickets"}:
        ticket_qs = _base_qs(tenant, "support_ticket")
        total = ticket_qs.count()
        resolved = ticket_qs.filter(data__resolution_status__iexact="Resolved").count()
        # common unresolved = everything not Resolved
        unresolved = max(total - resolved, 0)
        by_res = _aggregate_field_breakdown(
            tenant, "support_ticket", "resolution_status", top_n=15
        )
        out["support_tickets"] = {
            "total": total,
            "resolved": resolved,
            "unresolved": unresolved,
            "by_resolution_status": by_res.get("breakdown") or [],
            "complete": True,
        }

    if focus in {"all", "inventory"}:
        by_req = _aggregate_field_breakdown(
            tenant, "inventory_request", "status", top_n=15
        )
        out["inventory"] = {
            "inventory_items": _base_qs(tenant, "inventory_item").count(),
            "inventory_requests": by_req.get("total")
            or _base_qs(tenant, "inventory_request").count(),
            "requests_by_status": by_req.get("breakdown") or [],
            "complete": True,
        }

    return out


def _parse_optional_date(value: str, *, end_of_day: bool = False):
    """Legacy helper: parse YYYY-MM-DD into naive UTC (IST day bounds)."""
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        from datetime import datetime

        from analytics.utils import get_utc_datetime_range_for_ist_date

        if len(raw) == 10:
            d = datetime.strptime(raw, "%Y-%m-%d").date()
            start_dt, end_dt = get_utc_datetime_range_for_ist_date(d)
            return end_dt if end_of_day else start_dt
        # ISO datetimes: store naive UTC-compatible
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except ValueError:
        return None


def _resolve_stats_date_range(
    period: str = "",
    start_date: str = "",
    end_date: str = "",
    *,
    default_period: str = "this_month",
) -> dict[str, Any]:
    """
    Resolve relative period or explicit dates into naive UTC datetime bounds (IST days).

    period: today | yesterday | this_week | last_week | this_month | last_month | all
    Explicit start_date/end_date override period when provided.
    """
    from datetime import datetime, timedelta

    import pytz

    from analytics.utils import get_utc_datetime_range_for_ist_date

    start_raw = (start_date or "").strip()
    end_raw = (end_date or "").strip()
    period_raw = (period or "").strip().lower().replace("-", "_").replace(" ", "_")

    # Explicit dates win.
    if start_raw or end_raw:
        start_dt = _parse_optional_date(start_raw, end_of_day=False) if start_raw else None
        end_dt = _parse_optional_date(end_raw, end_of_day=True) if end_raw else None
        if start_raw and start_dt is None:
            return {"error": f"Invalid start_date: {start_raw}. Use YYYY-MM-DD."}
        if end_raw and end_dt is None:
            return {"error": f"Invalid end_date: {end_raw}. Use YYYY-MM-DD."}
        return {
            "period": "custom",
            "start_date": start_raw or None,
            "end_date": end_raw or None,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "timezone": "Asia/Kolkata",
        }

    if not period_raw:
        period_raw = (default_period or "this_month").strip().lower()

    aliases = {
        "todays": "today",
        "thisweek": "this_week",
        "lastweek": "last_week",
        "thismonth": "this_month",
        "lastmonth": "last_month",
        "mtd": "this_month",
        "wtd": "this_week",
        "all_time": "all",
        "everything": "all",
        "none": "all",
    }
    period_raw = aliases.get(period_raw, period_raw)

    if period_raw in {"all", "any"}:
        return {
            "period": "all",
            "start_date": None,
            "end_date": None,
            "start_dt": None,
            "end_dt": None,
            "timezone": "Asia/Kolkata",
        }

    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).date()

    if period_raw == "today":
        start_d = end_d = today
    elif period_raw == "yesterday":
        start_d = end_d = today - timedelta(days=1)
    elif period_raw == "this_week":
        start_d = today - timedelta(days=today.weekday())  # Monday
        end_d = today
    elif period_raw == "last_week":
        end_d = today - timedelta(days=today.weekday() + 1)  # last Sunday
        start_d = end_d - timedelta(days=6)
    elif period_raw == "this_month":
        start_d = today.replace(day=1)
        end_d = today
    elif period_raw == "last_month":
        first_this = today.replace(day=1)
        end_d = first_this - timedelta(days=1)
        start_d = end_d.replace(day=1)
    else:
        return {
            "error": (
                f"Invalid period: {period}. Use today, yesterday, this_week, "
                "last_week, this_month, last_month, or all."
            )
        }

    start_dt, _ = get_utc_datetime_range_for_ist_date(start_d)
    _, end_dt = get_utc_datetime_range_for_ist_date(end_d)
    return {
        "period": period_raw,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "start_dt": start_dt,
        "end_dt": end_dt,
        "timezone": "Asia/Kolkata",
    }


def tool_lead_trial_activation_stats(
    tenant,
    start_date: str = "",
    end_date: str = "",
    top_n: int = 10,
    period: str = "",
) -> dict[str, Any]:
    """
    Current TRIAL_ACTIVATED lead count + event-log activator leaderboard.
    Event stats default to this_month (IST) unless period/dates override.
    """
    from authz.models import TenantMembership
    from crm_records.models import EventLog

    top_n = max(1, min(int(top_n or 10), 50))
    date_range = _resolve_stats_date_range(
        period, start_date, end_date, default_period="this_month"
    )
    if date_range.get("error"):
        return {"error": date_range["error"]}

    leads_now = _base_qs(tenant, "lead").filter(
        data__lead_stage__iexact="TRIAL_ACTIVATED"
    ).count()

    events = EventLog.objects.filter(tenant=tenant, event="lead.trial_activated")
    if date_range.get("start_dt"):
        events = events.filter(timestamp__gte=date_range["start_dt"])
    if date_range.get("end_dt"):
        events = events.filter(timestamp__lte=date_range["end_dt"])

    event_total = events.count()
    ranked = list(
        events.exclude(payload__user_id__isnull=True)
        .exclude(payload__user_id="")
        .values("payload__user_id")
        .annotate(activations=Count("id"))
        .order_by("-activations")[:top_n]
    )

    user_ids = [str(r["payload__user_id"]) for r in ranked if r.get("payload__user_id")]
    membership_by_uid: dict[str, Any] = {}
    if user_ids:
        for m in TenantMembership.objects.filter(
            tenant=tenant, user_id__in=user_ids, is_active=True
        ).only("user_id", "name", "email"):
            membership_by_uid[str(m.user_id)] = m

    top_activators = []
    for row in ranked:
        uid = str(row.get("payload__user_id") or "")
        m = membership_by_uid.get(uid)
        top_activators.append(
            {
                "user_id": uid,
                "name": (m.name if m else None) or None,
                "email": (m.email if m else None) or None,
                "trial_activations": int(row["activations"]),
            }
        )

    top = top_activators[0] if top_activators else None
    return {
        "domain": "crm",
        "metric": "lead_trial_activation",
        "leads_currently_trial_activated": leads_now,
        "trial_activated_events": event_total,
        "date_filter": {
            "period": date_range.get("period"),
            "start_date": date_range.get("start_date"),
            "end_date": date_range.get("end_date"),
            "timezone": date_range.get("timezone"),
        },
        "top_activator": top,
        "top_activators": top_activators,
        "note": (
            "leads_currently_trial_activated is current lead_stage=TRIAL_ACTIVATED (not date-filtered). "
            "trial_activated_events / top_activators use event_logs (lead.trial_activated) "
            "for the selected period (default this_month, IST)."
        ),
    }


class _ResolutionTimeToSeconds(Func):
    """Parse data.resolution_time 'MM:SS' (or 'M:SS') into seconds."""

    function = "CAST"
    template = (
        "CAST(SPLIT_PART(%(expressions)s, ':', 1) AS INTEGER) * 60 + "
        "CAST(SPLIT_PART(%(expressions)s, ':', 2) AS INTEGER)"
    )
    output_field = IntegerField()


def _convert_seconds(value: Optional[float], unit: str) -> Optional[float]:
    if value is None:
        return None
    unit = (unit or "minutes").lower()
    if unit == "seconds":
        return round(float(value), 2)
    if unit == "hours":
        return round(float(value) / 3600.0, 2)
    return round(float(value) / 60.0, 2)


def _duration_to_seconds(value) -> Optional[float]:
    if value is None:
        return None
    if hasattr(value, "total_seconds"):
        return float(value.total_seconds())
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def tool_support_ticket_resolution_stats(
    tenant,
    start_date: str = "",
    end_date: str = "",
    top_n: int = 10,
    unit: str = "minutes",
    period: str = "",
) -> dict[str, Any]:
    """
    Top CSE resolvers (+ their avg resolution_time & SLA) and RM SLA leaderboard.
    Defaults to this_month (IST) when no period/dates are provided.
    """
    from support_ticket.records import annotate_ticket_datetimes, support_ticket_records_qs

    top_n = max(1, min(int(top_n or 10), 50))
    unit = (unit or "minutes").strip().lower()
    if unit not in {"seconds", "minutes", "hours"}:
        unit = "minutes"

    date_range = _resolve_stats_date_range(
        period, start_date, end_date, default_period="this_month"
    )
    if date_range.get("error"):
        return {"error": date_range["error"]}

    qs = annotate_ticket_datetimes(support_ticket_records_qs(tenant=tenant)).filter(
        data__resolution_status__iexact="Resolved",
        ticket_completed_at__isnull=False,
    )
    if date_range.get("start_dt"):
        qs = qs.filter(ticket_completed_at__gte=date_range["start_dt"])
    if date_range.get("end_dt"):
        qs = qs.filter(ticket_completed_at__lte=date_range["end_dt"])

    total_resolved = qs.count()

    def _rank(group_field: str) -> list[dict[str, Any]]:
        # Rank by all Resolved tickets with a non-empty group label.
        count_rows = {
            r[group_field]: int(r["resolved_count"] or 0)
            for r in (
                qs.exclude(**{f"{group_field}__isnull": True})
                .exclude(**{group_field: ""})
                .values(group_field)
                .annotate(resolved_count=Count("id"))
                .order_by("-resolved_count")[:top_n]
            )
        }
        if not count_rows:
            return []

        # Averages only where resolution_time / completed timestamps are usable.
        timed = (
            qs.exclude(**{f"{group_field}__isnull": True})
            .exclude(**{group_field: ""})
            .exclude(data__resolution_time__isnull=True)
            .exclude(data__resolution_time="")
            .filter(**{f"{group_field}__in": list(count_rows.keys())})
            .annotate(
                resolution_seconds=_ResolutionTimeToSeconds("data__resolution_time"),
                sla_duration=ExpressionWrapper(
                    F("ticket_completed_at") - F("created_at"),
                    output_field=DurationField(),
                ),
            )
            .values(group_field)
            .annotate(
                avg_resolution_seconds=Avg("resolution_seconds"),
                avg_sla=Avg("sla_duration"),
            )
        )
        avg_by_name = {r[group_field]: r for r in timed}

        out_rows = []
        for name, resolved_count in count_rows.items():
            avg_row = avg_by_name.get(name) or {}
            out_rows.append(
                {
                    "name": name,
                    "resolved_count": resolved_count,
                    "average_resolution_time": _convert_seconds(
                        avg_row.get("avg_resolution_seconds"), unit
                    ),
                    "average_sla_time": _convert_seconds(
                        _duration_to_seconds(avg_row.get("avg_sla")), unit
                    ),
                    "unit": unit,
                }
            )
        out_rows.sort(key=lambda x: x["resolved_count"], reverse=True)
        return out_rows

    cse_board = _rank("data__cse_name")
    rm_board = _rank("data__rm_name")

    return {
        "domain": "crm",
        "metric": "support_ticket_resolution",
        "total_resolved_tickets": total_resolved,
        "date_filter": {
            "period": date_range.get("period"),
            "start_date": date_range.get("start_date"),
            "end_date": date_range.get("end_date"),
            "timezone": date_range.get("timezone"),
        },
        "unit": unit,
        "top_resolver": cse_board[0] if cse_board else None,
        "top_resolvers_by_cse": cse_board,
        "rm_sla_leaderboard": rm_board,
        "note": (
            "Resolvers ranked by data.cse_name on tickets with resolution_status=Resolved. "
            "average_resolution_time uses data.resolution_time (MM:SS first-contact style). "
            "average_sla_time is completed_at - created_at. "
            "RM board groups the same resolved tickets by data.rm_name. "
            "Date window defaults to this_month (IST) unless period/dates are set."
        ),
    }


TOOL_HANDLERS = {
    "count_records": lambda tenant, args: tool_count_records(
        tenant, args.get("entity_type", ""), args.get("status", "")
    ),
    "search_records": lambda tenant, args: tool_search_records(
        tenant,
        args.get("entity_type", ""),
        args.get("query", ""),
        args.get("limit", SEARCH_RESULT_LIMIT),
    ),
    "summarize_entity_breakdown": lambda tenant, args: tool_summarize_entity_breakdown(
        tenant, args.get("entity_type", ""), args.get("field", "")
    ),
    "inventory_stock_summary": lambda tenant, args: tool_inventory_stock_summary(
        tenant, args.get("sku", ""), args.get("limit", 10)
    ),
    "domain_overview": lambda tenant, args: tool_domain_overview(tenant),
    "analytics_overview": lambda tenant, args: tool_analytics_overview(
        tenant, args.get("focus", "all")
    ),
    "lead_trial_activation_stats": lambda tenant, args: tool_lead_trial_activation_stats(
        tenant,
        args.get("start_date", ""),
        args.get("end_date", ""),
        args.get("top_n", 10),
        args.get("period", ""),
    ),
    "support_ticket_resolution_stats": lambda tenant, args: tool_support_ticket_resolution_stats(
        tenant,
        args.get("start_date", ""),
        args.get("end_date", ""),
        args.get("top_n", 10),
        args.get("unit", "minutes"),
        args.get("period", ""),
    ),
    **ACTION_TOOL_HANDLERS,
}


def run_tool(
    name: str,
    tenant,
    arguments: dict[str, Any] | str,
    *,
    user_id=None,
) -> dict[str, Any]:
    import inspect

    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments or "{}")
        except json.JSONDecodeError:
            arguments = {}
    handler = TOOL_HANDLERS.get(name)
    if not handler:
        return {"error": f"Unknown tool: {name}"}
    if tenant is None:
        return {"error": "Tenant is required for data tools"}
    try:
        sig = inspect.signature(handler)
        if "user_id" in sig.parameters:
            return handler(tenant, arguments or {}, user_id=user_id)
        return handler(tenant, arguments or {})
    except Exception as exc:
        return {"error": f"Tool {name} failed: {exc}"}


def tools_to_openai() -> list[dict[str, Any]]:
    return TOOL_DEFINITIONS
