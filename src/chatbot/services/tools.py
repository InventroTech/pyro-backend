"""Tenant-scoped CRM + ERP tools the chat LLM can call."""

from __future__ import annotations

import json
from typing import Any, Optional

from django.db.models import Q

from chatbot.constants import (
    ALLOWED_ENTITY_TYPES,
    CRM_ENTITY_TYPES,
    ERP_ENTITY_TYPES,
    SEARCH_RESULT_LIMIT,
)
from chatbot.services.action_tools import ACTION_TOOL_DEFINITIONS, ACTION_TOOL_HANDLERS
from crm_records.models import Record


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
                "Return counts grouped by a status-like field for a CRM/ERP entity_type "
                "(e.g. lead_status, resolution_status, status)."
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
                "dashboard stats, reports, SLA overview, or CSE/RM metrics summary."
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
    # Pull a bounded sample and aggregate in Python (JSONB group-by varies by Postgres version/setup).
    qs = _base_qs(tenant, entity_type).only("id", "data")[:5000]
    counts: dict[str, int] = {}
    total = 0
    for rec in qs.iterator(chunk_size=500):
        total += 1
        data = rec.data if isinstance(rec.data, dict) else {}
        key = str(data.get(field) or "(empty)")
        counts[key] = counts.get(key, 0) + 1
    breakdown = sorted(
        [{"value": k, "count": v} for k, v in counts.items()],
        key=lambda x: x["count"],
        reverse=True,
    )
    return {
        "entity_type": entity_type,
        "field": field,
        "total_sampled": total,
        "breakdown": breakdown[:30],
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


def _status_breakdown(tenant, entity_type: str, field: str, sample_limit: int = 5000) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    qs = _base_qs(tenant, entity_type).only("id", "data")[:sample_limit]
    for rec in qs.iterator(chunk_size=500):
        data = rec.data if isinstance(rec.data, dict) else {}
        key = str(data.get(field) or "(empty)")
        counts[key] = counts.get(key, 0) + 1
    return sorted(
        [{"value": k, "count": v} for k, v in counts.items()],
        key=lambda x: x["count"],
        reverse=True,
    )[:15]


def tool_analytics_overview(tenant, focus: str = "all") -> dict[str, Any]:
    """Tenant-scoped analytics snapshot (ORM, no raw SQL)."""
    focus = (focus or "all").strip().lower()
    if focus not in {"all", "leads", "tickets", "inventory"}:
        focus = "all"

    out: dict[str, Any] = {"domain": "analytics", "focus": focus}

    if focus in {"all", "leads"}:
        lead_total = _base_qs(tenant, "lead").count()
        out["leads"] = {
            "total": lead_total,
            "by_lead_status": _status_breakdown(tenant, "lead", "lead_status"),
            "by_stage": _status_breakdown(tenant, "lead", "lead_stage"),
        }

    if focus in {"all", "tickets"}:
        ticket_qs = _base_qs(tenant, "support_ticket")
        total = ticket_qs.count()
        resolved = ticket_qs.filter(data__resolution_status__iexact="Resolved").count()
        # common unresolved = everything not Resolved
        unresolved = max(total - resolved, 0)
        out["support_tickets"] = {
            "total": total,
            "resolved": resolved,
            "unresolved": unresolved,
            "by_resolution_status": _status_breakdown(
                tenant, "support_ticket", "resolution_status"
            ),
        }

    if focus in {"all", "inventory"}:
        out["inventory"] = {
            "inventory_items": _base_qs(tenant, "inventory_item").count(),
            "inventory_requests": _base_qs(tenant, "inventory_request").count(),
            "requests_by_status": _status_breakdown(
                tenant, "inventory_request", "status"
            ),
        }

    return out


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
