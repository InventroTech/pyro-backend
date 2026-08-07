"""
Action tools for bob-style operations: billing report, background jobs, pyro jobs.

Mutating job enqueues require confirm=true.
"""

from __future__ import annotations

import json
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

from django.db.models import Q

# Job types the chatbot must never enqueue
BLOCKED_BACKGROUND_JOBS = frozenset({"execute_function", "queue_function"})
BLOCKED_PYRO_JOBS = frozenset()

# Especially dangerous — still allowed only with confirm=true
DANGEROUS_BACKGROUND_JOBS = frozenset(
    {
        "purge_old_log_tables",
        "unassign_snoozed_leads",
        "release_leads_after_12h",
        "close_stale_self_trial_support_tickets",
        "snoozed_to_not_connected_midnight",
        "send_webhook",
        "send_to_praja",
        "partner_lead_assign",
        "sync_dispatch_to_records",
        "process_dumped_tickets",
    }
)
DANGEROUS_PYRO_JOBS = frozenset(
    {
        "purge_old_log_tables",
        "dispatch_data_sync",
        "snoozed_to_not_connected_midnight",
    }
)


ACTION_TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_billing_report",
            "description": (
                "Generate the tenant membership billing report for a month "
                "(same as bob Billing page). Read-only. "
                "month format YYYY-MM; defaults to current month."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "month": {
                        "type": "string",
                        "description": "Billing month YYYY-MM",
                    },
                    "include_members": {
                        "type": "boolean",
                        "description": "If true, include per-member rows (can be long). Default false = summary only.",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_background_job_types",
            "description": "List runnable background job types (bob Background Jobs page).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "enqueue_background_job",
            "description": (
                "Enqueue a background job for this tenant (bob Background Jobs). "
                "REQUIRES confirm=true. Dangerous types (purge, unassign, webhook, etc.) "
                "also require confirm=true. Do not invent payloads — ask the user if unsure."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "job_type": {"type": "string"},
                    "payload": {"type": "object"},
                    "priority": {"type": "integer", "default": 0},
                    "max_attempts": {"type": "integer", "default": 3},
                    "confirm": {
                        "type": "boolean",
                        "description": "Must be true to actually enqueue",
                    },
                },
                "required": ["job_type", "confirm"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_background_job_status",
            "description": "Get status of a background job by numeric id.",
            "parameters": {
                "type": "object",
                "properties": {"job_id": {"type": "integer"}},
                "required": ["job_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_pyro_job_types",
            "description": "List runnable pyro job names (bob Pyro Jobs page).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "enqueue_pyro_job",
            "description": (
                "Enqueue a pyro job (bob Pyro Jobs). REQUIRES confirm=true. "
                "Note: enqueue may also run any existing PENDING job of the same name. "
                "Dangerous: purge_old_log_tables, dispatch_data_sync."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "job_name": {"type": "string"},
                    "payload": {"type": "object"},
                    "max_attempts": {"type": "integer", "default": 3},
                    "confirm": {
                        "type": "boolean",
                        "description": "Must be true to actually enqueue",
                    },
                },
                "required": ["job_name", "confirm"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pyro_job_status",
            "description": "Get status of a pyro job by numeric id.",
            "parameters": {
                "type": "object",
                "properties": {"job_id": {"type": "integer"}},
                "required": ["job_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_my_pages",
            "description": (
                "List dashboard pages owned by the current user (bob My Pages). "
                "Returns id, name, icon, header_title, updated_at."
            ),
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_page",
            "description": (
                "Create a new dashboard page under the tenant's configured chatbot page owner "
                "(TenantSettings.chatbot_page_owner_email). Any GM/user can trigger create; "
                "ownership is always that configured account, not the requester. "
                "REQUIRES confirm=true. config defaults to []. "
                "role may be a role UUID or role key (e.g. RM, CSE) for visibility; optional."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Page name (required)"},
                    "header_title": {"type": "string"},
                    "icon_name": {
                        "type": "string",
                        "description": "Lucide/nav icon key, default Sparkles",
                    },
                    "display_order": {"type": "integer", "default": 0},
                    "role": {
                        "type": "string",
                        "description": "Optional role UUID or role key for page visibility",
                    },
                    "config": {
                        "type": "array",
                        "description": "Optional widget config list; default []",
                    },
                    "confirm": {
                        "type": "boolean",
                        "description": "Must be true to create the page",
                    },
                },
                "required": ["name", "confirm"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_page",
            "description": (
                "Update an existing dashboard page owned by the configured chatbot page owner. "
                "Use this to add widgets (e.g. lead table) to a page like Ops Home. "
                "Identify the page with page_id and/or page_name. "
                "action=add_widget with widget_type=leadTable (or lead_table) appends a lead table. "
                "action=set_config replaces the full widget config list. "
                "REQUIRES confirm=true."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "page_id": {
                        "type": "string",
                        "description": "Page UUID if known",
                    },
                    "page_name": {
                        "type": "string",
                        "description": "Page name lookup (e.g. Ops Home) when page_id omitted",
                    },
                    "action": {
                        "type": "string",
                        "enum": ["add_widget", "set_config"],
                        "description": "add_widget appends a template widget; set_config replaces config",
                    },
                    "widget_type": {
                        "type": "string",
                        "description": (
                            "For add_widget: leadTable / lead_table / ticketTable / "
                            "inventoryTable (templates supported)"
                        ),
                    },
                    "api_endpoint": {
                        "type": "string",
                        "description": (
                            "Optional override for table API endpoint. "
                            "Default for leadTable: /crm-records/records/?entity_type=lead"
                        ),
                    },
                    "config": {
                        "type": "array",
                        "description": "Required for action=set_config: full widget list",
                    },
                    "confirm": {
                        "type": "boolean",
                        "description": "Must be true to apply the update",
                    },
                },
                "required": ["action", "confirm"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delete_page",
            "description": (
                "Delete (soft-delete) an existing dashboard page owned by the configured "
                "chatbot page owner. page_name is REQUIRED and must match the page exactly. "
                "Optional page_id helps when multiple pages share a name. "
                "Never delete a different page than the name the user asked for. "
                "REQUIRES confirm=true."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "page_id": {
                        "type": "string",
                        "description": "Optional page UUID (must match page_name if both set)",
                    },
                    "page_name": {
                        "type": "string",
                        "description": "Exact page name to delete (required), e.g. Ops Home",
                    },
                    "confirm": {
                        "type": "boolean",
                        "description": "Must be true to delete the page",
                    },
                },
                "required": ["page_name", "confirm"],
            },
        },
    },
]


def tool_get_billing_report(
    tenant,
    month: str = "",
    include_members: bool = False,
) -> dict[str, Any]:
    from authz.models import TenantMembership
    from authz.views_management import (
        INTERNAL_BILLING_EMAIL_DOMAIN,
        _billing_period_end,
        _current_billing_month,
        _default_rate_for_role,
        _internal_billing_email_q,
        _membership_billing_end,
        _parse_billing_month,
        _parse_cycle_days,
        _tenant_billing_roles,
        calculate_membership_billing,
        get_membership_monthly_amount,
    )

    try:
        billing_month = _parse_billing_month(month or None)
        if billing_month > _current_billing_month():
            return {"error": "Cannot calculate billing for a future month"}
        cycle_days = _parse_cycle_days(None, billing_month)
    except ValueError as exc:
        return {"error": str(exc)}

    billing_roles = _tenant_billing_roles(tenant)
    role_rates = {str(role.id): _default_rate_for_role(role) for role in billing_roles}
    period_end = _billing_period_end(billing_month)
    internal_email_query = _internal_billing_email_q()

    base_memberships = (
        TenantMembership.all_objects.select_related("role")
        .filter(tenant=tenant)
        .filter(created_at__date__lte=period_end)
        .filter(Q(deleted_at__isnull=True) | Q(deleted_at__date__gte=billing_month))
    )
    excluded_internal = base_memberships.filter(internal_email_query).count()
    memberships = list(
        base_memberships.exclude(internal_email_query).order_by("created_at", "email")
    )

    rows = []
    total_amount = Decimal("0.00")
    total_billable_days = 0
    for membership in memberships:
        billing_role_key, monthly_amount = get_membership_monthly_amount(
            membership, role_rates
        )
        membership_period_end = _membership_billing_end(membership, period_end)
        billable_days, amount = calculate_membership_billing(
            membership.created_at,
            billing_month,
            monthly_amount,
            cycle_days,
            membership_period_end,
        )
        total_billable_days += billable_days
        total_amount += amount
        if include_members:
            rows.append(
                {
                    "name": membership.name or "",
                    "email": membership.email,
                    "role": membership.role.key if membership.role_id else None,
                    "billable_days": billable_days,
                    "billing_amount": str(amount),
                    "billing_role_key": billing_role_key,
                }
            )

    out: dict[str, Any] = {
        "month": billing_month.strftime("%Y-%m"),
        "period_start": billing_month.isoformat(),
        "period_end": period_end.isoformat(),
        "cycle_days": cycle_days,
        "excluded_email_domain": INTERNAL_BILLING_EMAIL_DOMAIN,
        "excluded_internal_member_count": excluded_internal,
        "summary": {
            "member_count": len(memberships),
            "total_billable_days": total_billable_days,
            "total_amount": str(
                total_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            ),
        },
    }
    if include_members:
        out["results"] = rows[:100]
        if len(rows) > 100:
            out["results_truncated"] = True
    return out


def tool_list_background_job_types(tenant) -> dict[str, Any]:
    from background_jobs.queue_service import get_queue_service

    types = [
        t
        for t in get_queue_service().list_job_types()
        if t not in BLOCKED_BACKGROUND_JOBS
    ]
    return {
        "job_types": types,
        "dangerous": sorted(DANGEROUS_BACKGROUND_JOBS & set(types)),
        "note": "Enqueue requires confirm=true",
    }


def tool_enqueue_background_job(
    tenant,
    job_type: str,
    payload: Optional[dict] = None,
    priority: int = 0,
    max_attempts: int = 3,
    confirm: bool = False,
) -> dict[str, Any]:
    from background_jobs.queue_service import get_queue_service

    job_type = (job_type or "").strip()
    if not job_type:
        return {"error": "job_type is required"}
    if job_type in BLOCKED_BACKGROUND_JOBS:
        return {"error": f"Job type {job_type} is blocked from chatbot"}
    if not confirm:
        return {
            "error": "confirm_required",
            "message": (
                f"Refusing to enqueue {job_type} without confirm=true. "
                "Ask the user to confirm, then call again with confirm=true."
            ),
            "job_type": job_type,
            "dangerous": job_type in DANGEROUS_BACKGROUND_JOBS,
        }

    payload = payload if isinstance(payload, dict) else {}
    tenant_id = str(getattr(tenant, "id", tenant))
    try:
        job = get_queue_service().enqueue_job(
            job_type=job_type,
            payload=payload,
            priority=int(priority or 0),
            tenant_id=tenant_id,
            max_attempts=int(max_attempts or 3),
        )
    except Exception as exc:
        return {"error": str(exc)}

    return {
        "enqueued": True,
        "id": job.id,
        "job_type": job.job_type,
        "status": job.status,
        "tenant_id": tenant_id,
        "dangerous": job_type in DANGEROUS_BACKGROUND_JOBS,
    }


def tool_get_background_job_status(tenant, job_id: int) -> dict[str, Any]:
    from background_jobs.queue_service import get_queue_service
    from background_jobs.models import BackgroundJob

    try:
        status = get_queue_service().get_job_status(int(job_id))
    except BackgroundJob.DoesNotExist:
        return {"error": f"Background job {job_id} not found"}
    except Exception as exc:
        return {"error": str(exc)}

    # Soft tenant check when job has tenant_id
    job_tenant = status.get("tenant_id")
    if job_tenant and str(getattr(tenant, "id", "")) != str(job_tenant):
        return {"error": "Job belongs to another tenant"}
    return status


def tool_list_pyro_job_types(tenant) -> dict[str, Any]:
    from pyro_jobs.queue_service import get_pyro_queue_service

    types = get_pyro_queue_service().list_job_types()
    return {
        "job_types": types,
        "dangerous": sorted(DANGEROUS_PYRO_JOBS & set(types)),
        "note": "Enqueue requires confirm=true; may run existing PENDING jobs first",
    }


def tool_enqueue_pyro_job(
    tenant,
    job_name: str,
    payload: Optional[dict] = None,
    max_attempts: int = 3,
    confirm: bool = False,
) -> dict[str, Any]:
    from pyro_jobs.queue_service import get_pyro_queue_service

    job_name = (job_name or "").strip()
    if not job_name:
        return {"error": "job_name is required"}
    if job_name in BLOCKED_PYRO_JOBS:
        return {"error": f"Job {job_name} is blocked from chatbot"}
    if not confirm:
        return {
            "error": "confirm_required",
            "message": (
                f"Refusing to enqueue pyro job {job_name} without confirm=true. "
                "Ask the user to confirm, then call again with confirm=true."
            ),
            "job_name": job_name,
            "dangerous": job_name in DANGEROUS_PYRO_JOBS,
        }

    payload = dict(payload) if isinstance(payload, dict) else {}
    if "tenant_id" not in payload:
        payload["tenant_id"] = str(getattr(tenant, "id", tenant))

    try:
        job, ran_pending = get_pyro_queue_service().enqueue_job(
            job_name=job_name,
            payload=payload,
            max_attempts=int(max_attempts or 3),
        )
    except Exception as exc:
        return {"error": str(exc)}

    return {
        "enqueued": True,
        "id": job.id,
        "job_name": job.job_name,
        "status": job.status,
        "ran_pending_jobs": ran_pending,
        "dangerous": job_name in DANGEROUS_PYRO_JOBS,
    }


def tool_get_pyro_job_status(tenant, job_id: int) -> dict[str, Any]:
    from pyro_jobs.queue_service import get_pyro_queue_service
    from pyro_jobs.models import PyroJob

    try:
        return get_pyro_queue_service().get_job_status(int(job_id))
    except PyroJob.DoesNotExist:
        return {"error": f"Pyro job {job_id} not found"}
    except Exception as exc:
        return {"error": str(exc)}


def _resolve_role_id(tenant, role_value: str):
    """Accept role UUID or role key; return role UUID or None."""
    from authz.models import Role
    from uuid import UUID

    raw = (role_value or "").strip()
    if not raw:
        return None
    try:
        rid = UUID(raw)
        if Role.objects.filter(id=rid, tenant=tenant).exists():
            return rid
        return None
    except (ValueError, TypeError):
        pass
    role = (
        Role.objects.filter(tenant=tenant, key__iexact=raw).first()
        or Role.objects.filter(tenant=tenant, name__iexact=raw).first()
    )
    return role.id if role else None


def resolve_chatbot_page_owner(tenant) -> tuple[Any, Optional[str], Optional[str]]:
    """
    Resolve the canonical page owner for chatbot-created pages.

    Returns (user_id, owner_email, error_message).
    Config order:
      1. TenantSettings.chatbot_page_owner_email
      2. env CHATBOT_PAGE_OWNER_EMAIL
    Then looks up an active TenantMembership with that email + user_id.
    """
    import os
    from core.models import TenantSettings
    from authz.models import TenantMembership

    email = ""
    settings_row = TenantSettings.objects.filter(tenant=tenant).first()
    if settings_row and settings_row.chatbot_page_owner_email:
        email = settings_row.chatbot_page_owner_email.strip().lower()
    if not email:
        email = (os.getenv("CHATBOT_PAGE_OWNER_EMAIL") or "").strip().lower()
    if not email:
        return (
            None,
            None,
            "No chatbot page owner configured. Set TenantSettings.chatbot_page_owner_email "
            "for this tenant (or CHATBOT_PAGE_OWNER_EMAIL in env).",
        )

    membership = (
        TenantMembership.objects.filter(
            tenant=tenant,
            email__iexact=email,
            is_active=True,
            user_id__isnull=False,
        )
        .order_by("-created_at")
        .first()
    )
    if not membership:
        return (
            None,
            email,
            f"No active membership with a linked user_id for page owner email {email} "
            f"in this tenant.",
        )
    return membership.user_id, email, None


def tool_list_my_pages(tenant, user_id=None) -> dict[str, Any]:
    """List pages owned by the configured chatbot page owner (not the requester)."""
    from pages.models import Page

    owner_id, owner_email, err = resolve_chatbot_page_owner(tenant)
    if err:
        return {"error": err}
    pages = list(
        Page.objects.filter(tenant=tenant, user_id=owner_id)
        .order_by("display_order", "-updated_at")[:50]
    )
    return {
        "count": len(pages),
        "page_owner_email": owner_email,
        "page_owner_user_id": str(owner_id),
        "pages": [
            {
                "id": str(p.id),
                "name": p.name,
                "header_title": p.header_title,
                "icon_name": p.icon_name,
                "display_order": p.display_order,
                "role_id": str(p.role_id) if p.role_id else None,
                "updated_at": p.updated_at.isoformat() if p.updated_at else None,
            }
            for p in pages
        ],
    }


def tool_create_page(
    tenant,
    name: str = "",
    header_title: str = "",
    icon_name: str = "Sparkles",
    display_order: int = 0,
    role: str = "",
    config=None,
    confirm: bool = False,
    user_id=None,
) -> dict[str, Any]:
    """
    Create a Page under the tenant's configured chatbot page owner.

    The requesting chat user may trigger creation, but ``Page.user_id`` is always
    the configured owner membership (TenantSettings / env) — never the requester
    and never LLM-supplied.
    """
    from pages.models import Page

    name = (name or "").strip()
    if not name:
        return {"error": "name is required"}

    owner_id, owner_email, err = resolve_chatbot_page_owner(tenant)
    if err:
        return {"error": err}

    if not confirm:
        return {
            "error": "confirm_required",
            "message": (
                f'Refusing to create page "{name}" without confirm=true. '
                "Ask the user to confirm, then call again with confirm=true."
            ),
            "preview": {
                "name": name,
                "header_title": header_title or name,
                "icon_name": icon_name or "Sparkles",
                "display_order": int(display_order or 0),
                "role": role or None,
                "page_owner_email": owner_email,
                "page_owner_user_id": str(owner_id),
                "requested_by_user_id": str(user_id) if user_id else None,
            },
        }

    role_id = _resolve_role_id(tenant, role) if role else None
    if role and role_id is None:
        return {"error": f"Role not found in this tenant: {role}"}

    if config is None:
        config = []
    if not isinstance(config, list):
        return {"error": "config must be a list of widget configs"}

    page = Page.objects.create(
        tenant=tenant,
        user_id=owner_id,
        name=name[:255],
        header_title=(header_title or name)[:255],
        icon_name=(icon_name or "Sparkles")[:100],
        display_order=int(display_order or 0),
        role_id=role_id,
        config=config,
    )
    return {
        "created": True,
        "id": str(page.id),
        "name": page.name,
        "header_title": page.header_title,
        "icon_name": page.icon_name,
        "display_order": page.display_order,
        "role_id": str(page.role_id) if page.role_id else None,
        "page_owner_email": owner_email,
        "page_owner_user_id": str(page.user_id),
        "requested_by_user_id": str(user_id) if user_id else None,
        "message": (
            f"Page created under configured owner {owner_email}. "
            "Open it in the page builder to add widgets."
        ),
    }


def _default_lead_table_widget(api_endpoint: str = "") -> dict[str, Any]:
    import time

    endpoint = (api_endpoint or "").strip() or (
        "/crm-records/records/?entity_type=lead"
    )
    return {
        "id": f"leadTable-{int(time.time() * 1000)}",
        "type": "leadTable",
        "props": {},
        "config": {
            "columns": [
                {"key": "name", "type": "text", "label": "Name"},
                {"key": "praja_id", "type": "text", "label": "Praja ID"},
                {"key": "phone_number", "type": "number", "label": "Phone No"},
                {"key": "lead_stage", "type": "chip", "label": "Stage"},
                {"key": "affiliated_party", "type": "text", "label": "Party"},
                {"key": "lead_score", "type": "number", "label": "Lead Score"},
                {
                    "key": "assigned_to_display",
                    "type": "text",
                    "label": "Assigned To",
                },
                {
                    "key": "",
                    "type": "action",
                    "label": "View Profile",
                    "openCard": "true",
                },
            ],
            "filters": [
                {
                    "key": "lead_stage",
                    "type": "select",
                    "label": "Stage",
                    "options": [
                        {"label": "Trial Activated", "value": "TRIAL_ACTIVATED"},
                        {"label": "Closed", "value": "CLOSED"},
                        {"label": "Not Interested", "value": "NOT_INTERESTED"},
                        {"label": "Not Connected", "value": "NOT_CONNECTED"},
                        {"label": "Call Later", "value": "SNOOZED"},
                        {"label": "Assigned", "value": "ASSIGNED"},
                        {"label": "Fresh", "value": "FRESH"},
                    ],
                    "accessor": "lead_stage",
                },
                {
                    "key": "assigned_to",
                    "type": "select",
                    "label": "Assigned To",
                    "options": [],
                    "accessor": "assigned_to",
                    "optionsApiUrl": "/membership/users",
                    "optionsValueKey": "user_id",
                    "optionsNullLabel": "NULL",
                    "optionsNullValue": "null",
                    "optionsDisplayKey": "name",
                    "optionsIncludeNull": True,
                },
            ],
            "apiEndpoint": endpoint,
        },
    }


def _default_ticket_table_widget(api_endpoint: str = "") -> dict[str, Any]:
    import time

    endpoint = (api_endpoint or "").strip() or (
        "/crm-records/records/?entity_type=support_ticket"
    )
    return {
        "id": f"ticketTable-{int(time.time() * 1000)}",
        "type": "ticketTable",
        "props": {},
        "config": {
            "columns": [
                {"key": "ticket_id", "type": "text", "label": "Ticket"},
                {"key": "status", "type": "chip", "label": "Status"},
                {"key": "priority", "type": "text", "label": "Priority"},
                {"key": "assigned_to_display", "type": "text", "label": "Assigned To"},
            ],
            "apiEndpoint": endpoint,
        },
    }


def _default_inventory_table_widget(api_endpoint: str = "") -> dict[str, Any]:
    import time

    endpoint = (api_endpoint or "").strip() or (
        "/crm-records/records/?entity_type=inventory_item"
    )
    return {
        "id": f"inventoryTable-{int(time.time() * 1000)}",
        "type": "inventoryTable",
        "props": {},
        "config": {
            "columns": [
                {"key": "sku", "type": "text", "label": "SKU"},
                {"key": "name", "type": "text", "label": "Name"},
                {"key": "available_quantity", "type": "number", "label": "Available"},
            ],
            "apiEndpoint": endpoint,
        },
    }


WIDGET_BUILDERS = {
    "leadtable": _default_lead_table_widget,
    "lead_table": _default_lead_table_widget,
    "lead": _default_lead_table_widget,
    "tickettable": _default_ticket_table_widget,
    "ticket_table": _default_ticket_table_widget,
    "inventorytable": _default_inventory_table_widget,
    "inventory_table": _default_inventory_table_widget,
}


def _resolve_owner_page(tenant, page_id: str = "", page_name: str = ""):
    """Resolve a page under the configured chatbot owner. Returns (page, error)."""
    from pages.models import Page
    from uuid import UUID

    owner_id, owner_email, err = resolve_chatbot_page_owner(tenant)
    if err:
        return None, err

    qs = Page.objects.filter(tenant=tenant, user_id=owner_id)
    raw_id = (page_id or "").strip()
    raw_name = (page_name or "").strip()

    if raw_id:
        try:
            pid = UUID(raw_id)
        except (ValueError, TypeError):
            return None, f"Invalid page_id: {raw_id}"
        page = qs.filter(id=pid).first()
        if not page:
            return (
                None,
                f"Page {raw_id} not found under chatbot owner {owner_email}.",
            )
        return page, None

    if raw_name:
        matches = list(qs.filter(name__iexact=raw_name).order_by("-updated_at")[:5])
        if not matches:
            return (
                None,
                f'No page named "{raw_name}" under chatbot owner {owner_email}.',
            )
        if len(matches) > 1:
            return {
                "error": "ambiguous_page",
                "message": f'Multiple pages named "{raw_name}". Pass page_id.',
                "matches": [
                    {"id": str(p.id), "name": p.name, "updated_at": p.updated_at.isoformat() if p.updated_at else None}
                    for p in matches
                ],
            }, "ambiguous"
        return matches[0], None

    return None, "Provide page_id or page_name"


def tool_update_page(
    tenant,
    page_id: str = "",
    page_name: str = "",
    action: str = "add_widget",
    widget_type: str = "",
    api_endpoint: str = "",
    config=None,
    confirm: bool = False,
    user_id=None,
) -> dict[str, Any]:
    """
    Update page widgets for a page owned by the configured chatbot page owner.
    """
    action = (action or "add_widget").strip().lower()
    if action not in {"add_widget", "set_config"}:
        return {"error": f"Unsupported action: {action}. Use add_widget or set_config."}

    page, err = _resolve_owner_page(tenant, page_id=page_id, page_name=page_name)
    if err == "ambiguous":
        return page  # page holds the error payload dict
    if err:
        return {"error": err}

    owner_id, owner_email, _ = resolve_chatbot_page_owner(tenant)
    current_config = list(page.config or []) if isinstance(page.config, list) else []

    new_widget = None
    next_config = current_config
    if action == "add_widget":
        key = (widget_type or "").strip().lower().replace(" ", "")
        builder = WIDGET_BUILDERS.get(key)
        if not builder:
            return {
                "error": (
                    f"Unknown widget_type: {widget_type}. "
                    "Supported: leadTable, ticketTable, inventoryTable"
                )
            }
        new_widget = builder(api_endpoint=api_endpoint)
        # Avoid duplicate identical type spam unless user wants another
        already = [w for w in current_config if isinstance(w, dict) and w.get("type") == new_widget["type"]]
        next_config = current_config + [new_widget]
        preview_note = (
            f"Append {new_widget['type']} widget"
            + (f" (page already has {len(already)})" if already else "")
        )
    else:
        if config is None:
            return {"error": "config array is required for action=set_config"}
        if not isinstance(config, list):
            return {"error": "config must be a list of widget configs"}
        next_config = config
        preview_note = f"Replace config with {len(next_config)} widget(s)"

    if not confirm:
        return {
            "error": "confirm_required",
            "message": (
                f'Refusing to update page "{page.name}" without confirm=true. '
                "Ask the user to confirm, then call again with confirm=true."
            ),
            "preview": {
                "page_id": str(page.id),
                "page_name": page.name,
                "action": action,
                "widget_type": (new_widget or {}).get("type") if new_widget else None,
                "change": preview_note,
                "current_widget_count": len(current_config),
                "next_widget_count": len(next_config),
                "page_owner_email": owner_email,
                "page_owner_user_id": str(owner_id),
                "requested_by_user_id": str(user_id) if user_id else None,
            },
        }

    page.config = next_config
    page.save(update_fields=["config", "updated_at"])
    return {
        "updated": True,
        "id": str(page.id),
        "name": page.name,
        "action": action,
        "widget_type": (new_widget or {}).get("type") if new_widget else None,
        "widget_count": len(next_config),
        "added_widget_id": (new_widget or {}).get("id") if new_widget else None,
        "page_owner_email": owner_email,
        "page_owner_user_id": str(page.user_id),
        "requested_by_user_id": str(user_id) if user_id else None,
        "message": (
            f'Updated page "{page.name}". Refresh the page builder / My Pages to see widgets.'
        ),
    }


def tool_delete_page(
    tenant,
    page_id: str = "",
    page_name: str = "",
    confirm: bool = False,
    user_id=None,
) -> dict[str, Any]:
    """Soft-delete a page owned by the configured chatbot page owner."""
    wanted_name = (page_name or "").strip()
    if not wanted_name:
        return {
            "error": "page_name is required so the correct page is deleted (never delete by id alone)."
        }

    raw_id = (page_id or "").strip()

    # Resolve by name first (avoids LLM picking an unrelated page_id).
    page, err = _resolve_owner_page(tenant, page_id="", page_name=wanted_name)
    if err == "ambiguous":
        payload = page if isinstance(page, dict) else {}
        matches = payload.get("matches") or []
        if raw_id and any(str(m.get("id")) == raw_id for m in matches):
            page, err = _resolve_owner_page(tenant, page_id=raw_id, page_name="")
        else:
            return payload
    if err:
        return {"error": err}

    if page.name.strip().lower() != wanted_name.lower():
        return {
            "error": (
                f'Refusing delete: resolved page is "{page.name}", '
                f'but requested page_name is "{wanted_name}".'
            )
        }

    # Extra guard if the model also sent a page_id.
    if raw_id and str(page.id) != raw_id:
        return {
            "error": (
                f'Refusing delete: page_id {raw_id} does not match '
                f'"{page.name}" ({page.id}). Use the id for "{wanted_name}" only.'
            )
        }

    owner_id, owner_email, _ = resolve_chatbot_page_owner(tenant)

    if not confirm:
        return {
            "error": "confirm_required",
            "message": (
                f'Refusing to delete page "{page.name}" without confirm=true. '
                "Ask the user to confirm, then call again with confirm=true and the same page_name."
            ),
            "preview": {
                "page_id": str(page.id),
                "page_name": page.name,
                "action": "delete",
                "change": f'Delete page "{page.name}"',
                "page_owner_email": owner_email,
                "page_owner_user_id": str(owner_id),
                "requested_by_user_id": str(user_id) if user_id else None,
            },
        }

    deleted_id = str(page.id)
    deleted_name = page.name
    page.delete()
    return {
        "deleted": True,
        "id": deleted_id,
        "name": deleted_name,
        "page_owner_email": owner_email,
        "page_owner_user_id": str(owner_id),
        "requested_by_user_id": str(user_id) if user_id else None,
        "message": (
            f'Page "{deleted_name}" deleted. Refresh My Pages / the app nav to see it gone.'
        ),
    }


ACTION_TOOL_HANDLERS = {
    "get_billing_report": lambda tenant, args, user_id=None: tool_get_billing_report(
        tenant,
        args.get("month", ""),
        bool(args.get("include_members", False)),
    ),
    "list_background_job_types": lambda tenant, args, user_id=None: tool_list_background_job_types(
        tenant
    ),
    "enqueue_background_job": lambda tenant, args, user_id=None: tool_enqueue_background_job(
        tenant,
        args.get("job_type", ""),
        args.get("payload"),
        args.get("priority", 0),
        args.get("max_attempts", 3),
        bool(args.get("confirm", False)),
    ),
    "get_background_job_status": lambda tenant, args, user_id=None: tool_get_background_job_status(
        tenant, args.get("job_id")
    ),
    "list_pyro_job_types": lambda tenant, args, user_id=None: tool_list_pyro_job_types(tenant),
    "enqueue_pyro_job": lambda tenant, args, user_id=None: tool_enqueue_pyro_job(
        tenant,
        args.get("job_name", ""),
        args.get("payload"),
        args.get("max_attempts", 3),
        bool(args.get("confirm", False)),
    ),
    "get_pyro_job_status": lambda tenant, args, user_id=None: tool_get_pyro_job_status(
        tenant, args.get("job_id")
    ),
    "list_my_pages": lambda tenant, args, user_id=None: tool_list_my_pages(
        tenant, user_id=user_id
    ),
    "create_page": lambda tenant, args, user_id=None: tool_create_page(
        tenant,
        name=args.get("name", ""),
        header_title=args.get("header_title", ""),
        icon_name=args.get("icon_name", "Sparkles"),
        display_order=args.get("display_order", 0),
        role=args.get("role", ""),
        config=args.get("config"),
        confirm=bool(args.get("confirm", False)),
        # Never trust LLM/tool args for ownership — only request auth user_id.
        user_id=user_id,
    ),
    "update_page": lambda tenant, args, user_id=None: tool_update_page(
        tenant,
        page_id=args.get("page_id", ""),
        page_name=args.get("page_name", ""),
        action=args.get("action", "add_widget"),
        widget_type=args.get("widget_type", ""),
        api_endpoint=args.get("api_endpoint", ""),
        config=args.get("config"),
        confirm=bool(args.get("confirm", False)),
        user_id=user_id,
    ),
    "delete_page": lambda tenant, args, user_id=None: tool_delete_page(
        tenant,
        page_id=args.get("page_id", ""),
        page_name=args.get("page_name", ""),
        confirm=bool(args.get("confirm", False)),
        user_id=user_id,
    ),
}
