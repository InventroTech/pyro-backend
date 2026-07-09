from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Optional

from django.db.models import Count, Q
from django.utils import timezone

from crm_records.models import Record
from support_ticket.records import (
    filter_records_callback_due,
    q_record_pending_resolution,
    q_record_unassigned,
    support_ticket_records_qs,
)
from user_settings.models import Group, TenantMemberSetting

_QUEUEABLE_LEADS_WHERE = """
    (
        (data->>'assigned_to') IS NULL
        OR TRIM(COALESCE(data->>'assigned_to', '')) = ''
        OR LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) IN ('null', 'none')
    )
    AND UPPER(COALESCE(data->>'lead_stage','')) IN ('FRESH','IN_QUEUE')
    AND COALESCE((data->>'call_attempts')::int, 0) = 0
"""

_EXPIRED_SUPPORT_TICKET_TYPES = frozenset({
    "Trial Expired",
    "Premium Expired",
    "trial_expired",
    "premium_expired",
})


def _exclude_expired_support_ticket_types(qs):
    expired = list(_EXPIRED_SUPPORT_TICKET_TYPES)
    return qs.exclude(
        Q(data__support_ticket_type__in=expired) | Q(data__poster__in=expired)
    )


def _apply_ticket_group_filters(qs, group_data: dict):
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


def count_available_support_tickets_for_group(tenant, group_data: dict) -> int:
    """
    Count unassigned support tickets available for assignment to a group.
    Mirrors get-next-ticket open queue + due snoozed retries, with group filters applied.
    """
    base = _exclude_expired_support_ticket_types(
        support_ticket_records_qs(tenant=tenant).filter(q_record_unassigned())
    )
    open_qs = _apply_ticket_group_filters(
        base.filter(q_record_pending_resolution()),
        group_data,
    )
    snoozed_due_qs = _apply_ticket_group_filters(
        filter_records_callback_due(
            base.filter(data__resolution_status="Snoozed"),
            at=timezone.now(),
        ),
        group_data,
    )
    return open_qs.count() + snoozed_due_qs.count()


def count_available_fresh_leads_for_group(tenant, group: Group) -> int:
    """
    Count queueable items matching a group's filter configuration.
    Lead groups: unassigned FRESH/IN_QUEUE leads with 0 call attempts.
    Ticket groups: unassigned open + due snoozed support tickets.
    """
    group_data = group.group_data if isinstance(group.group_data, dict) else {}
    queue_type = group_data.get("queue_type")
    if isinstance(queue_type, str) and queue_type.strip().lower() == "ticket":
        return count_available_support_tickets_for_group(tenant, group_data)

    party = group_data.get("party") if isinstance(group_data.get("party"), list) else []
    lead_sources = group_data.get("lead_sources") if isinstance(group_data.get("lead_sources"), list) else []
    lead_statuses = group_data.get("lead_statuses") if isinstance(group_data.get("lead_statuses"), list) else []
    states = group_data.get("states") if isinstance(group_data.get("states"), list) else []

    qs = Record.objects.filter(tenant=tenant, entity_type="lead").extra(where=[_QUEUEABLE_LEADS_WHERE])

    if party:
        qs = qs.filter(data__affiliated_party__in=party)
    if lead_sources:
        qs = qs.filter(data__lead_source__in=lead_sources)
    if lead_statuses:
        qs = qs.filter(data__lead_status__in=lead_statuses)
    if states:
        qs = qs.filter(data__state__in=states)

    return qs.count()


def fresh_leads_counts_for_groups(tenant, groups: Iterable[Group]) -> dict[int, int]:
    """
    Map group id -> available queue count (fresh leads or support tickets).

    Lead groups share one inventory scan instead of one COUNT query per group.
    """
    groups = list(groups)
    if not groups:
        return {}

    counts: dict[int, int] = {}
    lead_groups: list[Group] = []

    for group in groups:
        group_data = group.group_data if isinstance(group.group_data, dict) else {}
        queue_type = group_data.get("queue_type")
        if isinstance(queue_type, str) and queue_type.strip().lower() == "ticket":
            counts[group.id] = count_available_support_tickets_for_group(tenant, group_data)
        else:
            lead_groups.append(group)

    if not lead_groups:
        return counts

    # One grouped query: bucket by filter dimensions instead of loading every row.
    inventory = list(
        Record.objects.filter(tenant=tenant, entity_type="lead")
        .extra(where=[_QUEUEABLE_LEADS_WHERE])
        .values(
            "data__affiliated_party",
            "data__lead_source",
            "data__lead_status",
            "data__state",
        )
        .annotate(count=Count("id"))
    )

    for group in lead_groups:
        group_data = group.group_data if isinstance(group.group_data, dict) else {}
        party = group_data.get("party") if isinstance(group_data.get("party"), list) else []
        lead_sources = group_data.get("lead_sources") if isinstance(group_data.get("lead_sources"), list) else []
        lead_statuses = group_data.get("lead_statuses") if isinstance(group_data.get("lead_statuses"), list) else []
        states = group_data.get("states") if isinstance(group_data.get("states"), list) else []

        matched = 0
        for row in inventory:
            affiliated_party = row["data__affiliated_party"]
            lead_source = row["data__lead_source"]
            lead_status = row["data__lead_status"]
            state = row["data__state"]
            if party and affiliated_party not in party:
                continue
            if lead_sources and lead_source not in lead_sources:
                continue
            if lead_statuses and lead_status not in lead_statuses:
                continue
            if states and state not in states:
                continue
            matched += row["count"]
        counts[group.id] = matched

    return counts


_LEAD_FILTER_OPTIONS_TTL_SECONDS = 30
_lead_filter_options_cache: dict[str, tuple[float, dict[str, list[str]]]] = {}
_lead_filter_options_lock = threading.Lock()

# One indexed DISTINCT per column; run in parallel (faster than UNION of 4 full scans).
_LEAD_FILTER_DISTINCT_SQL = {
    "lead_types": """
        SELECT DISTINCT TRIM(data->>'affiliated_party') AS value
        FROM records
        WHERE tenant_id = %s
          AND entity_type = 'lead'
          AND data->>'affiliated_party' IS NOT NULL
          AND TRIM(data->>'affiliated_party') NOT IN ('', 'null')
        ORDER BY 1
    """,
    "lead_sources": """
        SELECT DISTINCT TRIM(data->>'lead_source') AS value
        FROM records
        WHERE tenant_id = %s
          AND entity_type = 'lead'
          AND data->>'lead_source' IS NOT NULL
          AND TRIM(data->>'lead_source') NOT IN ('', 'null')
        ORDER BY 1
    """,
    "lead_statuses": """
        SELECT DISTINCT TRIM(data->>'lead_status') AS value
        FROM records
        WHERE tenant_id = %s
          AND entity_type = 'lead'
          AND data->>'lead_status' IS NOT NULL
          AND TRIM(data->>'lead_status') NOT IN ('', 'null')
        ORDER BY 1
    """,
    "lead_states": """
        SELECT DISTINCT TRIM(data->>'state') AS value
        FROM records
        WHERE tenant_id = %s
          AND entity_type = 'lead'
          AND data->>'state' IS NOT NULL
          AND TRIM(data->>'state') NOT IN ('', 'null')
        ORDER BY 1
    """,
}


def _fetch_distinct_lead_filter_column(tenant_id, kind: str, sql: str) -> tuple[str, list[str]]:
    from django.db import connections

    conn = connections["default"]
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, [tenant_id])
            values = [row[0] for row in cursor.fetchall() if row[0]]
        return kind, values
    finally:
        conn.close()


def _fetch_lead_filter_options_from_db(tenant_id) -> dict[str, list[str]]:
    options = {
        "lead_types": [],
        "lead_sources": [],
        "lead_statuses": [],
        "lead_states": [],
    }
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(_fetch_distinct_lead_filter_column, tenant_id, kind, sql)
            for kind, sql in _LEAD_FILTER_DISTINCT_SQL.items()
        ]
        for future in as_completed(futures):
            kind, values = future.result()
            options[kind] = values
    return options


def get_lead_filter_options(tenant) -> dict[str, list[str]]:
    """
    Distinct lead filter dropdown values for a tenant.

    Cached briefly so parallel page-load requests (lead-types, lead-sources, etc.)
    share one database round trip.
    """
    if not tenant:
        return {
            "lead_types": [],
            "lead_sources": [],
            "lead_statuses": [],
            "lead_states": [],
        }

    cache_key = str(tenant.id)
    now = time.monotonic()
    with _lead_filter_options_lock:
        cached = _lead_filter_options_cache.get(cache_key)
        if cached and now - cached[0] < _LEAD_FILTER_OPTIONS_TTL_SECONDS:
            return cached[1]

    options = _fetch_lead_filter_options_from_db(tenant.id)

    with _lead_filter_options_lock:
        cached = _lead_filter_options_cache.get(cache_key)
        if cached and time.monotonic() - cached[0] < _LEAD_FILTER_OPTIONS_TTL_SECONDS:
            return cached[1]
        _lead_filter_options_cache[cache_key] = (time.monotonic(), options)
        return options


USER_KV_GROUP_ID_KEY = "GROUP"
USER_KV_DAILY_TARGET_KEY = "DAILY_TARGET"
USER_KV_DAILY_LIMIT_KEY = "DAILY_LIMIT"
USER_KV_LEAD_ASSIGNMENT_KEY = "LEAD_TYPE_ASSIGNMENT"


def coerce_kv_int(value) -> Optional[int]:
    """Coerce a TenantMemberSetting JSON value to a non-negative int, if possible."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, float) and value.is_integer() and value >= 0:
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def sum_kv_int_for_memberships(tenant, membership_ids: Iterable[int], key: str) -> int:
    """Sum integer KV values for the given memberships (one row per membership expected)."""
    total = 0
    rows = TenantMemberSetting.objects.filter(
        tenant=tenant,
        tenant_membership_id__in=list(membership_ids),
        key=key,
    )
    for row in rows:
        coerced = coerce_kv_int(row.value)
        if coerced is not None:
            total += coerced
    return total


def kv_int_by_membership(tenant, membership_ids: Iterable[int], key: str) -> dict[int, int]:
    """Map tenant_membership_id -> int value for rows with a coercible integer."""
    result: dict[int, int] = {}
    rows = TenantMemberSetting.objects.filter(
        tenant=tenant,
        tenant_membership_id__in=list(membership_ids),
        key=key,
    )
    for row in rows:
        coerced = coerce_kv_int(row.value)
        if coerced is not None:
            result[row.tenant_membership_id] = coerced
    return result


def upsert_user_kv_settings(
    *,
    tenant,
    tenant_membership,
    group_id: Optional[int],
    daily_target: Optional[int],
    daily_limit: Optional[int],
) -> None:
    """Persist core per-user settings in TenantMemberSetting KV rows."""

    TenantMemberSetting.objects.update_or_create(
        tenant=tenant,
        tenant_membership=tenant_membership,
        key=USER_KV_GROUP_ID_KEY,
        defaults={"value": group_id},
    )
    TenantMemberSetting.objects.update_or_create(
        tenant=tenant,
        tenant_membership=tenant_membership,
        key=USER_KV_DAILY_TARGET_KEY,
        defaults={"value": daily_target},
    )
    TenantMemberSetting.objects.update_or_create(
        tenant=tenant,
        tenant_membership=tenant_membership,
        key=USER_KV_DAILY_LIMIT_KEY,
        defaults={"value": daily_limit},
    )


def upsert_user_lead_assignment_kv(
    *,
    tenant,
    tenant_membership,
    assignment_value,
) -> None:
    TenantMemberSetting.objects.update_or_create(
        tenant=tenant,
        tenant_membership=tenant_membership,
        key=USER_KV_LEAD_ASSIGNMENT_KEY,
        defaults={"value": assignment_value},
    )

