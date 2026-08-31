from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Optional

from django.core.cache import cache
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


_FRESH_LEADS_INVENTORY_TTL_SECONDS = 30
_QUEUE_INVENTORY_TTL_SECONDS = _FRESH_LEADS_INVENTORY_TTL_SECONDS


def _ticket_group_filter_lists(group_data: dict) -> tuple[list, list]:
    states = group_data.get("states") if isinstance(group_data.get("states"), list) else []
    ticket_types = group_data.get("support_ticket_types")
    if not isinstance(ticket_types, list):
        ticket_types = group_data.get("posters") if isinstance(group_data.get("posters"), list) else []
    return states, ticket_types


def _ticket_inventory_row_matches(row: dict, states: list, ticket_types: list) -> bool:
    state = row["data__state"]
    support_ticket_type = row["data__support_ticket_type"]
    poster = row["data__poster"]
    if states and state not in states:
        return False
    if ticket_types and support_ticket_type not in ticket_types and poster not in ticket_types:
        return False
    return True


def _count_ticket_group_from_inventory(
    open_inventory: list[dict],
    snoozed_due_inventory: list[dict],
    group_data: dict,
) -> int:
    states, ticket_types = _ticket_group_filter_lists(group_data)
    matched = 0
    for row in open_inventory:
        if _ticket_inventory_row_matches(row, states, ticket_types):
            matched += row["count"]
    for row in snoozed_due_inventory:
        if _ticket_inventory_row_matches(row, states, ticket_types):
            matched += row["count"]
    return matched


def _support_ticket_base_qs(tenant):
    return _exclude_expired_support_ticket_types(
        support_ticket_records_qs(tenant=tenant).filter(q_record_unassigned())
    )


def _fetch_support_tickets_inventory(tenant) -> dict[str, list[dict]]:
    """Tenant-wide GROUP BY for open + due-snoozed unassigned ticket queues."""
    base = _support_ticket_base_qs(tenant)
    open_inventory = list(
        base.filter(q_record_pending_resolution())
        .values("data__state", "data__support_ticket_type", "data__poster")
        .annotate(count=Count("id"))
    )
    snoozed_due_inventory = list(
        filter_records_callback_due(
            base.filter(data__resolution_status="Snoozed"),
            at=timezone.now(),
        )
        .values("data__state", "data__support_ticket_type", "data__poster")
        .annotate(count=Count("id"))
    )
    return {"open": open_inventory, "snoozed_due": snoozed_due_inventory}


def _support_tickets_inventory(tenant) -> dict[str, list[dict]]:
    cache_key = f"support_tickets_inventory:{tenant.id}"
    return cache.get_or_set(
        cache_key,
        lambda: _fetch_support_tickets_inventory(tenant),
        timeout=_QUEUE_INVENTORY_TTL_SECONDS,
    )


def count_available_support_tickets_for_group(tenant, group_data: dict) -> int:
    """
    Count unassigned support tickets available for assignment to a group.
    Mirrors get-next-ticket open queue + due snoozed retries, with group filters applied.
    """
    inventory = _support_tickets_inventory(tenant)
    return _count_ticket_group_from_inventory(
        inventory["open"],
        inventory["snoozed_due"],
        group_data,
    )


def _fetch_fresh_leads_inventory(tenant) -> list[dict]:
    """Tenant-wide GROUP BY over queueable leads, bucketed by filter dimensions."""
    return list(
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


def _fresh_leads_inventory(tenant) -> list[dict]:
    """
    Cached tenant-wide inventory scan shared by every caller of
    fresh_leads_counts_for_groups, regardless of which groups they pass in —
    the expensive part is this one GROUP BY, not the per-group bucket matching.
    """
    cache_key = f"fresh_leads_inventory:{tenant.id}"
    return cache.get_or_set(
        cache_key,
        lambda: _fetch_fresh_leads_inventory(tenant),
        timeout=_FRESH_LEADS_INVENTORY_TTL_SECONDS,
    )


def _count_lead_group_from_inventory(inventory: list[dict], group_data: dict) -> int:
    """Sum cached inventory buckets that match a lead group's filter dimensions."""
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
    return matched


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

    return _count_lead_group_from_inventory(_fresh_leads_inventory(tenant), group_data)


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
    ticket_groups: list[Group] = []

    for group in groups:
        group_data = group.group_data if isinstance(group.group_data, dict) else {}
        queue_type = group_data.get("queue_type")
        if isinstance(queue_type, str) and queue_type.strip().lower() == "ticket":
            ticket_groups.append(group)
        else:
            lead_groups.append(group)

    if ticket_groups:
        ticket_inventory = _support_tickets_inventory(tenant)
        for group in ticket_groups:
            group_data = group.group_data if isinstance(group.group_data, dict) else {}
            counts[group.id] = _count_ticket_group_from_inventory(
                ticket_inventory["open"],
                ticket_inventory["snoozed_due"],
                group_data,
            )

    if not lead_groups:
        return counts

    inventory = _fresh_leads_inventory(tenant)

    for group in lead_groups:
        group_data = group.group_data if isinstance(group.group_data, dict) else {}
        counts[group.id] = _count_lead_group_from_inventory(inventory, group_data)

    return counts


_LEAD_FILTER_OPTIONS_TTL_SECONDS = 30

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

    cache_key = f"lead_filter_options:{tenant.id}"
    return cache.get_or_set(
        cache_key,
        lambda: _fetch_lead_filter_options_from_db(tenant.id),
        timeout=_LEAD_FILTER_OPTIONS_TTL_SECONDS,
    )


USER_KV_GROUP_ID_KEY = "GROUP"
USER_KV_DAILY_TARGET_KEY = "DAILY_TARGET"
USER_KV_DAILY_LIMIT_KEY = "DAILY_LIMIT"
USER_KV_LEAD_ASSIGNMENT_KEY = "LEAD_TYPE_ASSIGNMENT"
USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY = "SUPPORT_DAILY_LIMIT_SELF_TRIAL"
USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY = "SUPPORT_DAILY_LIMIT_OTHER"
USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY = "SUPPORT_RESOLVE_RATE_GOAL"
USER_KV_STATE_KEY = "STATE"
USER_KV_DISTRICT_KEY = "DISTRICT"
USER_KV_PARTY_KEY = "PARTY"


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


def resolve_party_match_name(raw) -> Optional[str]:
    """
    Resolve RM PARTY KV to the string matched against lead ``affiliated_party``.

    Catalog IDs map to English party names; plain strings are used as-is.
    """
    party_id = coerce_kv_int(raw)
    if party_id is not None:
        from user_settings.geo_party_catalog import load_geo_party_catalog

        for item in load_geo_party_catalog().get("parties") or []:
            try:
                if int(item.get("id")) == party_id:
                    name = str(item.get("name") or "").strip()
                    return name or None
            except (TypeError, ValueError):
                continue
        return None
    if isinstance(raw, str):
        return raw.strip() or None
    if raw is not None and not isinstance(raw, bool):
        text = str(raw).strip()
        return text or None
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
    state: Optional[int] = None,
    district: Optional[int] = None,
    party: Optional[int] = None,
    update_state: bool = False,
    update_district: bool = False,
    update_party: bool = False,
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
    if update_state:
        TenantMemberSetting.objects.update_or_create(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key=USER_KV_STATE_KEY,
            defaults={"value": coerce_kv_int(state)},
        )
    if update_district:
        TenantMemberSetting.objects.update_or_create(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key=USER_KV_DISTRICT_KEY,
            defaults={"value": coerce_kv_int(district)},
        )
    if update_party:
        party_id = coerce_kv_int(party)
        if party_id is not None:
            party_value = party_id
        elif isinstance(party, str):
            party_value = party.strip() or None
        else:
            party_value = None
        TenantMemberSetting.objects.update_or_create(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key=USER_KV_PARTY_KEY,
            defaults={"value": party_value},
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


def _upsert_or_clear_kv(
    *,
    tenant,
    tenant_membership,
    key: str,
    value: Optional[int],
) -> None:
    if value is None:
        TenantMemberSetting.objects.filter(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key=key,
        ).delete()
    else:
        TenantMemberSetting.objects.update_or_create(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key=key,
            defaults={"value": value},
        )


def upsert_support_daily_limit_kv(
    *,
    tenant,
    tenant_membership,
    self_trial_limit: Optional[int] = None,
    other_limit: Optional[int] = None,
    update_self_trial: bool = False,
    update_other: bool = False,
    resolve_rate_goal: Optional[int] = None,
    update_resolve_rate_goal: bool = False,
) -> None:
    """
    Persist CSE support daily limits (hard caps) and/or resolve-rate goal (%).

    When an ``update_*`` flag is True and the value is ``None``, the KV row is removed.
    """
    if update_self_trial:
        _upsert_or_clear_kv(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key=USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
            value=self_trial_limit,
        )
    if update_other:
        _upsert_or_clear_kv(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key=USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY,
            value=other_limit,
        )
    if update_resolve_rate_goal:
        _upsert_or_clear_kv(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key=USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY,
            value=resolve_rate_goal,
        )


USER_KV_RESERVED_KEYS = frozenset(
    {
        USER_KV_GROUP_ID_KEY,
        USER_KV_DAILY_TARGET_KEY,
        USER_KV_DAILY_LIMIT_KEY,
        USER_KV_LEAD_ASSIGNMENT_KEY,
        USER_KV_SUPPORT_DAILY_LIMIT_SELF_TRIAL_KEY,
        USER_KV_SUPPORT_DAILY_LIMIT_OTHER_KEY,
        USER_KV_SUPPORT_RESOLVE_RATE_GOAL_KEY,
        USER_KV_STATE_KEY,
        USER_KV_DISTRICT_KEY,
        USER_KV_PARTY_KEY,
    }
)


def is_user_management_kv_key(key: str) -> bool:
    """
    True for free-form User Management custom field keys stored as bare uppercase
    names (e.g. EMPLOYEE_CODE) — not reserved system keys like GROUP / STATE.
    """
    if not isinstance(key, str) or not key:
        return False
    if key != key.upper() or len(key) > 100:
        return False
    if key in USER_KV_RESERVED_KEYS:
        return False
    if not key[0].isalpha():
        return False
    return all(c.isalnum() or c == "_" for c in key)


def upsert_custom_field_kv(
    *,
    tenant,
    tenant_membership,
    fields: dict,
) -> None:
    """
    Persist User Management custom fields into user_kv_settings.

    ``fields`` maps bare uppercase keys (e.g. STATE) to JSON-serializable values.
    ``None`` / empty string clears (deletes) the row.
    """
    for key, value in fields.items():
        if not is_user_management_kv_key(key):
            continue
        if value is None or value == "":
            TenantMemberSetting.objects.filter(
                tenant=tenant,
                tenant_membership=tenant_membership,
                key=key,
            ).delete()
        else:
            TenantMemberSetting.objects.update_or_create(
                tenant=tenant,
                tenant_membership=tenant_membership,
                key=key,
                defaults={"value": value},
            )

