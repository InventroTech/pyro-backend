from __future__ import annotations

from typing import Iterable, Optional

from user_settings.models import TenantMemberSetting


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

