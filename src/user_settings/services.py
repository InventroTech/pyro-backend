from __future__ import annotations

from typing import Optional

from django.utils import timezone

from user_settings.models import TenantMemberSetting


USER_KV_GROUP_ID_KEY = "GROUP"
USER_KV_DAILY_TARGET_KEY = "DAILY_TARGET"
USER_KV_DAILY_LIMIT_KEY = "DAILY_LIMIT"
USER_KV_LEAD_ASSIGNMENT_KEY = "LEAD_TYPE_ASSIGNMENT"

_CORE_KV_KEYS = (
    USER_KV_GROUP_ID_KEY,
    USER_KV_DAILY_TARGET_KEY,
    USER_KV_DAILY_LIMIT_KEY,
)


def upsert_user_kv_settings(
    *,
    tenant,
    tenant_membership,
    group_id: Optional[int],
    daily_target: Optional[int],
    daily_limit: Optional[int],
) -> None:
    """
    Keep a dedicated key/value row for commonly used "core" user settings.

    This is intentionally redundant with the existing LEAD_TYPE_ASSIGNMENT row/columns,
    but gives a simple 'key' -> 'value' table for quick UI and reporting use cases.

    Uses one SELECT plus bulk_update/bulk_create instead of three update_or_create
    round-trips (reduces N+1-style query noise on user create/update).
    """
    keys_values = {
        USER_KV_GROUP_ID_KEY: group_id,
        USER_KV_DAILY_TARGET_KEY: daily_target,
        USER_KV_DAILY_LIMIT_KEY: daily_limit,
    }

    existing_by_key = {
        row.key: row
        for row in TenantMemberSetting.objects.filter(
            tenant=tenant,
            tenant_membership=tenant_membership,
            key__in=_CORE_KV_KEYS,
        )
    }

    to_update = []
    to_create = []
    now = timezone.now()

    for key in _CORE_KV_KEYS:
        value = keys_values[key]
        row = existing_by_key.get(key)
        if row is not None:
            if row.value != value:
                row.value = value
                row.updated_at = now
                to_update.append(row)
        else:
            to_create.append(
                TenantMemberSetting(
                    tenant=tenant,
                    tenant_membership=tenant_membership,
                    key=key,
                    value=value,
                )
            )

    if to_update:
        TenantMemberSetting.objects.bulk_update(to_update, ["value", "updated_at"])
    if to_create:
        TenantMemberSetting.objects.bulk_create(to_create)


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

