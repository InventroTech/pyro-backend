from __future__ import annotations

from typing import Optional

from user_settings.models import UserKVSetting


USER_KV_GROUP_ID_KEY = "GROUP"
USER_KV_DAILY_TARGET_KEY = "DAILY_TARGET"
USER_KV_DAILY_LIMIT_KEY = "DAILY_LIMIT"
USER_KV_LEAD_ASSIGNMENT_KEY = "LEAD_TYPE_ASSIGNMENT"


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
    """

    UserKVSetting.objects.update_or_create(
        tenant=tenant,
        tenant_membership=tenant_membership,
        key=USER_KV_GROUP_ID_KEY,
        defaults={"value": group_id},
    )
    UserKVSetting.objects.update_or_create(
        tenant=tenant,
        tenant_membership=tenant_membership,
        key=USER_KV_DAILY_TARGET_KEY,
        defaults={"value": daily_target},
    )
    UserKVSetting.objects.update_or_create(
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
    UserKVSetting.objects.update_or_create(
        tenant=tenant,
        tenant_membership=tenant_membership,
        key=USER_KV_LEAD_ASSIGNMENT_KEY,
        defaults={"value": assignment_value},
    )

