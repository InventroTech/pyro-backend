"""
Lead filters from Group + TenantMemberSetting only.
All configuration is read from the database (no frontend overrides).
Routing rules are separate: applied via apply_routing_rule_to_queryset(tenant, user_id, queue_type) using user_uuid from here.

Used by Get Next Lead API to determine which leads a user is eligible to receive.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional
import uuid as uuid_module

from authz.models import TenantMembership
from user_settings.models import Group, TenantMemberSetting
from user_settings.services import (
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_GROUP_ID_KEY,
)

logger = logging.getLogger(__name__)


@dataclass
class LeadFilters:
    """Lead filter configuration for a user from Group/KV settings."""

    eligible_lead_types: List[str]  # party / affiliated_party values
    eligible_lead_sources: List[str]
    eligible_lead_statuses: List[str]
    eligible_states: List[str]
    daily_limit: Optional[int]
    user_uuid: Optional[uuid_module.UUID]
    tenant_membership: Optional[TenantMembership]


def get_lead_filters_for_user(tenant, user_identifier: str) -> LeadFilters:
    """
    Load lead filters for the given user from the database only (no frontend params).
    Group/KV settings: eligible_lead_types, eligible_lead_sources, eligible_lead_statuses, states, daily_limit, user_uuid.
    Routing is separate: use user_uuid with apply_routing_rule_to_queryset(tenant, user_id, queue_type).
    """
    eligible_lead_types: List[str] = []
    eligible_lead_sources: List[str] = []
    eligible_lead_statuses: List[str] = []
    eligible_states: List[str] = []
    daily_limit: Optional[int] = None
    user_uuid = None
    tenant_membership = None

    if not tenant or not user_identifier:
        return LeadFilters(
            eligible_lead_types=eligible_lead_types,
            eligible_lead_sources=eligible_lead_sources,
            eligible_lead_statuses=eligible_lead_statuses,
            eligible_states=eligible_states,
            daily_limit=daily_limit,
            user_uuid=user_uuid,
            tenant_membership=tenant_membership,
        )

    try:
        # Resolve user_identifier to user_uuid (UUID or email -> TenantMembership)
        try:
            user_uuid = uuid_module.UUID(str(user_identifier))
            logger.debug("[LeadFilters] user_identifier parsed as UUID: %s", user_uuid)
        except (ValueError, AttributeError):
            tenant_membership = TenantMembership.objects.filter(
                tenant=tenant,
                email__iexact=str(user_identifier),
            ).exclude(user_id__isnull=True).first()
            user_uuid = tenant_membership.user_id if tenant_membership and tenant_membership.user_id else None
            logger.debug("[LeadFilters] Resolved user_uuid from TenantMembership: %s", user_uuid)

        if user_uuid and not tenant_membership:
            tenant_membership = TenantMembership.objects.filter(
                tenant=tenant,
                user_id=user_uuid,
            ).first()

        if tenant_membership:
            kv_settings = TenantMemberSetting.objects.filter(
                tenant=tenant,
                tenant_membership=tenant_membership,
                key__in=[USER_KV_DAILY_LIMIT_KEY, USER_KV_GROUP_ID_KEY],
            )
            kv_map = {row.key: row.value for row in kv_settings}
            _dl = kv_map.get(USER_KV_DAILY_LIMIT_KEY)
            if _dl is not None and not isinstance(_dl, bool):
                try:
                    daily_limit = int(_dl)
                except (TypeError, ValueError):
                    pass

            group = None
            group_id = kv_map.get(USER_KV_GROUP_ID_KEY)
            if group_id:
                group = Group.objects.filter(tenant=tenant, id=group_id).first()
            if group:
                group_data = group.group_data if isinstance(group.group_data, dict) else {}
                eligible_lead_types = group_data.get("party") if isinstance(group_data.get("party"), list) else []
                eligible_lead_sources = group_data.get("lead_sources") if isinstance(group_data.get("lead_sources"), list) else []
                eligible_lead_statuses = group_data.get("lead_statuses") if isinstance(group_data.get("lead_statuses"), list) else []
                eligible_states = group_data.get("states") if isinstance(group_data.get("states"), list) else []
                logger.info(
                    "[LeadFilters] From Group(%s): lead_types=%s lead_sources=%s lead_statuses=%s states=%s daily_limit=%s",
                    group.name,
                    eligible_lead_types,
                    eligible_lead_sources or "(none)",
                    eligible_lead_statuses or "(none)",
                    eligible_states or "(none)",
                    daily_limit,
                )
            else:
                logger.info(
                    "[LeadFilters] No GROUP KV for user %s - all queueable leads eligible",
                    user_identifier,
                )
        else:
            logger.warning("[LeadFilters] No TenantMembership for user_identifier=%s", user_identifier)
    except Exception as e:
        logger.error("[LeadFilters] Error loading filters: %s", str(e))

    return LeadFilters(
        eligible_lead_types=eligible_lead_types,
        eligible_lead_sources=eligible_lead_sources,
        eligible_lead_statuses=eligible_lead_statuses,
        eligible_states=eligible_states,
        daily_limit=daily_limit,
        user_uuid=user_uuid,
        tenant_membership=tenant_membership,
    )
