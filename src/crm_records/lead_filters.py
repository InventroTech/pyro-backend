"""
Lead filters: party (lead types), lead source, lead status (from UserSettings table only).
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
from user_settings.models import UserSettings

logger = logging.getLogger(__name__)


@dataclass
class LeadFilters:
    """Lead filter configuration for a user from UserSettings table (party, sources, statuses, daily_limit)."""

    eligible_lead_types: List[str]  # party / affiliated_party values
    eligible_lead_sources: List[str]
    eligible_lead_statuses: List[str]
    daily_limit: Optional[int]
    user_uuid: Optional[uuid_module.UUID]
    tenant_membership: Optional[TenantMembership]


def get_lead_filters_for_user(tenant, user_identifier: str) -> LeadFilters:
    """
    Load lead filters for the given user from the database only (no frontend params).
    UserSettings table: eligible_lead_types, eligible_lead_sources, eligible_lead_statuses, daily_limit, user_uuid.
    Routing is separate: use user_uuid with apply_routing_rule_to_queryset(tenant, user_id, queue_type).
    """
    eligible_lead_types: List[str] = []
    eligible_lead_sources: List[str] = []
    eligible_lead_statuses: List[str] = []
    daily_limit: Optional[int] = None
    user_uuid = None
    tenant_membership = None

    if not tenant or not user_identifier:
        return LeadFilters(
            eligible_lead_types=eligible_lead_types,
            eligible_lead_sources=eligible_lead_sources,
            eligible_lead_statuses=eligible_lead_statuses,
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
            any_setting = UserSettings.objects.filter(
                tenant=tenant, tenant_membership=tenant_membership
            ).first()
            daily_limit = getattr(any_setting, "daily_limit", None) if any_setting else None

            try:
                setting = UserSettings.objects.get(
                    tenant=tenant,
                    tenant_membership=tenant_membership,
                    key="LEAD_TYPE_ASSIGNMENT",
                )
                eligible_lead_types = setting.value if isinstance(setting.value, list) else []
                eligible_lead_sources = (
                    setting.lead_sources
                    if isinstance(getattr(setting, "lead_sources", None), list)
                    else []
                )
                try:
                    eligible_lead_statuses = (
                        setting.lead_statuses
                        if isinstance(getattr(setting, "lead_statuses", None), list)
                        else []
                    )
                except (AttributeError, Exception):
                    eligible_lead_statuses = []
                logger.info(
                    "[LeadFilters] From DB: lead_types=%s lead_sources=%s lead_statuses=%s daily_limit=%s",
                    eligible_lead_types,
                    eligible_lead_sources or "(none)",
                    eligible_lead_statuses or "(none)",
                    daily_limit,
                )
            except UserSettings.DoesNotExist:
                logger.info(
                    "[LeadFilters] No LEAD_TYPE_ASSIGNMENT for user %s - all queueable leads eligible",
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
        daily_limit=daily_limit,
        user_uuid=user_uuid,
        tenant_membership=tenant_membership,
    )
