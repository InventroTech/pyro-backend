from __future__ import annotations

import logging
import uuid as uuid_module
from dataclasses import dataclass
from typing import List, Optional

from authz.models import TenantMembership

from crm_records.lead_filters import get_lead_filters_for_user

logger = logging.getLogger(__name__)


@dataclass
class ResolvedUser:
    identifier: str  # supabase_uid or email — used as assigned_to value
    uuid: Optional[uuid_module.UUID]  # used for routing rule lookup
    membership: Optional[TenantMembership]
    email: Optional[str]

    eligible_lead_types: List[str]
    eligible_lead_sources: List[str]
    eligible_lead_statuses: List[str]
    daily_limit: Optional[int]


class UserResolver:
    """
    Centralizes user identifier resolution + DB-only lead filter loading.
    """

    def resolve(self, tenant, request_user) -> ResolvedUser:
        identifier = getattr(request_user, "supabase_uid", None) or getattr(request_user, "email", None)
        email = getattr(request_user, "email", None)

        filters = get_lead_filters_for_user(tenant, identifier)
        return ResolvedUser(
            identifier=identifier,
            uuid=filters.user_uuid,
            membership=filters.tenant_membership,
            email=email,
            eligible_lead_types=filters.eligible_lead_types,
            eligible_lead_sources=filters.eligible_lead_sources,
            eligible_lead_statuses=filters.eligible_lead_statuses,
            daily_limit=filters.daily_limit,
        )

