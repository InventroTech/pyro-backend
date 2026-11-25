"""
Leads that match the configured sticky-assignment group stay assigned.

The group name comes from the ``STICKY_ASSIGNMENT_GROUP_NAME`` setting (env).
Automatic unassign jobs skip those leads. The midnight snooze job still moves
them from SNOOZED to NOT_CONNECTED, and leaves ``assigned_to`` in place.

A lead has no group id. Membership is the same filter match used for
assignment: party (``affiliated_party``), lead source, lead status, and state.
An empty list on a dimension does not restrict that dimension. A group with
no lists set does not match any lead. A blank setting matches no group.
"""
from __future__ import annotations

import logging
from typing import Optional, Union
from uuid import UUID

from django.conf import settings
from django.db.models import Q

from user_settings.models import Group

logger = logging.getLogger(__name__)


def sticky_assignment_group_name() -> str:
    """Exact group name from settings, or '' when the feature is off."""
    raw = getattr(settings, "STICKY_ASSIGNMENT_GROUP_NAME", "") or ""
    return str(raw).strip()


def _as_list(value) -> list:
    return value if isinstance(value, list) else []


def _filter_lists(group_data: dict) -> tuple[list, list, list, list]:
    return (
        _as_list(group_data.get("party")),
        _as_list(group_data.get("lead_sources")),
        _as_list(group_data.get("lead_statuses")),
        _as_list(group_data.get("states")),
    )


def _usable_group_data(group_data, *, tenant_id, group_name: str) -> Optional[dict]:
    """Group filters that identify sticky-assignment leads, or None."""
    if not isinstance(group_data, dict):
        return None
    queue_type = group_data.get("queue_type")
    if isinstance(queue_type, str) and queue_type.strip().lower() == "ticket":
        return None
    party, lead_sources, lead_statuses, states = _filter_lists(group_data)
    if not (party or lead_sources or lead_statuses or states):
        logger.warning(
            "[StickyAssignment] Group %r on tenant %s has no lead filters; "
            "automatic unassign still applies",
            group_name,
            tenant_id,
        )
        return None
    return group_data


def sticky_group_data_for_tenant(tenant_id: Optional[Union[str, UUID]]) -> Optional[dict]:
    """Sticky-assignment filters for one tenant, when the group can match leads."""
    group_name = sticky_assignment_group_name()
    if not tenant_id or not group_name:
        return None
    group = Group.objects.filter(
        tenant_id=tenant_id,
        name=group_name,
    ).first()
    if group is None:
        return None
    return _usable_group_data(group.group_data, tenant_id=tenant_id, group_name=group_name)


def lead_keeps_assignment(data: dict, group_data: Optional[dict]) -> bool:
    """True when this lead matches the configured group's filters."""
    if not group_data or not isinstance(data, dict):
        return False
    party, lead_sources, lead_statuses, states = _filter_lists(group_data)
    if party and data.get("affiliated_party") not in party:
        return False
    if lead_sources and data.get("lead_source") not in lead_sources:
        return False
    if lead_statuses and data.get("lead_status") not in lead_statuses:
        return False
    if states and data.get("state") not in states:
        return False
    return bool(party or lead_sources or lead_statuses or states)


def _q_matching_group_data(group_data: dict) -> Q:
    party, lead_sources, lead_statuses, states = _filter_lists(group_data)
    q = Q()
    if party:
        q &= Q(data__affiliated_party__in=party)
    if lead_sources:
        q &= Q(data__lead_source__in=lead_sources)
    if lead_statuses:
        q &= Q(data__lead_status__in=lead_statuses)
    if states:
        q &= Q(data__state__in=states)
    return q


def exclude_sticky_assignment_leads(qs, tenant_id=None):
    """Drop sticky-assignment leads from an automatic-unassign queryset."""
    group_name = sticky_assignment_group_name()
    if not group_name:
        return qs
    if tenant_id:
        group_data = sticky_group_data_for_tenant(tenant_id)
        if group_data is None:
            return qs
        return qs.exclude(_q_matching_group_data(group_data))

    combined = Q()
    matched = False
    for group in Group.objects.filter(name=group_name):
        group_data = _usable_group_data(
            group.group_data,
            tenant_id=group.tenant_id,
            group_name=group_name,
        )
        if group_data is None:
            continue
        combined |= Q(tenant_id=group.tenant_id) & _q_matching_group_data(group_data)
        matched = True
    if not matched:
        return qs
    return qs.exclude(combined)
