"""
CSE support-ticket analytics (Mixpanel-style metrics from CRM records).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from django.db.models import Q
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from authz.models import TenantMembership
from support_ticket.records import distinct_data_values, q_data_unset, support_ticket_records_qs
from support_ticket.ticket_types import canonical_support_ticket_type_key, q_record_support_ticket_type_key

from .services import (
    TeamResolver,
    filter_user_ids_by_manager_i,
    get_manager_i_map,
    get_manager_i_options,
)
from .utils import get_date_range

TERMINAL_RESOLUTION_STATUSES = frozenset({
    "Resolved",
    "Can't Resolve",
    "Already Resolved",
    "No Issue",
    "Not Possible",
    "Feature Requested",
})

HANDLING_TIME_STATUS_FILTERS = {
    "resolved": Q(data__resolution_status__iexact="resolved"),
    "not_connected": (
        Q(data__call_status__icontains="not connected")
        | Q(data__call_status__icontains="no answer")
        | Q(data__call_status__icontains="unreachable")
    ),
    "call_back": (
        Q(data__resolution_status__iexact="wip")
        | Q(data__call_status__icontains="call later")
        | Q(data__call_status__icontains="callback")
    ),
    "wip": Q(data__resolution_status__iexact="wip"),
    "cant_resolve": Q(data__resolution_status__iexact="can't resolve"),
    "pending": Q(data__resolution_status__isnull=True) | Q(data__resolution_status=""),
}

# Categorical ``data`` fields users can filter reports by (Mixpanel-style).
# key -> human label. Kept as an allowlist so arbitrary ORM lookups can't be
# injected via the ``af`` query param.
CSE_ATTRIBUTE_FIELDS = [
    ("support_ticket_type", "Ticket Type"),
    ("resolution_status", "Resolution Status"),
    ("call_status", "Call Status"),
    ("cse_name", "CSE"),
    ("source", "Source"),
    ("subscription_status", "Subscription Status"),
    ("layout_status", "Layout Status"),
    ("state", "State"),
    ("badge", "Badge"),
    ("rm_name", "RM Name"),
    ("poster", "Poster"),
]
CSE_ATTRIBUTE_KEYS = frozenset(key for key, _ in CSE_ATTRIBUTE_FIELDS) | {
    "manager_i"
}


class CseVisibilityResolver:
    """Role/hierarchy scoping: CSE=self, ASM=team CSEs, GM=all."""

    TENANT_WIDE_ROLES = frozenset({"GM", "PYRO_ADMIN", "OWNER"})

    @staticmethod
    def _normalize_email(email: Optional[str]) -> Optional[str]:
        if not email:
            return None
        normalized = str(email).strip().lower()
        return normalized or None

    @classmethod
    def _cse_emails_in_team(cls, manager_user_id: str, tenant) -> Set[str]:
        team_user_ids = TeamResolver.get_team_user_ids(str(manager_user_id), tenant)
        emails: Set[str] = set()
        memberships = TenantMembership.objects.filter(
            tenant=tenant,
            is_active=True,
            user_id__in=team_user_ids,
        ).select_related("role")
        for membership in memberships:
            role_key = (membership.role.key or "").upper() if membership.role else ""
            if role_key != "CSE":
                continue
            normalized = cls._normalize_email(membership.email)
            if normalized:
                emails.add(normalized)
        return emails

    @classmethod
    def resolve(cls, user_id: str, tenant) -> Tuple[Optional[Set[str]], str]:
        membership = (
            TenantMembership.objects.filter(tenant=tenant, user_id=user_id, is_active=True)
            .select_related("role")
            .first()
        )
        if not membership:
            return set(), "self"

        role_key = (membership.role.key or "").upper() if membership.role else ""

        if role_key in cls.TENANT_WIDE_ROLES:
            return None, "all"

        if role_key == "CSE":
            own_email = cls._normalize_email(membership.email)
            return ({own_email} if own_email else set()), "self"

        team_emails = cls._cse_emails_in_team(str(user_id), tenant)
        if team_emails:
            return team_emails, "team"

        return set(), "none"

    @staticmethod
    def cse_name_allowed(cse_name: str, allowed_cse_emails: Optional[Set[str]]) -> bool:
        if allowed_cse_emails is None:
            return True
        normalized = str(cse_name or "").strip().lower()
        return bool(normalized and normalized in allowed_cse_emails)

    @classmethod
    def clamp_cse_name_filter(
        cls,
        cse_name: Optional[str],
        allowed_cse_emails: Optional[Set[str]],
    ) -> Optional[str]:
        if not cse_name or allowed_cse_emails is None:
            return cse_name
        normalized = str(cse_name).strip().lower()
        if normalized in allowed_cse_emails:
            return cse_name
        for allowed in allowed_cse_emails:
            if allowed in normalized or normalized in allowed:
                return cse_name
        return None


def _resolution_time_to_seconds(value: Any) -> Optional[int]:
    """
    Parse ticket work time (MM:SS or HH:MM:SS) to seconds.

    ``resolution_time`` is accumulated session time the CSE actively worked on
    the ticket (not wall-clock assigned→completed). Returns None when missing
    or zero so averages only include tickets that were actually worked.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or ":" not in text:
        return None
    parts = text.split(":")
    try:
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = int(parts[1])
        elif len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1]) + hours * 60
            seconds = int(parts[2])
        else:
            return None
        if minutes < 0 or seconds < 0:
            return None
        # Normalize non-canonical values like "1:99".
        total = minutes * 60 + seconds
        return total if total > 0 else None
    except (TypeError, ValueError):
        return None


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _cse_name_from_data(data: Dict[str, Any]) -> str:
    return str(data.get("cse_name") or "").strip()


def _parse_data_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"null", "none"}:
        return None
    parsed = parse_datetime(text)
    if parsed is None:
        try:
            from dateutil import parser as date_parser

            parsed = date_parser.parse(text)
        except (ValueError, TypeError, OverflowError):
            return None
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
    return parsed


def _timestamp_date(value: datetime) -> date:
    if timezone.is_aware(value):
        return timezone.localtime(value).date()
    return value.date()


def _record_in_period(data: Dict[str, Any], start_date: date, end_date: date) -> bool:
    completed_at = _parse_data_timestamp(data.get("completed_at"))
    if completed_at is not None:
        day = _timestamp_date(completed_at)
        return start_date <= day <= end_date
    dumped_at = _parse_data_timestamp(data.get("dumped_at"))
    if dumped_at is not None:
        day = _timestamp_date(dumped_at)
        return start_date <= day <= end_date
    return False


def _is_call_back_data(data: Dict[str, Any]) -> bool:
    if _normalize_status(data.get("resolution_status")) == "wip":
        return True
    call_status = _normalize_status(data.get("call_status"))
    if "call later" in call_status or "callback" in call_status:
        return True
    snooze_until = data.get("snooze_until")
    return bool(snooze_until and str(snooze_until).strip())


def _is_not_connected_data(data: Dict[str, Any]) -> bool:
    call_status = _normalize_status(data.get("call_status"))
    return any(token in call_status for token in ("not connected", "no answer", "unreachable"))


def _matches_handling_status(data: Dict[str, Any], handling_status: Optional[str]) -> bool:
    if not handling_status:
        return True
    status = _normalize_status(data.get("resolution_status"))
    if handling_status == "resolved":
        return status == "resolved"
    if handling_status == "not_connected":
        return _is_not_connected_data(data)
    if handling_status == "call_back":
        return _is_call_back_data(data)
    if handling_status == "wip":
        return status == "wip"
    if handling_status == "cant_resolve":
        return status == "can't resolve"
    if handling_status == "pending":
        return not status
    return True


def _q_open_ticket() -> Q:
    terminal_q = Q()
    for status in TERMINAL_RESOLUTION_STATUSES:
        terminal_q |= Q(data__resolution_status__iexact=status)
    return ~terminal_q


class CseMetricsService:
    """Aggregates support-ticket metrics per CSE for the analytics dashboard."""

    def __init__(self, tenant, allowed_cse_emails: Optional[Set[str]] = None):
        self.tenant = tenant
        self.allowed_cse_emails = allowed_cse_emails

    def get_filter_options(self, visibility_scope: str = "all") -> Dict[str, List[str]]:
        qs = support_ticket_records_qs(tenant=self.tenant)
        ticket_types = sorted(
            set(distinct_data_values(qs, "support_ticket_type"))
            | set(distinct_data_values(qs, "poster"))
        )
        ticket_types = [t for t in ticket_types if t and str(t).strip()]
        cse_names = sorted(
            distinct_data_values(
                qs.exclude(data__cse_name__isnull=True).exclude(data__cse_name=""),
                "cse_name",
            )
        )
        cse_names = [name for name in cse_names if name and str(name).strip()]
        if self.allowed_cse_emails is not None:
            cse_names = [
                name
                for name in cse_names
                if CseVisibilityResolver.cse_name_allowed(name, self.allowed_cse_emails)
            ]

        attributes = []
        for key, label in CSE_ATTRIBUTE_FIELDS:
            values = [
                str(v).strip()
                for v in distinct_data_values(qs, key)
                if v is not None and str(v).strip()
            ]
            if key == "cse_name" and self.allowed_cse_emails is not None:
                values = [
                    v
                    for v in values
                    if CseVisibilityResolver.cse_name_allowed(v, self.allowed_cse_emails)
                ]
            values = sorted(set(values))[:300]
            if values:
                attributes.append({"key": key, "label": label, "values": values})

        if self.allowed_cse_emails is None:
            # Always expose Manager I for GM-scope so the filter/breakdown appear
            # even before Manager I users are assigned in the hierarchy.
            attributes.append(
                {
                    "key": "manager_i",
                    "label": "Manager I",
                    "values": get_manager_i_options(self.tenant),
                }
            )

        return {
            "ticket_types": ticket_types,
            "cse_names": cse_names,
            "handling_time_statuses": list(HANDLING_TIME_STATUS_FILTERS.keys()),
            "attributes": attributes,
            "visibility_scope": visibility_scope,
        }

    def get_support_ticket_breakdown(
        self,
        ticket_type_filter: Optional[List[str]] = None,
        resolution_status_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Current assigned-ticket inventory by type and resolution status."""
        inventory: List[Tuple[str, str]] = []
        for _, data in self._iter_assigned_records(self._assigned_qs()):
            ticket_type = str(
                data.get("support_ticket_type") or data.get("poster") or "Unknown"
            ).strip() or "Unknown"
            resolution_status = str(
                data.get("resolution_status") or "Open"
            ).strip() or "Open"
            inventory.append((ticket_type, resolution_status))

        available_types = sorted({ticket_type for ticket_type, _ in inventory})
        available_statuses = sorted({status for _, status in inventory})
        selected_types = set(ticket_type_filter or [])

        by_type_counts: Dict[str, int] = {}
        by_status_counts: Dict[str, int] = {}
        total = 0
        for ticket_type, resolution_status in inventory:
            if selected_types and ticket_type not in selected_types:
                continue
            if (
                resolution_status_filter
                and resolution_status.lower() != resolution_status_filter.lower()
            ):
                continue
            total += 1
            by_type_counts[ticket_type] = by_type_counts.get(ticket_type, 0) + 1
            by_status_counts[resolution_status] = (
                by_status_counts.get(resolution_status, 0) + 1
            )

        return {
            "total": total,
            "by_type": [
                {"ticket_type": ticket_type, "count": count}
                for ticket_type, count in sorted(
                    by_type_counts.items(), key=lambda item: (-item[1], item[0])
                )
            ],
            "by_status": [
                {"resolution_status": resolution_status, "count": count}
                for resolution_status, count in sorted(
                    by_status_counts.items(), key=lambda item: (-item[1], item[0])
                )
            ],
            "available_types": available_types,
            "available_statuses": available_statuses,
        }

    def _apply_cse_filter(self, qs, cse_name: Optional[str]):
        if not cse_name:
            return qs
        return qs.filter(data__cse_name__icontains=cse_name)

    def _apply_ticket_type_filter(self, qs, ticket_types: Optional[List[str]]):
        if not ticket_types:
            return qs
        type_q = Q()
        for ticket_type in ticket_types:
            key = canonical_support_ticket_type_key(ticket_type)
            type_q |= q_record_support_ticket_type_key(key)
            type_q |= Q(data__support_ticket_type=ticket_type) | Q(data__poster=ticket_type)
        return qs.filter(type_q)

    def _apply_attribute_filters(
        self, qs, attribute_filters: Optional[Dict[str, List[str]]]
    ):
        if not attribute_filters:
            return qs
        for field, values in attribute_filters.items():
            if field not in CSE_ATTRIBUTE_KEYS:
                continue
            clean = [str(v) for v in values if v is not None and str(v) != ""]
            if not clean:
                continue
            if field == "manager_i":
                continue
            if field == "support_ticket_type":
                qs = qs.filter(
                    Q(data__support_ticket_type__in=clean) | Q(data__poster__in=clean)
                )
            else:
                qs = qs.filter(**{f"data__{field}__in": clean})
        return qs

    def _assigned_qs(
        self,
        ticket_types: Optional[List[str]] = None,
        attribute_filters: Optional[Dict[str, List[str]]] = None,
    ):
        qs = (
            support_ticket_records_qs(tenant=self.tenant)
            .exclude(q_data_unset("cse_name"))
            .exclude(Q(data__cse_name=""))
        )
        manager_names = (
            attribute_filters.get("manager_i")
            if attribute_filters
            else None
        )
        if manager_names:
            cse_memberships = list(
                TenantMembership.objects.filter(
                    tenant=self.tenant,
                    is_active=True,
                    role__key__iexact="CSE",
                )
            )
            by_user_id = {
                str(membership.user_id): membership
                for membership in cse_memberships
                if membership.user_id
            }
            selected_ids = filter_user_ids_by_manager_i(
                set(by_user_id.keys()), self.tenant, manager_names
            )
            # Match tickets by CSE email or display name under the selected Manager I.
            identifiers: List[str] = []
            for user_id in selected_ids:
                membership = by_user_id.get(user_id)
                if not membership:
                    continue
                if membership.email:
                    identifiers.append(str(membership.email).strip())
                if membership.name:
                    identifiers.append(str(membership.name).strip())
            manager_q = Q()
            for identifier in {i for i in identifiers if i}:
                manager_q |= Q(data__cse_name__iexact=identifier)
            qs = qs.filter(manager_q) if manager_q else qs.none()
        qs = self._apply_ticket_type_filter(qs, ticket_types)
        return self._apply_attribute_filters(qs, attribute_filters)

    def _iter_assigned_records(self, qs):
        for record in qs.only("id", "data"):
            data = record.data or {}
            cse = _cse_name_from_data(data)
            if not cse:
                continue
            if not CseVisibilityResolver.cse_name_allowed(cse, self.allowed_cse_emails):
                continue
            yield record, data

    def get_overview(
        self,
        start_date: date,
        end_date: date,
        ticket_types: Optional[List[str]] = None,
        handling_status: Optional[str] = None,
        cse_name: Optional[str] = None,
        attribute_filters: Optional[Dict[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        open_qs = self._apply_cse_filter(
            self._assigned_qs(ticket_types, attribute_filters).filter(_q_open_ticket()),
            cse_name,
        )
        assigned_qs = self._apply_cse_filter(
            self._assigned_qs(ticket_types, attribute_filters), cse_name
        )

        open_call_back = 0
        open_not_connected = 0
        for _, data in self._iter_assigned_records(open_qs):
            if _is_call_back_data(data):
                open_call_back += 1
            if _is_not_connected_data(data):
                open_not_connected += 1

        leads_assigned = 0
        resolved = 0
        not_connected = 0
        call_later = 0
        cant_resolve = 0
        handling_seconds: List[int] = []

        for _, data in self._iter_assigned_records(assigned_qs):
            if not _record_in_period(data, start_date, end_date):
                continue
            leads_assigned += 1
            status = _normalize_status(data.get("resolution_status"))
            if status == "resolved":
                resolved += 1
            if status == "can't resolve":
                cant_resolve += 1
            if _is_not_connected_data(data):
                not_connected += 1
            if _is_call_back_data(data):
                call_later += 1
            if _matches_handling_status(data, handling_status):
                seconds = _resolution_time_to_seconds(data.get("resolution_time"))
                if seconds is not None:
                    handling_seconds.append(seconds)

        resolve_rate = (resolved / leads_assigned) if leads_assigned > 0 else None
        avg_seconds = (sum(handling_seconds) / len(handling_seconds)) if handling_seconds else None

        return {
            "open_call_back": open_call_back,
            "open_not_connected": open_not_connected,
            "leads_assigned": leads_assigned,
            "resolved": resolved,
            "not_connected": not_connected,
            "call_later": call_later,
            "cant_resolve": cant_resolve,
            "resolve_rate": resolve_rate,
            "average_handling_time_seconds": float(avg_seconds) if avg_seconds is not None else None,
            "handling_time_ticket_count": len(handling_seconds),
        }

    def get_time_series(
        self,
        start_date: date,
        end_date: date,
        ticket_types: Optional[List[str]] = None,
        cse_name: Optional[str] = None,
        attribute_filters: Optional[Dict[str, List[str]]] = None,
    ) -> List[Dict[str, Any]]:
        assigned_qs = self._apply_cse_filter(
            self._assigned_qs(ticket_types, attribute_filters), cse_name
        )

        assigned_map: Dict[date, int] = {}
        resolved_map: Dict[date, int] = {}
        not_connected_map: Dict[date, int] = {}
        call_later_map: Dict[date, int] = {}
        handling_by_day: Dict[date, List[int]] = {}
        stacked_resolved_map: Dict[date, int] = {}
        stacked_unresolved_map: Dict[date, int] = {}

        for _, data in self._iter_assigned_records(assigned_qs):
            dumped_at = _parse_data_timestamp(data.get("dumped_at"))
            completed_at = _parse_data_timestamp(data.get("completed_at"))

            if dumped_at is not None:
                dump_day = _timestamp_date(dumped_at)
                if start_date <= dump_day <= end_date:
                    assigned_map[dump_day] = assigned_map.get(dump_day, 0) + 1

            if not _record_in_period(data, start_date, end_date):
                continue

            if completed_at is not None:
                day = _timestamp_date(completed_at)
                if _normalize_status(data.get("resolution_status")) == "resolved":
                    resolved_map[day] = resolved_map.get(day, 0) + 1
                if _is_not_connected_data(data):
                    not_connected_map[day] = not_connected_map.get(day, 0) + 1
                if _is_call_back_data(data):
                    call_later_map[day] = call_later_map.get(day, 0) + 1
                seconds = _resolution_time_to_seconds(data.get("resolution_time"))
                if seconds is not None:
                    handling_by_day.setdefault(day, []).append(seconds)
            if dumped_at is not None:
                dump_day = _timestamp_date(dumped_at)
                if start_date <= dump_day <= end_date:
                    if _normalize_status(data.get("resolution_status")) == "resolved":
                        stacked_resolved_map[dump_day] = stacked_resolved_map.get(dump_day, 0) + 1
                    else:
                        stacked_unresolved_map[dump_day] = stacked_unresolved_map.get(dump_day, 0) + 1

        avg_by_day = {
            day: (sum(values) / len(values)) for day, values in handling_by_day.items() if values
        }

        result = []
        for day in get_date_range(start_date, end_date):
            assigned = assigned_map.get(day, 0)
            resolved = resolved_map.get(day, 0)
            result.append({
                "date": day.strftime("%Y-%m-%d"),
                "assigned": assigned,
                "resolved": resolved,
                "not_connected": not_connected_map.get(day, 0),
                "call_later": call_later_map.get(day, 0),
                "resolve_rate": (resolved / assigned) if assigned > 0 else None,
                "average_handling_time_seconds": avg_by_day.get(day),
                "handling_time_ticket_count": len(handling_by_day.get(day, [])),
                "stacked_resolved": stacked_resolved_map.get(day, 0),
                "stacked_unresolved": stacked_unresolved_map.get(day, 0),
            })
        return result

    def get_member_breakdown(
        self,
        start_date: date,
        end_date: date,
        ticket_types: Optional[List[str]] = None,
        handling_status: Optional[str] = None,
        cse_name: Optional[str] = None,
        attribute_filters: Optional[Dict[str, List[str]]] = None,
    ) -> List[Dict[str, Any]]:
        open_qs = self._apply_cse_filter(
            self._assigned_qs(ticket_types, attribute_filters).filter(_q_open_ticket()),
            cse_name,
        )
        assigned_qs = self._apply_cse_filter(
            self._assigned_qs(ticket_types, attribute_filters), cse_name
        )

        open_stats: Dict[str, Dict[str, int]] = {}
        for _, data in self._iter_assigned_records(open_qs):
            cse = _cse_name_from_data(data)
            bucket = open_stats.setdefault(cse, {"open_call_back": 0, "open_not_connected": 0})
            if _is_call_back_data(data):
                bucket["open_call_back"] += 1
            if _is_not_connected_data(data):
                bucket["open_not_connected"] += 1

        period_stats: Dict[str, Dict[str, int]] = {}
        handling_stats: Dict[str, List[int]] = {}
        for _, data in self._iter_assigned_records(assigned_qs):
            if not _record_in_period(data, start_date, end_date):
                continue
            cse = _cse_name_from_data(data)
            bucket = period_stats.setdefault(cse, {"leads_assigned": 0, "resolved": 0})
            bucket["leads_assigned"] += 1
            if _normalize_status(data.get("resolution_status")) == "resolved":
                bucket["resolved"] += 1
            if _matches_handling_status(data, handling_status):
                seconds = _resolution_time_to_seconds(data.get("resolution_time"))
                if seconds is not None:
                    handling_stats.setdefault(cse, []).append(seconds)

        cse_memberships = list(
            TenantMembership.objects.filter(
                tenant=self.tenant,
                is_active=True,
                role__key__iexact="CSE",
            )
        )
        manager_map = get_manager_i_map(
            {
                str(membership.user_id)
                for membership in cse_memberships
                if membership.user_id
            },
            self.tenant,
        )
        manager_by_identity = {}
        for membership in cse_memberships:
            if not membership.user_id:
                continue
            manager_name = manager_map.get(str(membership.user_id), "")
            if membership.email:
                manager_by_identity[str(membership.email).strip().lower()] = manager_name
            if membership.name:
                manager_by_identity[str(membership.name).strip().lower()] = manager_name

        result = []
        for cse in sorted(set(open_stats.keys()) | set(period_stats.keys())):
            open_row = open_stats.get(cse, {})
            period_row = period_stats.get(cse, {})
            handling_values = handling_stats.get(cse, [])
            leads_assigned = period_row.get("leads_assigned", 0)
            resolved = period_row.get("resolved", 0)
            avg_handling = (sum(handling_values) / len(handling_values)) if handling_values else None
            result.append({
                "cse_name": cse,
                "manager_i_name": manager_by_identity.get(
                    str(cse).strip().lower(), ""
                ),
                "open_call_back": open_row.get("open_call_back", 0),
                "open_not_connected": open_row.get("open_not_connected", 0),
                "leads_assigned": leads_assigned,
                "resolved": resolved,
                "resolve_rate": (resolved / leads_assigned) if leads_assigned > 0 else None,
                "average_handling_time_seconds": (
                    float(avg_handling) if avg_handling is not None else None
                ),
                "handling_time_ticket_count": len(handling_values),
            })
        return result
