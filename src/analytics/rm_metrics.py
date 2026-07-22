"""
RM analytics for the Mixpanel-style analytics board (EventLog metrics).
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from django.db.models import Count, Q

from authz.models import TenantMembership
from user_settings.services import USER_KV_DAILY_LIMIT_KEY, kv_int_by_membership

from .services import (
    TeamMetricsService,
    TeamResolver,
    filter_user_ids_by_manager_i,
    get_manager_i_map,
    get_manager_i_options,
)

RM_ATTRIBUTE_KEYS = frozenset({"rm_name", "manager_i"})


class RmVisibilityResolver:
    """Tenant admins see all RMs; RM=self; other managers see team RMs."""

    TENANT_WIDE_ROLES = frozenset({"GM", "PYRO_ADMIN", "OWNER"})

    @staticmethod
    def _normalize_email(email: Optional[str]) -> Optional[str]:
        if not email:
            return None
        normalized = str(email).strip().lower()
        return normalized or None

    @classmethod
    def _tenant_rm_user_ids(cls, tenant) -> Set[str]:
        ids: Set[str] = set()
        memberships = TenantMembership.objects.filter(
            tenant=tenant,
            is_active=True,
        ).select_related("role")
        for membership in memberships:
            role_key = (membership.role.key or "").upper() if membership.role else ""
            if role_key != "RM":
                continue
            if membership.user_id:
                ids.add(str(membership.user_id))
        return ids

    @classmethod
    def resolve(cls, user_id: str, tenant) -> Tuple[Set[str], str]:
        membership = (
            TenantMembership.objects.filter(
                tenant=tenant, user_id=user_id, is_active=True
            )
            .select_related("role")
            .first()
        )
        if not membership:
            return set(), "self"

        role_key = (membership.role.key or "").upper() if membership.role else ""
        if role_key in cls.TENANT_WIDE_ROLES:
            return cls._tenant_rm_user_ids(tenant), "all"

        if role_key == "RM":
            return {str(user_id)}, "self"

        team_user_ids = TeamResolver.get_team_user_ids(str(user_id), tenant)
        team_rm_ids = cls._tenant_rm_user_ids(tenant) & team_user_ids
        if team_rm_ids:
            return team_rm_ids, "team"

        return set(), "none"

    @classmethod
    def rm_email_allowed(cls, email: str, allowed_user_ids: Set[str], tenant) -> bool:
        if not email:
            return False
        normalized = str(email).strip().lower()
        return TenantMembership.objects.filter(
            tenant=tenant,
            is_active=True,
            user_id__in=list(allowed_user_ids),
            email__iexact=normalized,
        ).exists()

    @classmethod
    def filter_user_ids_by_rm_name(
        cls,
        allowed_user_ids: Set[str],
        tenant,
        rm_names: Optional[List[str]],
    ) -> Set[str]:
        if not rm_names:
            return allowed_user_ids
        qs = TenantMembership.objects.filter(
            tenant=tenant,
            is_active=True,
            user_id__in=list(allowed_user_ids),
        )
        name_q = Q()
        for name in rm_names:
            normalized = str(name).strip()
            if normalized:
                name_q |= Q(email__iexact=normalized)
        if not name_q:
            return allowed_user_ids
        return {str(uid) for uid in qs.filter(name_q).values_list("user_id", flat=True)}


def _apply_rm_attribute_filters(
    allowed_user_ids: Set[str],
    tenant,
    attribute_filters: Optional[Dict[str, List[str]]],
) -> Set[str]:
    if not attribute_filters:
        return allowed_user_ids
    filtered = allowed_user_ids
    for key, values in attribute_filters.items():
        if key not in RM_ATTRIBUTE_KEYS or not values:
            continue
        if key == "rm_name":
            filtered = RmVisibilityResolver.filter_user_ids_by_rm_name(
                filtered, tenant, values
            )
        if key == "manager_i":
            filtered = filter_user_ids_by_manager_i(filtered, tenant, values)
    return filtered


class RmMetricsService:
    def __init__(
        self,
        tenant,
        allowed_user_ids: Set[str],
        visibility_scope: str = "self",
    ):
        self.tenant = tenant
        self.allowed_user_ids = allowed_user_ids
        self.visibility_scope = visibility_scope

    def _team_service(self, user_ids: Set[str]) -> TeamMetricsService:
        return TeamMetricsService(user_ids, self.tenant)

    def _rm_memberships(self, user_ids: Set[str]):
        return TenantMembership.objects.filter(
            tenant=self.tenant,
            is_active=True,
            user_id__in=list(user_ids),
        ).select_related("role")

    def _visible_rm_user_ids(self, user_ids: Set[str]) -> Set[str]:
        result: Set[str] = set()
        for membership in self._rm_memberships(user_ids):
            role_key = (membership.role.key or "").upper() if membership.role else ""
            if role_key == "RM" and membership.user_id:
                result.add(str(membership.user_id))
        return result

    def get_filter_options(self, visibility_scope: str = "all") -> Dict[str, Any]:
        user_ids = self._visible_rm_user_ids(self.allowed_user_ids)
        emails = sorted(
            {
                str(m.email).strip()
                for m in self._rm_memberships(user_ids)
                if m.email and str(m.email).strip()
            }
        )
        attributes = []
        if emails:
            attributes.append({"key": "rm_name", "label": "RM", "values": emails})
        if self.visibility_scope == "all":
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
            "attributes": attributes,
            "visibility_scope": visibility_scope,
        }

    def get_overview(
        self,
        start_date: date,
        end_date: date,
        attribute_filters: Optional[Dict[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        user_ids = _apply_rm_attribute_filters(
            self.allowed_user_ids, self.tenant, attribute_filters
        )
        user_ids = self._visible_rm_user_ids(user_ids)
        if not user_ids:
            return self._empty_overview()

        service = self._team_service(user_ids)
        queryset = service._get_base_queryset(start_date, end_date)
        connected_events = Q(
            event__in=[
                "lead.trial_activated",
                "lead.call_back_later",
                "lead.not_interested",
            ]
        )
        agg = queryset.aggregate(
            attendance=Count(
                "payload__user_id", filter=Q(event="lead.get_next_lead"), distinct=True
            ),
            calls_made=Count("id", filter=Q(event="lead.get_next_lead")),
            trials_activated=Count("id", filter=Q(event="lead.trial_activated")),
            calls_connected=Count("id", filter=connected_events),
            take_break_count=Count("id", filter=Q(event="agent.take_break")),
            not_interested_count=Count("id", filter=Q(event="lead.not_interested")),
        )
        calls_connected = agg["calls_connected"] or 0
        trials_activated = agg["trials_activated"] or 0
        connected_to_trial_ratio = (
            trials_activated / calls_connected if calls_connected > 0 else None
        )
        handling_stats = service._get_handling_time_stats_bulk(
            list(user_ids), start_date, end_date
        )
        handling_time_volume = sum(
            int(values.get("volume", 0)) for values in handling_stats.values()
        )
        # Preserve the established team-average calculation; volume is context.
        avg_time = service.get_average_time_spent(start_date, end_date)
        memberships = list(
            self._rm_memberships(user_ids).values_list("id", flat=True)
        )
        allotted = 0
        if memberships:
            allotted = int(
                sum(
                    kv_int_by_membership(
                        self.tenant, memberships, USER_KV_DAILY_LIMIT_KEY
                    ).values()
                )
            )

        return {
            "attendance": agg["attendance"] or 0,
            "calls_made": agg["calls_made"] or 0,
            "calls_connected": calls_connected,
            "trials_activated": trials_activated,
            "connected_to_trial_ratio": connected_to_trial_ratio,
            "average_time_spent_seconds": avg_time,
            "handling_time_volume": handling_time_volume,
            "take_break_count": agg["take_break_count"] or 0,
            "not_interested_count": agg["not_interested_count"] or 0,
            "allotted_leads": allotted,
            "unassigned_leads": (
                service.get_unassigned_leads_count()
                if self.visibility_scope == "all"
                else 0
            ),
        }

    @staticmethod
    def _empty_overview() -> Dict[str, Any]:
        return {
            "attendance": 0,
            "calls_made": 0,
            "calls_connected": 0,
            "trials_activated": 0,
            "connected_to_trial_ratio": None,
            "average_time_spent_seconds": None,
            "handling_time_volume": 0,
            "take_break_count": 0,
            "not_interested_count": 0,
            "allotted_leads": 0,
            "unassigned_leads": 0,
        }

    def get_member_breakdown(
        self,
        start_date: date,
        end_date: date,
        attribute_filters: Optional[Dict[str, List[str]]] = None,
    ) -> List[Dict[str, Any]]:
        user_ids = _apply_rm_attribute_filters(
            self.allowed_user_ids, self.tenant, attribute_filters
        )
        user_ids = self._visible_rm_user_ids(user_ids)
        if not user_ids:
            return []

        service = self._team_service(user_ids)
        raw_members = service.get_member_breakdown(start_date, end_date)
        allowed = user_ids
        manager_map = get_manager_i_map(user_ids, self.tenant)
        result = []
        for row in raw_members:
            uid = str(row.get("user_id") or "")
            if uid not in allowed:
                continue
            membership = (
                self._rm_memberships({uid}).values("email").first()
            )
            if not membership:
                continue
            limit = 0
            mid = (
                self._rm_memberships({uid}).values_list("id", flat=True).first()
            )
            if mid:
                limit = kv_int_by_membership(
                    self.tenant, [mid], USER_KV_DAILY_LIMIT_KEY
                ).get(mid, 0)
            result.append(
                {
                    "rm_name": membership.get("email") or "Unknown",
                    "manager_i_name": manager_map.get(uid, ""),
                    "user_id": uid,
                    "attendance": row.get("attendance", 0),
                    "calls_made": row.get("calls_made", 0),
                    "calls_connected": row.get("calls_connected", 0),
                    "trials_activated": row.get("trials_activated", 0),
                    "connected_to_trial_ratio": row.get("connected_to_trial_ratio"),
                    "average_time_spent_seconds": row.get(
                        "average_time_spent_seconds", 0.0
                    ),
                    "handling_time_volume": row.get("handling_time_volume", 0),
                    "take_break_count": row.get("take_break_count", 0),
                    "not_interested_count": row.get("not_interested_count", 0),
                    "allotted_leads": int(limit or 0),
                }
            )
        result.sort(key=lambda r: (r.get("rm_name") or "").lower())
        return result

    def get_time_series(
        self,
        start_date: date,
        end_date: date,
        attribute_filters: Optional[Dict[str, List[str]]] = None,
    ) -> List[Dict[str, Any]]:
        user_ids = _apply_rm_attribute_filters(
            self.allowed_user_ids, self.tenant, attribute_filters
        )
        user_ids = self._visible_rm_user_ids(user_ids)
        if not user_ids:
            return self._zero_filled_series(start_date, end_date)

        service = self._team_service(user_ids)
        queryset = service._get_base_queryset(start_date, end_date)
        daily_metrics = (
            queryset.extra(select={"day": "DATE(timestamp)"})
            .values("day", "event")
            .annotate(count=Count("id"))
            .order_by("day", "event")
        )

        day_metrics: Dict[Any, Dict[str, Any]] = {}
        for metric in daily_metrics:
            day = metric["day"]
            if hasattr(day, "isoformat"):
                day_key = day.isoformat() if hasattr(day, "isoformat") else str(day)
            else:
                day_key = str(day)
            if day_key not in day_metrics:
                day_metrics[day_key] = self._empty_day(day_key)

            event = metric["event"]
            count = metric["count"]
            row = day_metrics[day_key]
            if event == "lead.get_next_lead":
                row["attendance"] += count
                row["calls_made"] += count
            if event in (
                "lead.trial_activated",
                "lead.call_back_later",
                "lead.not_interested",
            ):
                row["calls_connected"] += count
            if event == "lead.trial_activated":
                row["trials_activated"] += count
            if event == "agent.take_break":
                row["take_break_count"] += count
            if event == "lead.not_interested":
                row["not_interested_count"] += count

        handling_by_day = service.get_handling_time_stats_by_day(
            list(user_ids), start_date, end_date
        )
        for row in day_metrics.values():
            cc = row["calls_connected"]
            ta = row["trials_activated"]
            row["connected_to_trial_ratio"] = ta / cc if cc > 0 else None
            handling = handling_by_day.get(str(row["date"]), {})
            row["average_time_spent_seconds"] = handling.get(
                "average_seconds"
            )
            row["handling_time_volume"] = int(handling.get("volume", 0))

        # A day may contain a completed handling session but no counted event row.
        for day, handling in handling_by_day.items():
            row = day_metrics.setdefault(day, self._empty_day(day))
            row["average_time_spent_seconds"] = handling.get("average_seconds")
            row["handling_time_volume"] = int(handling.get("volume", 0))

        return self._merge_zero_filled(start_date, end_date, day_metrics)

    @staticmethod
    def _empty_day(day_key: str) -> Dict[str, Any]:
        return {
            "date": day_key,
            "attendance": 0,
            "calls_made": 0,
            "calls_connected": 0,
            "trials_activated": 0,
            "connected_to_trial_ratio": None,
            "average_time_spent_seconds": None,
            "handling_time_volume": 0,
            "handling_time_volume": 0,
            "take_break_count": 0,
            "not_interested_count": 0,
        }

    def _zero_filled_series(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        return [
            self._empty_day((start_date + timedelta(days=i)).isoformat())
            for i in range((end_date - start_date).days + 1)
        ]

    def _merge_zero_filled(
        self,
        start_date: date,
        end_date: date,
        day_metrics: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        result = []
        current = start_date
        while current <= end_date:
            key = current.isoformat()
            result.append(day_metrics.get(key, self._empty_day(key)))
            current += timedelta(days=1)
        return result
