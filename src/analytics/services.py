"""
Analytics services for team metrics and event aggregation.
"""
import logging
from typing import Set, Optional, Dict, List, Any
from datetime import datetime, date, timedelta
from django.db.models import Q, Count, F, Avg, Min, Max
from django.utils import timezone

from authz.models import TenantMembership
from crm_records.models import EventLog, Record
from user_settings.services import (
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_DAILY_TARGET_KEY,
    kv_int_by_membership,
    sum_kv_int_for_memberships,
)
from django.db.models.expressions import RawSQL
from .constants import TRACKED_EVENTS, TERMINAL_EVENTS
from .utils import get_utc_datetime_range_for_ist_date


def _normalize_role_token(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .upper()
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
    )


def _is_manager_i_membership(membership) -> bool:
    """
    Detect Manager I / Manager roles used above CSE/RM teams.

    Accepts common keys/names: M, MI, Manager, Manager I, Manager1, manager_i, etc.
    Avoids unrelated *Manager roles (Engineering Manager, Area Sales Manager, …).
    """
    role = getattr(membership, "role", None)
    key = _normalize_role_token(getattr(role, "key", "") or "")
    name = _normalize_role_token(getattr(role, "name", "") or "")
    exact = {"M", "MI", "M1", "MANAGER", "MANAGERI", "MANAGER1", "MANAGERL1"}
    if key in exact or name in exact:
        return True
    # "Manager I", "Manager-I", "manager_i", "Manager I - RM", etc.
    if "MANAGERI" in key or key.startswith("MANAGERI"):
        return True
    if name == "MANAGERI" or name.startswith("MANAGERI"):
        return True
    return False


def _manager_i_label(membership) -> str:
    return str(membership.name or membership.email or "Manager I").strip()


def list_manager_i_memberships(tenant) -> List[Any]:
    """All active Manager I memberships in the tenant."""
    return [
        membership
        for membership in TenantMembership.objects.filter(
            tenant=tenant,
            is_active=True,
        ).select_related("role")
        if _is_manager_i_membership(membership)
    ]


def get_manager_i_options(tenant) -> List[str]:
    """
    Labels for the Manager I filter.

    Prefer people with the Manager I role. Also include nearest hierarchy
    parents of CSE/RM who themselves look like Manager I (covers custom keys).
    """
    labels = {
        _manager_i_label(membership)
        for membership in list_manager_i_memberships(tenant)
        if _manager_i_label(membership)
    }

    # Fallback / supplement: nearest Manager I ancestor of each CSE/RM.
    cse_rm = TenantMembership.objects.filter(
        tenant=tenant,
        is_active=True,
    ).filter(
        Q(role__key__iexact="CSE") | Q(role__key__iexact="RM")
    ).select_related("role")
    manager_map = get_manager_i_map(
        {str(m.user_id) for m in cse_rm if m.user_id},
        tenant,
    )
    labels.update(name for name in manager_map.values() if name and str(name).strip())
    return sorted(labels)


def get_manager_i_map(user_ids: Set[str], tenant) -> Dict[str, str]:
    """Map each user ID to the nearest Manager I ancestor's display name."""
    memberships = list(
        TenantMembership.objects.filter(tenant=tenant, is_active=True).select_related(
            "role", "user_parent_id__role"
        )
    )
    by_id = {membership.id: membership for membership in memberships}
    by_user_id = {
        str(membership.user_id): membership
        for membership in memberships
        if membership.user_id
    }
    result: Dict[str, str] = {}
    for user_id in user_ids:
        membership = by_user_id.get(str(user_id))
        visited = set()
        parent_id = membership.user_parent_id_id if membership else None
        while parent_id and parent_id not in visited:
            visited.add(parent_id)
            parent = by_id.get(parent_id)
            if not parent:
                break
            if _is_manager_i_membership(parent):
                result[str(user_id)] = _manager_i_label(parent)
                break
            parent_id = parent.user_parent_id_id
    return result


def filter_user_ids_by_manager_i(
    user_ids: Set[str],
    tenant,
    manager_names: Optional[List[str]],
) -> Set[str]:
    """Keep only users who report (directly or indirectly) to the selected Manager I(s)."""
    if not manager_names:
        return user_ids

    selected = {
        str(name).strip().lower() for name in manager_names if str(name).strip()
    }
    if not selected:
        return user_ids

    team_ids: Set[str] = set()
    for membership in list_manager_i_memberships(tenant):
        labels = {
            _manager_i_label(membership).lower(),
            str(membership.email or "").strip().lower(),
            str(membership.name or "").strip().lower(),
        }
        labels.discard("")
        if not (labels & selected):
            continue
        if not membership.user_id:
            continue
        # Full hierarchy under this Manager I.
        team_ids.update(
            TeamResolver.get_team_user_ids(str(membership.user_id), tenant)
        )
        team_ids.discard(str(membership.user_id))

    # Also keep anyone whose nearest Manager I ancestor matches the selection.
    # Keeps filtering consistent with manager_i_name on member rows.
    manager_map = get_manager_i_map(user_ids, tenant)
    for user_id in user_ids:
        label = str(manager_map.get(str(user_id), "") or "").strip().lower()
        if label in selected:
            team_ids.add(str(user_id))

    if not team_ids:
        return set()
    return {str(user_id) for user_id in user_ids if str(user_id) in team_ids}


class TeamResolver:
    """
    Resolves team members for a given manager using TenantMembership hierarchy.
    """
    
    @staticmethod
    def get_reports_count(manager_user_id: str, tenant) -> int:
        """
        Get count of all direct and indirect reports (excluding manager).
        Uses get_team_user_ids (single bulk query) then subtracts 1 for manager.
        
        Args:
            manager_user_id: UUID of the manager user
            tenant: Tenant instance
            
        Returns:
            Count of reports (direct + indirect, excluding manager)
        """
        logger = logging.getLogger(__name__)
        team_ids = TeamResolver.get_team_user_ids(manager_user_id, tenant)
        reports_count = max(0, len(team_ids) - 1)  # Exclude manager
        logger.info(f"[TeamResolver] Total reports count (excluding manager): {reports_count}")
        return reports_count
    
    @staticmethod
    def get_team_user_ids(manager_user_id: str, tenant) -> Set[str]:
        """
        Get all user_ids in the team (manager + all direct and indirect reports).
        Uses a single bulk query to avoid N+1 when traversing the hierarchy.
        
        Args:
            manager_user_id: UUID of the manager user
            tenant: Tenant instance
            
        Returns:
            Set of user_id strings (UUIDs) including manager and all team members
        """
        logger = logging.getLogger(__name__)
        logger.info(f"[TeamResolver] Starting team resolution for manager_user_id: {manager_user_id}, tenant: {tenant.id}")
        
        team_ids = {str(manager_user_id)}
        
        # Single bulk query: fetch ALL active TenantMemberships for this tenant
        # Avoids N+1 from recursive per-parent queries
        all_memberships = list(
            TenantMembership.objects.filter(
                tenant=tenant,
                is_active=True
            ).values('id', 'user_id', 'user_parent_id_id')
        )
        
        # Build parent_id -> [membership] map for in-memory traversal
        parent_to_children: Dict[int, List[Dict]] = {}
        for m in all_memberships:
            parent_id = m.get('user_parent_id_id')
            if parent_id is not None:
                parent_to_children.setdefault(parent_id, []).append(m)
        
        # Find manager's membership
        manager_membership = next(
            (m for m in all_memberships if str(m.get('user_id')) == str(manager_user_id)),
            None
        )
        if not manager_membership:
            logger.warning(f"[TeamResolver] Manager TenantMembership NOT FOUND for user_id: {manager_user_id}, tenant: {tenant.id}")
            return team_ids
        
        def collect_reports_recursive(parent_id: int) -> Set[str]:
            """Traverse hierarchy in memory (no DB queries)."""
            report_ids = set()
            for m in parent_to_children.get(parent_id, []):
                if m.get('user_id'):
                    user_id_str = str(m['user_id'])
                    report_ids.add(user_id_str)
                    report_ids.update(collect_reports_recursive(m['id']))
            return report_ids
        
        nested_ids = collect_reports_recursive(manager_membership['id'])
        team_ids.update(nested_ids)
        
        logger.info(f"[TeamResolver] Final team_ids: {team_ids} (total: {len(team_ids)})")
        return team_ids


class TeamMetricsService:
    """
    Service for calculating team metrics from EventLog.
    """
    
    def __init__(self, team_user_ids: Set[str], tenant):
        self.team_user_ids = team_user_ids
        self.tenant = tenant
    
    def _get_team_user_ids_excluding_manager(self, manager_user_id: Optional[str] = None) -> Set[str]:
        """Get team user IDs excluding the manager."""
        if manager_user_id:
            manager_user_id_str = str(manager_user_id)
            return {uid for uid in self.team_user_ids if str(uid) != manager_user_id_str}
        return self.team_user_ids
    
    def _get_base_queryset(self, start_date: Optional[date] = None, end_date: Optional[date] = None, manager_user_id: Optional[str] = None):
        """Get base queryset filtered by tenant, team, and date range."""
        team_user_ids_to_use = self._get_team_user_ids_excluding_manager(manager_user_id) if manager_user_id else self.team_user_ids

        queryset = EventLog.objects.filter(
            tenant=self.tenant,
            event__in=TRACKED_EVENTS,
            payload__user_id__in=list(team_user_ids_to_use),
        )

        if start_date:
            utc_start, _ = get_utc_datetime_range_for_ist_date(start_date)
            queryset = queryset.filter(timestamp__gte=utc_start)
        if end_date:
            _, utc_end = get_utc_datetime_range_for_ist_date(end_date)
            queryset = queryset.filter(timestamp__lte=utc_end)

        return queryset
    
    def get_attendance(self, target_date: date, manager_user_id: Optional[str] = None) -> int:
        """Get count of distinct users with get_next_lead event on target_date (excluding manager)."""
        return self._get_base_queryset(
            start_date=target_date,
            end_date=target_date,
            manager_user_id=manager_user_id
        ).filter(
            event='lead.get_next_lead'
        ).values('payload__user_id').distinct().count()
    
    def get_calls_made(self, start_date: Optional[date] = None, end_date: Optional[date] = None, manager_user_id: Optional[str] = None) -> int:
        """Get total calls made (number of get_next_lead events, excluding manager)."""
        return self._get_base_queryset(start_date, end_date, manager_user_id).filter(
            event='lead.get_next_lead'
        ).count()
    
    def get_trials_activated(self, start_date: Optional[date] = None, end_date: Optional[date] = None, manager_user_id: Optional[str] = None) -> int:
        """Get count of trial_activated events (excluding manager)."""
        return self._get_base_queryset(start_date, end_date, manager_user_id).filter(
            event='lead.trial_activated'
        ).count()
    
    def get_connected_to_trial_ratio(self, start_date: Optional[date] = None, end_date: Optional[date] = None, manager_user_id: Optional[str] = None) -> Optional[float]:
        """Calculate connected to trial ratio (excluding manager)."""
        calls_connected = self._get_base_queryset(start_date, end_date, manager_user_id).filter(
            event__in=[
            'lead.trial_activated',
            'lead.call_back_later',
            'lead.not_interested'
        ]
        ).count()
        
        trials = self.get_trials_activated(start_date, end_date, manager_user_id)
        
        if calls_connected == 0:
            return None
        
        return trials / calls_connected if calls_connected > 0 else None
    
    def _get_average_time_spent_bulk(
        self,
        user_ids: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, float]:
        stats = self._get_handling_time_stats_bulk(user_ids, start_date, end_date)
        return {
            user_id: float(values["average_seconds"])
            for user_id, values in stats.items()
        }

    def _get_handling_time_stats_bulk(
        self,
        user_ids: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Dict[str, float]]:
        """
        Average handling time and the exact lead volume used as its denominator.

        For each lead session: time from ``lead.get_next_lead`` to the first
        subsequent call outcome by the same user on that record. The established
        denominator is get-next-lead volume minus take-break volume.
        """
        if not user_ids:
            return {}

        queryset = EventLog.objects.filter(
            tenant=self.tenant,
            event__in=TRACKED_EVENTS,
            payload__user_id__in=user_ids,
        )
        if start_date:
            utc_start, _ = get_utc_datetime_range_for_ist_date(start_date)
            queryset = queryset.filter(timestamp__gte=utc_start)
        if end_date:
            _, utc_end = get_utc_datetime_range_for_ist_date(end_date)
            queryset = queryset.filter(timestamp__lte=utc_end)

        events = list(queryset.values("event", "timestamp", "record_id", "payload"))

        CALLS_MADE_EVENTS = {
            "lead.call_not_connected",
            "lead.call_back_later",
            "lead.trial_activated",
            "lead.not_interested",
        }

        result: Dict[str, Dict[str, float]] = {}
        for user_id in user_ids:
            user_id_str = str(user_id)
            get_next_by_record: Dict[Any, List[Any]] = {}
            calls_made_by_record: Dict[Any, List[Any]] = {}
            take_break_count = 0

            for e in events:
                payload_user = e.get("payload") or {}
                if str(payload_user.get("user_id")) != user_id_str:
                    continue
                record_id = e.get("record_id")
                event_type = e.get("event")
                ts = e.get("timestamp")

                if event_type == "lead.get_next_lead":
                    get_next_by_record.setdefault(record_id, []).append(ts)
                elif event_type in CALLS_MADE_EVENTS:
                    calls_made_by_record.setdefault(record_id, []).append(ts)
                elif event_type == "agent.take_break":
                    take_break_count += 1

            time_sum = 0.0
            for record_id, get_next_list in get_next_by_record.items():
                sorted_calls = sorted(calls_made_by_record.get(record_id, []))
                if not sorted_calls:
                    continue
                for get_next_ts in sorted(get_next_list):
                    for call_ts in sorted_calls:
                        if call_ts > get_next_ts:
                            diff = (call_ts - get_next_ts).total_seconds()
                            if diff > 0:
                                time_sum += diff
                            break

            get_next_count = sum(
                len(values) for values in get_next_by_record.values()
            )
            volume = max(0, get_next_count - take_break_count)
            result[user_id_str] = {
                "average_seconds": (
                    time_sum / volume if volume > 0 else 0.0
                ),
                "volume": float(volume),
                "total_seconds": time_sum,
            }

        return result

    def get_handling_time_stats_by_day(
        self,
        user_ids: List[str],
        start_date: date,
        end_date: date,
    ) -> Dict[str, Dict[str, float]]:
        """Return weighted handling average and volume grouped by work-start day."""
        if not user_ids:
            return {}

        utc_start, _ = get_utc_datetime_range_for_ist_date(start_date)
        _, utc_end = get_utc_datetime_range_for_ist_date(end_date)
        events = list(
            EventLog.objects.filter(
                tenant=self.tenant,
                event__in=TRACKED_EVENTS,
                payload__user_id__in=user_ids,
                timestamp__gte=utc_start,
                timestamp__lte=utc_end,
            ).values("event", "timestamp", "record_id", "payload")
        )
        outcome_events = {
            "lead.call_not_connected",
            "lead.call_back_later",
            "lead.trial_activated",
            "lead.not_interested",
        }
        starts: Dict[tuple, List[Any]] = {}
        outcomes: Dict[tuple, List[Any]] = {}
        breaks_by_day: Dict[str, int] = {}
        for event in events:
            payload = event.get("payload") or {}
            user_id = str(payload.get("user_id") or "")
            key = (user_id, event.get("record_id"))
            if event.get("event") == "lead.get_next_lead":
                starts.setdefault(key, []).append(event.get("timestamp"))
            elif event.get("event") in outcome_events:
                outcomes.setdefault(key, []).append(event.get("timestamp"))
            elif event.get("event") == "agent.take_break":
                timestamp = event.get("timestamp")
                local_timestamp = (
                    timezone.localtime(timestamp)
                    if timezone.is_aware(timestamp)
                    else timestamp
                )
                day = local_timestamp.date().isoformat()
                breaks_by_day[day] = breaks_by_day.get(day, 0) + 1

        totals: Dict[str, Dict[str, float]] = {}
        for key, start_times in starts.items():
            sorted_outcomes = sorted(outcomes.get(key, []))
            for start_time in sorted(start_times):
                local_start = (
                    timezone.localtime(start_time)
                    if timezone.is_aware(start_time)
                    else start_time
                )
                day = local_start.date().isoformat()
                bucket = totals.setdefault(
                    day,
                    {
                        "total_seconds": 0.0,
                        "get_next_count": 0.0,
                        "volume": 0.0,
                    },
                )
                bucket["get_next_count"] += 1
                for outcome_time in sorted_outcomes:
                    if outcome_time <= start_time:
                        continue
                    seconds = (outcome_time - start_time).total_seconds()
                    if seconds > 0:
                        bucket["total_seconds"] += seconds
                    break

        for day, bucket in totals.items():
            volume = max(
                0, int(bucket["get_next_count"]) - breaks_by_day.get(day, 0)
            )
            bucket["volume"] = float(volume)
            bucket["average_seconds"] = (
                bucket["total_seconds"] / volume if volume > 0 else 0.0
            )
        return totals

    def get_average_time_spent_per_user(self, user_id: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Optional[float]:
        """
        Average active work time per lead for one user.

        Only counts sessions where the RM got a lead and later recorded a call
        outcome on that same lead (time they actually worked it).
        """
        logger = logging.getLogger(__name__)
        logger.info(
            f"[TeamMetricsService] Calculating average time spent for user {user_id} "
            f"from {start_date} to {end_date}"
        )
        avg_by_user = self._get_average_time_spent_bulk(
            [str(user_id)], start_date, end_date
        )
        return avg_by_user.get(str(user_id), 0.0)    
    def get_average_time_spent(self, start_date: Optional[date] = None, end_date: Optional[date] = None, manager_user_id: Optional[str] = None) -> Optional[float]:
        """
        Calculate team-wide average time spent per lead (excluding manager).
        Averages the per-user averages. Uses bulk fetch to avoid N+1.
        """
        logger = logging.getLogger(__name__)
        logger.info(f"[TeamMetricsService] Calculating team average time spent for {start_date} to {end_date} (excluding manager)")
        
        team_user_ids_to_use = list(
            self._get_team_user_ids_excluding_manager(manager_user_id) if manager_user_id else self.team_user_ids
        )
        if not team_user_ids_to_use:
            return None
        
        avg_by_user = self._get_average_time_spent_bulk(
            [str(uid) for uid in team_user_ids_to_use],
            start_date,
            end_date,
        )
        user_averages = [v for v in avg_by_user.values() if v is not None]

        if not user_averages:
            logger.warning(f"[TeamMetricsService] No user averages found for team average calculation")
            return None
        
        team_avg = sum(user_averages) / len(user_averages)
        logger.info(f"[TeamMetricsService] Team average time spent: {team_avg:.2f}s (from {len(user_averages)} users)")
        
        return team_avg
    
    def get_trail_target(self, manager_user_id: Optional[str] = None) -> int:
        """
        Calculate trail target by summing DAILY_TARGET KV values for all team members (excluding manager).
        Returns 0 if no targets are set.
        """
        team_user_ids_to_use = self._get_team_user_ids_excluding_manager(manager_user_id) if manager_user_id else self.team_user_ids
        logger = logging.getLogger(__name__)
        logger.info(f"[TeamMetricsService] Calculating trail target for {len(team_user_ids_to_use)} team members (excluding manager)")
        
        # Get TenantMembership IDs for all team user_ids (excluding manager)
        memberships = TenantMembership.objects.filter(
            tenant=self.tenant,
            user_id__in=list(team_user_ids_to_use),
            is_active=True
        ).values_list('id', flat=True)
        
        logger.info(f"[TeamMetricsService] Found {memberships.count()} TenantMembership records for team members")
        
        membership_ids = list(memberships)
        target_sum = sum_kv_int_for_memberships(
            self.tenant, membership_ids, USER_KV_DAILY_TARGET_KEY
        )
        
        logger.info(f"[TeamMetricsService] Trail target calculated: {target_sum}")
        return int(target_sum)
    
    _UNASSIGNED_WHERE = """
        (
            (data->>'assigned_to') IS NULL
            OR TRIM(COALESCE(data->>'assigned_to', '')) = ''
            OR LOWER(TRIM(COALESCE(data->>'assigned_to', ''))) IN ('null', 'none')
        )
    """

    def _get_unassigned_leads_qs(self):
        return Record.objects.filter(
            tenant=self.tenant,
            entity_type="lead",
        ).extra(where=[self._UNASSIGNED_WHERE])

    def get_unassigned_leads_count(self) -> int:
        """Count leads in the tenant where assigned_to is null/empty/none."""
        return self._get_unassigned_leads_qs().count()

    def get_unassigned_leads_breakdown(
        self,
        lead_source_filter: Optional[List[str]] = None,
        lead_stage_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Breakdown of unassigned leads by lead_source and lead_stage.
        lead_source_filter accepts a list of sources for multi-select.
        """
        from django.db import connection

        sql = f"""
            SELECT
                COALESCE(NULLIF(TRIM(data->>'lead_source'), ''), 'Unknown') AS lead_source,
                COALESCE(NULLIF(TRIM(data->>'lead_stage'), ''), 'Unknown') AS lead_stage,
                COUNT(*)::int AS count
            FROM records
            WHERE tenant_id = %s
              AND entity_type = 'lead'
              AND ({self._UNASSIGNED_WHERE.strip()})
            GROUP BY 1, 2
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, [self.tenant.id])
            inventory = [
                {"lead_source": row[0], "lead_stage": row[1], "count": row[2]}
                for row in cursor.fetchall()
            ]

        source_filter = set(lead_source_filter) if lead_source_filter else None
        available_sources = sorted({row["lead_source"] for row in inventory})
        available_stages = sorted({row["lead_stage"] for row in inventory})

        by_source_counts: Dict[str, int] = {}
        by_status_counts: Dict[str, int] = {}
        total = 0

        for row in inventory:
            lead_source = row["lead_source"]
            lead_stage = row["lead_stage"]
            count = row["count"]
            if source_filter and lead_source not in source_filter:
                continue
            if lead_stage_filter and lead_stage != lead_stage_filter:
                continue
            total += count
            by_source_counts[lead_source] = by_source_counts.get(lead_source, 0) + count
            by_status_counts[lead_stage] = by_status_counts.get(lead_stage, 0) + count

        by_source = [
            {"lead_source": source, "count": count}
            for source, count in sorted(
                by_source_counts.items(), key=lambda item: (-item[1], item[0])
            )
        ]
        by_status = [
            {"lead_stage": stage, "count": count}
            for stage, count in sorted(
                by_status_counts.items(), key=lambda item: (-item[1], item[0])
            )
        ]

        return {
            "total": total,
            "by_source": by_source,
            "by_status": by_status,
            "available_sources": available_sources,
            "available_stages": available_stages,
        }

    def get_allotted_leads(self, manager_user_id: Optional[str] = None) -> int:
        """
        Calculate allotted leads by summing DAILY_LIMIT KV values for all team members (excluding manager).
        Returns 0 if no limits are set.
        """
        team_user_ids_to_use = self._get_team_user_ids_excluding_manager(manager_user_id) if manager_user_id else self.team_user_ids
        logger = logging.getLogger(__name__)
        logger.info(f"[TeamMetricsService] Calculating allotted leads for {len(team_user_ids_to_use)} team members (excluding manager)")
        
        # Get TenantMembership IDs for all team user_ids (excluding manager)
        memberships = TenantMembership.objects.filter(
            tenant=self.tenant,
            user_id__in=list(team_user_ids_to_use),
            is_active=True
        ).values_list('id', flat=True)
        
        logger.info(f"[TeamMetricsService] Found {memberships.count()} TenantMembership records for team members")
        
        membership_ids = list(memberships)
        limit_sum = sum_kv_int_for_memberships(
            self.tenant, membership_ids, USER_KV_DAILY_LIMIT_KEY
        )
        
        logger.info(f"[TeamMetricsService] Allotted leads calculated: {limit_sum}")
        return int(limit_sum)
    
    def get_overview(self, target_date: date, manager_user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get overview metrics for a specific date.
        
        Args:
            target_date: Date to get metrics for
            manager_user_id: Optional manager user_id to calculate reports count (excludes manager from total_team_size)
        """
        if manager_user_id:
            total_team_size = TeamResolver.get_reports_count(manager_user_id, self.tenant)
        else:
            total_team_size = max(0, len(self.team_user_ids) - 1)

        queryset = self._get_base_queryset(
            start_date=target_date,
            end_date=target_date,
            manager_user_id=manager_user_id,
        )
        connected_events = Q(
            event__in=[
                "lead.trial_activated",
                "lead.call_back_later",
                "lead.not_interested",
            ]
        )
        agg = queryset.aggregate(
            attendance=Count("payload__user_id", filter=Q(event="lead.get_next_lead"), distinct=True),
            calls_made=Count("id", filter=Q(event="lead.get_next_lead")),
            trials_activated=Count("id", filter=Q(event="lead.trial_activated")),
            calls_connected=Count("id", filter=connected_events),
        )

        calls_connected = agg["calls_connected"] or 0
        trials_activated = agg["trials_activated"] or 0
        connected_to_trial_ratio = (
            trials_activated / calls_connected if calls_connected > 0 else None
        )

        return {
            "attendance": agg["attendance"] or 0,
            "total_team_size": total_team_size,
            "calls_made": agg["calls_made"] or 0,
            "trials_activated": trials_activated,
            "connected_to_trial_ratio": connected_to_trial_ratio,
            "average_time_spent_seconds": self.get_average_time_spent(
                target_date, target_date, manager_user_id
            ),
            "trail_target": self.get_trail_target(manager_user_id),
            "allotted_leads": self.get_allotted_leads(manager_user_id),
            "unassigned_leads": self.get_unassigned_leads_count(),
        }
    
    def get_member_breakdown(self, start_date: Optional[date] = None, end_date: Optional[date] = None, manager_user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get per-member metrics breakdown (excluding manager if manager_user_id is provided)."""
        queryset = self._get_base_queryset(start_date, end_date, manager_user_id)

        team_user_ids_to_use = self._get_team_user_ids_excluding_manager(manager_user_id) if manager_user_id else self.team_user_ids
        user_metrics = {}
        for user_id in team_user_ids_to_use:
            user_id_str = str(user_id)
            user_metrics[user_id_str] = {
                'user_id': user_id_str,
                'total_events': 0,
                'calls_made': 0,
                'calls_connected': 0,
                'trials_activated': 0,
                'get_next_lead_count': 0,
                'take_break_count': 0,
                'not_interested_count': 0,
            }

        member_metrics = queryset.values('payload__user_id', 'event').annotate(
            count=Count('id')
        ).order_by('payload__user_id', 'event')
        
        for metric in member_metrics:
            user_id = metric['payload__user_id']
            if user_id not in user_metrics:
                user_metrics[user_id] = {
                    'user_id': user_id,
                    'total_events': 0,
                    'calls_made': 0,
                    'calls_connected': 0,
                    'trials_activated': 0,
                    'get_next_lead_count': 0,
                    'take_break_count': 0,
                    'not_interested_count': 0,
                }
            
            user_metrics[user_id]['total_events'] += metric['count']
            
            event = metric['event']
            if event == 'lead.get_next_lead':
                user_metrics[user_id]['calls_made'] += metric['count']
                user_metrics[user_id]['get_next_lead_count'] += metric['count']
            if event == 'lead.call_back_later':
                user_metrics[user_id]['calls_connected'] += metric['count']
            if event == 'lead.trial_activated':
                user_metrics[user_id]['trials_activated'] += metric['count']
                # Trial activated is also a connected call outcome
                user_metrics[user_id]['calls_connected'] += metric['count']
            if event == 'lead.not_interested':
                user_metrics[user_id]['not_interested_count'] += metric['count']
                # Not interested is also a connected call outcome
                user_metrics[user_id]['calls_connected'] += metric['count']
            if event == 'agent.take_break':
                user_metrics[user_id]['take_break_count'] += metric['count']
        
        # Fetch user emails/names from TenantMembership for all users in user_metrics
        # (includes team_user_ids plus any additional users found in events)
        all_user_ids = list(user_metrics.keys())
        memberships = TenantMembership.objects.filter(
            tenant=self.tenant,
            user_id__in=all_user_ids,
            is_active=True
        ).values('id', 'user_id', 'email')
        
        # Create a mapping of user_id to email and membership_id
        user_id_to_email = {}
        membership_id_to_user_id = {}
        for m in memberships:
            user_id_str = str(m['user_id'])
            user_id_to_email[user_id_str] = m['email']
            membership_id_to_user_id[m['id']] = user_id_str

        membership_ids = [m['id'] for m in memberships]
        target_by_membership = kv_int_by_membership(
            self.tenant, membership_ids, USER_KV_DAILY_TARGET_KEY
        )
        user_id_to_daily_target = {}
        for membership_id, daily_target in target_by_membership.items():
            user_id_str = membership_id_to_user_id.get(membership_id)
            if user_id_str:
                user_id_to_daily_target[user_id_str] = daily_target

        user_ids_for_avg = [
            uid for uid in user_metrics.keys()
            if not manager_user_id or str(uid) != str(manager_user_id)
        ]
        handling_stats_by_user = self._get_handling_time_stats_bulk(
            user_ids_for_avg, start_date, end_date
        ) if user_ids_for_avg else {}
        
        result = []
        manager_user_id_str = str(manager_user_id) if manager_user_id else None
        
        for user_id_str, metrics in user_metrics.items():
            if manager_user_id_str and user_id_str == manager_user_id_str:
                continue
            
            member_data = metrics.copy()
            member_data['email'] = user_id_to_email.get(user_id_str, 'Unknown')
            member_data['daily_target'] = user_id_to_daily_target.get(user_id_str, 0)
            
            # Per-user attendance: 1 if they have get_next_lead events, 0 otherwise
            member_data['attendance'] = 1 if metrics['get_next_lead_count'] > 0 else 0
            
            # Per-user connected_to_trial_ratio: trials_activated / calls_connected
            calls_connected = metrics['calls_connected']
            trials_activated = metrics['trials_activated']
            if calls_connected > 0:
                member_data['connected_to_trial_ratio'] = trials_activated / calls_connected
            else:
                member_data['connected_to_trial_ratio'] = None
            
            # Per-user average and the completed lead volume behind that average.
            handling_stats = handling_stats_by_user.get(user_id_str, {})
            member_data['average_time_spent_seconds'] = handling_stats.get(
                "average_seconds", 0.0
            )
            member_data['handling_time_volume'] = int(
                handling_stats.get("volume", 0)
            )
            
            result.append(member_data)

        return result
    
    def get_event_breakdown(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Dict[str, int]:
        """Get event type breakdown (count per event type)."""
        queryset = self._get_base_queryset(start_date, end_date)
        
        event_counts = queryset.values('event').annotate(
            count=Count('id')
        ).order_by('event')
        
        return {item['event']: item['count'] for item in event_counts}
    
    def get_time_series(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Get daily time series data over date range."""
        queryset = self._get_base_queryset(start_date, end_date)
        
        # Group by date and event type
        daily_metrics = queryset.extra(
            select={'day': "DATE(timestamp)"}
        ).values('day', 'event').annotate(
            count=Count('id')
        ).order_by('day', 'event')
        
        # Aggregate by day
        day_metrics = {}
        for metric in daily_metrics:
            day = metric['day']
            if day not in day_metrics:
                day_metrics[day] = {
                    'date': day,
                    'attendance': 0,
                    'calls_made': 0,
                    'trials_activated': 0,
                    'total_events': 0,
                }
            
            event = metric['event']
            day_metrics[day]['total_events'] += metric['count']
            
            if event == 'lead.get_next_lead':
                day_metrics[day]['attendance'] += metric['count']
            if event in ['lead.call_not_connected', 'lead.call_back_later']:
                day_metrics[day]['calls_made'] += metric['count']
            if event == 'lead.trial_activated':
                day_metrics[day]['trials_activated'] += metric['count']
        
        return list(day_metrics.values())

