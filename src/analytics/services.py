"""
Analytics services for team metrics and event aggregation.
"""
import hashlib
import json
import logging
from contextlib import contextmanager
from typing import Set, Optional, Dict, List, Any
from datetime import datetime, date, timedelta
import psycopg2
import pytz
from django.conf import settings
from django.core.cache import cache
from django.db import connection, transaction
from django.db.models import Q, Count, F, Avg, Min, Max
from django.utils import timezone

from authz.models import TenantMembership
from crm_records.models import EventLog, Record
from user_settings.services import (
    USER_KV_DAILY_LIMIT_KEY,
    USER_KV_DAILY_TARGET_KEY,
    get_rm_daily_targets_sum,
    kv_int_by_membership,
    sum_kv_int_for_memberships,
)
from django.db.models.expressions import RawSQL
from .constants import TRACKED_EVENTS, TERMINAL_EVENTS
from .utils import get_utc_datetime_range_for_ist_date

# Team hierarchy changes rarely (a manager reassignment, not a per-minute
# event) but get_team_user_ids is called on nearly every manager-scoped
# request across this file AND analytics/views.py's RM PRD endpoints — each
# call re-fetches and re-traverses every active TenantMembership in the
# tenant from scratch. A short TTL turns N redundant full-tenant scans per
# page load (one per manager-scoped endpoint) into one, without meaningfully
# delaying a real reassignment showing up.
_TEAM_USER_IDS_TTL_SECONDS = 60


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

        Cached per (tenant, manager_user_id) for _TEAM_USER_IDS_TTL_SECONDS —
        see the module-level comment on that constant.

        Args:
            manager_user_id: UUID of the manager user
            tenant: Tenant instance

        Returns:
            Set of user_id strings (UUIDs) including manager and all team members
        """
        cache_key = f"team_user_ids:{tenant.id}:{manager_user_id}"
        cached_ids = cache.get(cache_key)
        if cached_ids is not None:
            return cached_ids
        team_ids = TeamResolver._resolve_team_user_ids(manager_user_id, tenant)
        cache.set(cache_key, team_ids, timeout=_TEAM_USER_IDS_TTL_SECONDS)
        return team_ids

    @staticmethod
    def _resolve_team_user_ids(manager_user_id: str, tenant) -> Set[str]:
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
        """
        Calculate average time spent per lead for multiple users in bulk (avoids N+1).
        Returns dict mapping user_id -> average_seconds.
        """
        logger = logging.getLogger(__name__)
        if not user_ids:
            return {}
        
        # Single bulk query: fetch ALL relevant events for all users at once
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
        
        events = list(queryset.values('event', 'timestamp', 'record_id', 'payload'))
        
        # Group by user_id in memory
        CALLS_MADE_EVENTS = [
            'lead.call_not_connected',
            'lead.call_back_later',
            'lead.trial_activated',
            'lead.not_interested',
        ]
        
        result = {}
        for user_id in user_ids:
            user_id_str = str(user_id)
            get_next_by_record = {}
            calls_made_by_record = {}
            take_break_count = 0
            
            for e in events:
                payload_user = e.get('payload') or {}
                if str(payload_user.get('user_id')) != user_id_str:
                    continue
                record_id = e.get('record_id')
                event_type = e.get('event')
                ts = e.get('timestamp')
                
                if event_type == 'lead.get_next_lead':
                    get_next_by_record.setdefault(record_id, []).append(ts)
                elif event_type in CALLS_MADE_EVENTS:
                    calls_made_by_record.setdefault(record_id, []).append(ts)
                elif event_type == 'agent.take_break':
                    take_break_count += 1
            
            get_next_count = sum(len(v) for v in get_next_by_record.values())
            denominator = get_next_count - take_break_count
            if denominator <= 0:
                result[user_id_str] = 0.0
                continue
            
            time_sum = 0.0
            for record_id, get_next_list in get_next_by_record.items():
                calls_list = calls_made_by_record.get(record_id, [])
                sorted_calls = sorted(calls_list)
                for get_next_ts in sorted(get_next_list):
                    for call_ts in sorted_calls:
                        if call_ts > get_next_ts:
                            time_sum += (call_ts - get_next_ts).total_seconds()
                            break
            
            result[user_id_str] = time_sum / denominator if time_sum > 0 else 0.0
        
        return result

    def get_average_time_spent_per_user(self, user_id: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Optional[float]:
        """
        Calculate average time spent per lead for a specific user.
        Formula: sum of (calls_made time - get_next_lead time) / (get_next_lead events - take_break events)
        
        Where:
        - calls_made events are: lead.call_not_connected, lead.call_back_later, lead.trial_activated, lead.not_interested
        - For each get_next_lead by this user, find the first calls_made event by same user for same record
        """
        logger = logging.getLogger(__name__)
        logger.info(f"[TeamMetricsService] Calculating average time spent for user {user_id} from {start_date} to {end_date}")
        
        # Get all get_next_lead events for this user
        get_next_lead_events = self._get_base_queryset(start_date, end_date).filter(
            event='lead.get_next_lead',
            payload__user_id=user_id
        ).order_by('record_id', 'timestamp')
        
        # Get all calls_made events (any call outcome) for this user
        # This includes: call_not_connected, call_back_later, trial_activated, not_interested
        calls_made_events = self._get_base_queryset(start_date, end_date).filter(
            event__in=[
                'lead.call_not_connected',
                'lead.call_back_later',
                'lead.trial_activated',
                'lead.not_interested'
            ],
            payload__user_id=user_id
        ).order_by('record_id', 'timestamp')
        
        # Get all take_break events for this user
        take_break_events = self._get_base_queryset(start_date, end_date).filter(
            event='agent.take_break',
            payload__user_id=user_id
        )
        
        get_next_count = get_next_lead_events.count()
        take_break_count = take_break_events.count()
        
        logger.info(f"[TeamMetricsService] User {user_id}: {get_next_count} get_next_lead, {take_break_count} take_break, {calls_made_events.count()} calls_made events")
        
        # Build maps by record_id
        get_next_by_record = {}
        for event in get_next_lead_events:
            record_id = event.record_id
            if record_id not in get_next_by_record:
                get_next_by_record[record_id] = []
            get_next_by_record[record_id].append(event)
        
        calls_made_by_record = {}
        for event in calls_made_events:
            record_id = event.record_id
            if record_id not in calls_made_by_record:
                calls_made_by_record[record_id] = []
            calls_made_by_record[record_id].append(event)
        
        # Calculate time differences: sum of (calls_made time - get_next_lead time)
        time_sum = 0.0
        for record_id, get_next_list in get_next_by_record.items():
            if record_id not in calls_made_by_record:
                continue
            
            calls_list = calls_made_by_record[record_id]
            
            # For each get_next_lead, find the first calls_made event after it
            for get_next_event in get_next_list:
                get_next_time = get_next_event.timestamp
                
                # Find first calls_made event after this get_next_lead
                for calls_event in calls_list:
                    if calls_event.timestamp > get_next_time:
                        diff = (calls_event.timestamp - get_next_time).total_seconds()
                        if diff > 0:
                            time_sum += diff
                            logger.debug(f"[TeamMetricsService] User {user_id}, Record {record_id}: {diff:.2f}s between get_next_lead and calls_made")
                        break
        
        # Denominator: get_next_lead events - take_break events
        denominator = get_next_count - take_break_count
        
        if denominator <= 0:
            logger.warning(f"[TeamMetricsService] User {user_id}: denominator is {denominator}, returning 0")
            return 0.0
        
        if time_sum == 0:
            logger.warning(f"[TeamMetricsService] User {user_id}: no time differences found, returning 0")
            return 0.0
        
        average_time = time_sum / denominator
        logger.info(f"[TeamMetricsService] User {user_id}: average time spent = {time_sum:.2f}s / {denominator} = {average_time:.2f}s")
        
        return average_time
    
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
        avg_time_by_user = self._get_average_time_spent_bulk(
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
            
            # Per-user average time spent (from bulk fetch)
            member_data['average_time_spent_seconds'] = avg_time_by_user.get(user_id_str, 0.0)
            
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


class RmPerformanceSummaryService:
    """
    Computes and stores one tenant's one local-day RmPerformanceDailySummary
    rows from raw rm_activity_events — the actual aggregation work behind
    the refresh_rm_performance_summary management command. Kept as a plain
    function (not called from any live request) so it's cheap to unit-test
    directly and to run from a scheduled job without any web-request
    machinery in the way.

    One call = one tenant, one local day. The day boundary comes from
    get_utc_datetime_range_for_ist_date, not a UTC-calendar-day window —
    a previous live endpoint used a UTC-widened window the frontend sent
    and never trimmed back down, which silently queried up to ~2x the
    intended range for IST users. This function is the one place that
    boundary gets computed from now on, so every reader downstream just
    sums whichever `date` rows fall in range instead of re-deriving it.

    Uses a plain cursor (no Django QuerySet .iterator()/server-side cursor)
    — Supabase's transaction-mode pooler can swap backends between
    statements, which breaks named server-side cursors (see
    DISABLE_SERVER_SIDE_CURSORS in settings).
    """

    @staticmethod
    def compute_and_store_day(tenant, target_date: date, statement_timeout_ms: Optional[int] = None) -> int:
        """
        Replaces every RmPerformanceDailySummary row for (tenant,
        target_date) with a fresh computation — a DELETE + INSERT in one
        transaction, not an upsert, so a bucket that no longer has any
        touches (e.g. the last touch for a given state/party/lead_group
        combo got reassigned or soft-deleted) actually disappears instead
        of lingering with stale counts forever.

        statement_timeout_ms, if given, is applied with SET LOCAL inside
        the same transaction as the read query — not a bare session-level
        SET, which isn't safe here: Supabase's transaction-mode pooler can
        swap the underlying backend connection between statements outside
        an explicit transaction. SET LOCAL is scoped to — and
        automatically cleared at the end of — this one transaction, so it
        can't leak onto whatever connection the pool hands back for an
        unrelated later request.

        Returns the number of summary rows written for this tenant/day.
        """
        utc_start, utc_end = get_utc_datetime_range_for_ist_date(target_date)
        params = {
            "tenant_id": str(tenant.id),
            "start": json.dumps(utc_start.isoformat()),
            "end": json.dumps(utc_end.isoformat()),
        }

        sql = """
            WITH closed AS (
                SELECT
                    id,
                    event_data->>'rm_user_id' AS rm_user_id,
                    NULLIF(event_data->>'rm_name', '') AS rm_name,
                    NULLIF(event_data->>'manager_name', '') AS manager_name,
                    COALESCE(NULLIF(event_data->>'state', ''), '') AS state,
                    COALESCE(NULLIF(event_data->>'party', ''), '') AS party,
                    COALESCE(NULLIF(event_data->>'lead_group', ''), '') AS lead_group,
                    NULLIF(event_data->>'lead_record_id', '')::bigint AS lead_record_id,
                    NULLIF(event_data->>'updated_status', '') AS updated_status,
                    event_data->>'started_at' AS started_at
                FROM rm_activity_events
                WHERE tenant_id = %(tenant_id)s
                    AND is_deleted = false AND deleted_at IS NULL
                    AND event_type = 'CALL_TOUCH'
                    AND event_data->>'ended_at' IS NOT NULL
                    AND event_data->>'rm_user_id' IS NOT NULL
                    AND (event_data -> 'started_at') >= %(start)s::jsonb
                    AND (event_data -> 'started_at') <= %(end)s::jsonb
            ),
            first_row AS (
                SELECT DISTINCT ON (rm_user_id, state, party, lead_group)
                    rm_user_id, state, party, lead_group, rm_name, manager_name
                FROM closed
                ORDER BY rm_user_id, state, party, lead_group, id ASC
            ),
            touches_agg AS (
                SELECT rm_user_id, state, party, lead_group,
                    count(*) AS touches, count(DISTINCT lead_record_id) AS unique_leads
                FROM closed
                GROUP BY rm_user_id, state, party, lead_group
            ),
            latest AS (
                SELECT DISTINCT ON (rm_user_id, state, party, lead_group, lead_record_id)
                    rm_user_id, state, party, lead_group, updated_status
                FROM closed
                WHERE lead_record_id IS NOT NULL
                ORDER BY rm_user_id, state, party, lead_group, lead_record_id, started_at DESC, id ASC
            ),
            disp AS (
                SELECT rm_user_id, state, party, lead_group, updated_status, count(*) AS cnt
                FROM latest
                WHERE updated_status IS NOT NULL
                GROUP BY rm_user_id, state, party, lead_group, updated_status
            )
            SELECT
                t.rm_user_id, f.rm_name, f.manager_name, t.state, t.party, t.lead_group,
                t.touches, t.unique_leads,
                COALESCE(
                    jsonb_object_agg(d.updated_status, d.cnt) FILTER (WHERE d.updated_status IS NOT NULL),
                    '{}'::jsonb
                ) AS disposition_counts
            FROM touches_agg t
            JOIN first_row f
                ON f.rm_user_id = t.rm_user_id AND f.state = t.state
                AND f.party = t.party AND f.lead_group = t.lead_group
            LEFT JOIN disp d
                ON d.rm_user_id = t.rm_user_id AND d.state = t.state
                AND d.party = t.party AND d.lead_group = t.lead_group
            GROUP BY t.rm_user_id, f.rm_name, f.manager_name, t.state, t.party, t.lead_group,
                t.touches, t.unique_leads
        """

        # Same `closed` universe, but deduped globally per lead_record_id
        # (no rm_user_id in the DISTINCT ON) instead of per-RM-bucket — see
        # RmPerformanceDailyLeadLatest's docstring for why this has to be a
        # separate query rather than derived from the rows above: a lead
        # touched by two different RMs the same day must appear exactly
        # once here, attributed to whichever touch was truly latest.
        leads_sql = """
            WITH closed AS (
                SELECT
                    id,
                    event_data->>'rm_user_id' AS rm_user_id,
                    COALESCE(NULLIF(event_data->>'state', ''), '') AS state,
                    COALESCE(NULLIF(event_data->>'party', ''), '') AS party,
                    COALESCE(NULLIF(event_data->>'lead_group', ''), '') AS lead_group,
                    NULLIF(event_data->>'lead_record_id', '')::bigint AS lead_record_id,
                    NULLIF(event_data->>'updated_status', '') AS updated_status,
                    event_data->>'started_at' AS started_at
                FROM rm_activity_events
                WHERE tenant_id = %(tenant_id)s
                    AND is_deleted = false AND deleted_at IS NULL
                    AND event_type = 'CALL_TOUCH'
                    AND event_data->>'ended_at' IS NOT NULL
                    AND event_data->>'rm_user_id' IS NOT NULL
                    AND (event_data -> 'started_at') >= %(start)s::jsonb
                    AND (event_data -> 'started_at') <= %(end)s::jsonb
            )
            SELECT DISTINCT ON (lead_record_id)
                lead_record_id, rm_user_id, state, party, lead_group, updated_status
            FROM closed
            WHERE lead_record_id IS NOT NULL
            ORDER BY lead_record_id, started_at DESC, id ASC
        """

        from .models import RmPerformanceDailyLeadLatest, RmPerformanceDailySummary

        with transaction.atomic():
            with connection.cursor() as cursor:
                if statement_timeout_ms is not None:
                    cursor.execute("SET LOCAL statement_timeout = %s", [statement_timeout_ms])
                cursor.execute(sql, params)
                columns = [col[0] for col in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

                cursor.execute(leads_sql, params)
                lead_columns = [col[0] for col in cursor.description]
                lead_rows = [dict(zip(lead_columns, row)) for row in cursor.fetchall()]

            RmPerformanceDailySummary.objects.filter(tenant=tenant, date=target_date).delete()
            RmPerformanceDailySummary.objects.bulk_create(
                [
                    RmPerformanceDailySummary(
                        tenant=tenant,
                        rm_user_id=row["rm_user_id"],
                        rm_name=row["rm_name"] or "",
                        manager_name=row["manager_name"] or "",
                        state=row["state"],
                        party=row["party"],
                        lead_group=row["lead_group"],
                        date=target_date,
                        touches=row["touches"],
                        unique_leads=row["unique_leads"],
                        # jsonb_object_agg comes back from the raw cursor as a
                        # JSON string, not auto-decoded like Django's JSONField
                        disposition_counts=json.loads(row["disposition_counts"]),
                    )
                    for row in rows
                ]
            )

            RmPerformanceDailyLeadLatest.objects.filter(tenant=tenant, date=target_date).delete()
            RmPerformanceDailyLeadLatest.objects.bulk_create(
                [
                    RmPerformanceDailyLeadLatest(
                        tenant=tenant,
                        lead_record_id=row["lead_record_id"],
                        rm_user_id=row["rm_user_id"],
                        state=row["state"],
                        party=row["party"],
                        lead_group=row["lead_group"],
                        date=target_date,
                        updated_status=row["updated_status"] or "",
                    )
                    for row in lead_rows
                ]
            )

        return len(rows)


def compute_score(achieved: int, target: int) -> Optional[float]:
    """
    achieved/target as a 0-100+ PERCENTAGE (not a 0-1 ratio) — matches
    formatVsTarget's convention on the frontend. Returns None (not
    float('-inf')) when target is 0/unset: -inf isn't valid JSON (the
    stdlib `json` module happily emits it as the literal token `-Infinity`,
    which most JSON parsers, including JS's JSON.parse, reject outright),
    and None round-trips through DRF/JSON cleanly as `null`. Callers that
    need "unset sorts last" semantics (the leaderboard's own ranking) must
    handle the None case explicitly when ordering — see
    RmLeaderboardService.compute_and_store_period.
    """
    return (achieved / target) * 100 if target else None


def ist_today() -> date:
    """Today's date in IST — `timezone.now()` is naive UTC in this project
    (USE_TZ=False), so this isn't simply `timezone.now().date()`: that
    would read as the wrong calendar day for part of the evening/night IST
    (e.g. 11pm IST is already the next UTC day)."""
    ist_now = timezone.now().replace(tzinfo=pytz.UTC).astimezone(pytz.timezone("Asia/Kolkata"))
    return ist_now.date()


def ist_week_start(d: date) -> date:
    """Monday of the ISO week containing `d` (date.weekday(): Monday=0)."""
    return d - timedelta(days=d.weekday())


def ist_week_end(week_start: date) -> date:
    return week_start + timedelta(days=6)


def ist_month_start(d: date) -> date:
    return d.replace(day=1)


def ist_month_end(month_start: date) -> date:
    if month_start.month == 12:
        next_month_start = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month_start = month_start.replace(month=month_start.month + 1)
    return next_month_start - timedelta(days=1)


# Arbitrary fixed bigint — pg_advisory_lock's key space is just "pick a
# number unique to this one job across the whole system." Not derived
# from anything; just needs to never collide with another job's lock key.
_LEADERBOARD_REFRESH_LOCK_KEY = 887_001_001


def _session_mode_connection_params() -> dict:
    """
    Same session-mode-port pattern as realtime.pg_listener's LISTEN
    connection: Supabase's transaction-mode pooler (port 6543) can swap
    the underlying Postgres backend between statements outside an
    explicit transaction. A session-scoped advisory lock taken over that
    pooled connection would be meaningless — the "session" it's scoped to
    might not even be the same backend by the time later work runs. The
    session-mode port (5432, same host) guarantees one real backend for
    the lifetime of this one dedicated connection.
    """
    db = settings.DATABASES["default"]
    port = int(db.get("PORT") or 5432)
    if port == 6543:
        port = 5432
    params = {
        "dbname": db["NAME"],
        "user": db["USER"],
        "password": db["PASSWORD"],
        "host": db["HOST"],
        "port": port,
    }
    sslmode = (db.get("OPTIONS") or {}).get("sslmode")
    if sslmode:
        params["sslmode"] = sslmode
    return params


@contextmanager
def leaderboard_refresh_lock():
    """
    Session-scoped pg_try_advisory_lock so two overlapping
    refresh_rm_leaderboard_snapshot runs (a slow run plus the next
    scheduled cron tick firing before it finishes) exit immediately
    instead of both doing the same work concurrently. Yields True if this
    call acquired the lock (caller should proceed), False if another
    instance already holds it (caller should log and exit without doing
    any work — not wait, not retry).

    Deliberately opens its OWN dedicated connection on the session-mode
    port rather than using Django's pooled `connection` — see
    _session_mode_connection_params. This connection is held open for the
    whole duration of the `with` block and explicitly closed after
    releasing the lock; it is never used for any other query.
    """
    conn = psycopg2.connect(**_session_mode_connection_params())
    acquired = False
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", [_LEADERBOARD_REFRESH_LOCK_KEY])
            acquired = cur.fetchone()[0]
        yield acquired
    finally:
        if acquired:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", [_LEADERBOARD_REFRESH_LOCK_KEY])
        conn.close()


class RmLeaderboardService:
    """
    Computes and stores one tenant's one CURRENT period's
    RmLeaderboardSnapshot rows from RmPerformanceDailySummary — the
    aggregation behind refresh_rm_leaderboard_snapshot. Unlike
    RmPerformanceSummaryService, this never touches raw rm_activity_events
    — it only sums already-small daily-summary rows for one RM at a time
    (no cross-RM dedup ambiguity: a single RM's own achieved/touches count
    has none of the "reassigned lead" double-counting risk that
    RmPerformanceDailyLeadLatest exists to solve for team-wide totals), so
    there's no raw-cursor/JSONB-string-decoding concern here at all — this
    is plain Django ORM aggregation over a small, already-indexed table.
    """

    @staticmethod
    def compute_and_store_period(
        tenant,
        period_type: str,
        period_start: date,
        period_end: date,
        statement_timeout_ms: Optional[int] = None,
    ) -> int:
        """
        Replaces every RmLeaderboardSnapshot row for (tenant, period_type,
        period_start) with a fresh ranked computation. The read (summing
        RmPerformanceDailySummary), the DELETE, and the INSERT all happen
        inside ONE transaction.atomic() — not just the delete+insert — so
        a reader can never observe an empty/partial board mid-refresh, and
        so statement_timeout_ms (applied via SET LOCAL, same pooler-safety
        reasoning as RmPerformanceSummaryService.compute_and_store_day)
        covers every statement this call makes, not just some of them.

        Only RMs with a currently-active TenantMembership are ranked —
        historical RmPerformanceDailySummary rows for a since-deactivated
        or deleted RM are excluded entirely, not just hidden client-side.

        Returns the number of leaderboard rows written.
        """
        from .models import RmLeaderboardSnapshot, RmPerformanceDailySummary

        with transaction.atomic():
            if statement_timeout_ms is not None:
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL statement_timeout = %s", [statement_timeout_ms])

            active_memberships = list(
                TenantMembership.objects.filter(
                    tenant=tenant, is_active=True, user_id__isnull=False
                ).values("id", "user_id")
            )
            active_rm_user_ids = {str(m["user_id"]) for m in active_memberships}
            membership_id_by_user_id = {str(m["user_id"]): m["id"] for m in active_memberships}

            if not active_rm_user_ids:
                RmLeaderboardSnapshot.objects.filter(
                    tenant=tenant, period_type=period_type, period_start=period_start
                ).delete()
                return 0

            # Sum each active RM's own daily-summary buckets across the
            # period — no cross-RM dedup concern (see class docstring), so
            # this is a plain per-RM sum, not the global-dedup query the
            # old team-totals path needed.
            per_rm: Dict[str, Dict[str, Any]] = {
                rm_user_id: {"touches": 0, "unique_leads": 0, "achieved": 0}
                for rm_user_id in active_rm_user_ids
            }
            rows = RmPerformanceDailySummary.objects.filter(
                tenant=tenant,
                rm_user_id__in=active_rm_user_ids,
                date__gte=period_start,
                date__lte=period_end,
            ).values("rm_user_id", "touches", "unique_leads", "disposition_counts")
            for row in rows:
                bucket = per_rm[row["rm_user_id"]]
                bucket["touches"] += row["touches"]
                bucket["unique_leads"] += row["unique_leads"]
                bucket["achieved"] += (row["disposition_counts"] or {}).get("TRIAL_ACTIVATED", 0)

            membership_ids = list(membership_id_by_user_id.values())
            target_by_membership = get_rm_daily_targets_sum(tenant, membership_ids, period_start, period_end)
            target_by_rm_user_id = {
                user_id: target_by_membership.get(membership_id_by_user_id[user_id], 0)
                for user_id in active_rm_user_ids
            }

            entries = []
            for rm_user_id, bucket in per_rm.items():
                target = target_by_rm_user_id.get(rm_user_id, 0)
                entries.append(
                    {
                        "rm_user_id": rm_user_id,
                        "touches": bucket["touches"],
                        "unique_leads": bucket["unique_leads"],
                        "achieved": bucket["achieved"],
                        "target": target,
                        "score": compute_score(bucket["achieved"], target),
                    }
                )

            # score DESC NULLS LAST, achieved DESC, rm_id ASC — split into
            # "has a real score" and "no target configured" groups so
            # Python's sort (which can't compare None to a float) never
            # has to, while still applying the achieved/rm_id tie-break
            # within each group.
            scored = sorted(
                (e for e in entries if e["score"] is not None),
                key=lambda e: (-e["score"], -e["achieved"], e["rm_user_id"]),
            )
            unscored = sorted(
                (e for e in entries if e["score"] is None),
                key=lambda e: (-e["achieved"], e["rm_user_id"]),
            )
            ranked = scored + unscored

            RmLeaderboardSnapshot.objects.filter(
                tenant=tenant, period_type=period_type, period_start=period_start
            ).delete()
            RmLeaderboardSnapshot.objects.bulk_create(
                [
                    RmLeaderboardSnapshot(
                        tenant=tenant,
                        rm_user_id=entry["rm_user_id"],
                        period_type=period_type,
                        period_start=period_start,
                        touches=entry["touches"],
                        unique_leads=entry["unique_leads"],
                        achieved=entry["achieved"],
                        target=entry["target"],
                        score=entry["score"],
                        rank=index + 1,
                    )
                    for index, entry in enumerate(ranked)
                ]
            )

        return len(ranked)


def leaderboard_cache_key(
    tenant_id, period_type: str, period_start: date, scope_user_ids: Optional[Set[str]], limit: int
) -> str:
    """
    Cache key for one leaderboard read. tenant_id is always first and
    always present, so a cache entry can never be returned across
    tenants even on a shared backend — see
    test_no_cross_tenant_cache_hit.

    scope_user_ids=None means "the whole tenant's board" (what a manager
    or an RM's top-N view requests) — every viewer of that same
    tenant/period/limit shares one entry. A concrete set means "one
    ASM's team" — hashed rather than joined raw so the key stays a
    short, fixed length regardless of team size, and so two ASMs with
    different teams (same tenant, same period, same limit) never read
    each other's cached board.
    """
    if scope_user_ids is None:
        scope_component = "tenant"
    else:
        ids_digest = hashlib.sha256(",".join(sorted(scope_user_ids)).encode()).hexdigest()[:16]
        scope_component = f"team:{ids_digest}"
    return f"rm_leaderboard:{tenant_id}:{period_type}:{period_start.isoformat()}:{scope_component}:{limit}"


_LEADERBOARD_ROW_FIELDS = ("rm_user_id", "rank", "score", "achieved", "target", "touches", "unique_leads", "computed_at")


def _leaderboard_is_stale(computed_at: Optional[datetime]) -> bool:
    if computed_at is None:
        return True
    threshold = getattr(settings, "RM_LEADERBOARD_STALE_THRESHOLD_SECONDS", 900)
    return (timezone.now() - computed_at).total_seconds() > threshold


class RmLeaderboardReadService:
    """
    Read-side of the leaderboard: only ever SELECTs RmLeaderboardSnapshot
    rows written by RmLeaderboardService.compute_and_store_period — never
    aggregates anything live. Wrapped in a short Django-cache TTL (see
    leaderboard_cache_key) so repeated polling doesn't re-hit Postgres on
    every request.
    """

    @staticmethod
    def get_board(tenant, period_type: str, period_start: date, scope_user_ids: Optional[Set[str]] = None, limit: int = 50) -> dict:
        """
        scope_user_ids=None: plain tenant-wide top-N, ordered by the
        rank the refresh job already computed — a single indexed,
        LIMIT-pushed-down SELECT (the read query Step 5's EXPLAIN
        ANALYZE targets).

        scope_user_ids={...}: an ASM/manager's team view. The stored
        `rank` is tenant-wide, so it has gaps once filtered to one
        team — re-numbered 1..N here (equivalent to ROW_NUMBER() OVER
        ORDER BY rank, just applied in Python on an already-ordered,
        already-small result set instead of a second raw SQL query).
        """
        from .models import RmLeaderboardSnapshot

        cache_key = leaderboard_cache_key(tenant.id, period_type, period_start, scope_user_ids, limit)
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        base_qs = RmLeaderboardSnapshot.objects.filter(
            tenant=tenant, period_type=period_type, period_start=period_start,
        )
        if scope_user_ids is None:
            rows = list(
                base_qs.order_by("rank")[:limit].values(*_LEADERBOARD_ROW_FIELDS)
            )
        else:
            rows = list(
                base_qs.filter(rm_user_id__in=scope_user_ids).order_by("rank").values(*_LEADERBOARD_ROW_FIELDS)
            )
            for index, row in enumerate(rows):
                row["rank"] = index + 1
            rows = rows[:limit]

        computed_at = max((row["computed_at"] for row in rows), default=None)
        stale = _leaderboard_is_stale(computed_at)
        for row in rows:
            row.pop("computed_at", None)

        result = {
            "rows": rows,
            "stale": stale,
            "computed_at": computed_at.isoformat() if computed_at else None,
        }
        cache.set(cache_key, result, getattr(settings, "RM_LEADERBOARD_CACHE_TTL_SECONDS", 45))
        return result

    @staticmethod
    def get_my_row(tenant, period_type: str, period_start: date, rm_user_id: str) -> dict:
        """
        The requesting RM's own row, independent of the top-N cutoff —
        what /rm-leaderboard/me/ uses so an RM outside the visible top N
        still sees their own rank (fix #7: never hide the board from an
        RM, just show them less of everyone else's).
        """
        from .models import RmLeaderboardSnapshot

        cache_key = leaderboard_cache_key(tenant.id, period_type, period_start, {rm_user_id}, limit=1)
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        row = (
            RmLeaderboardSnapshot.objects.filter(
                tenant=tenant, period_type=period_type, period_start=period_start, rm_user_id=rm_user_id,
            )
            .values(*_LEADERBOARD_ROW_FIELDS)
            .first()
        )
        computed_at = row["computed_at"] if row else None
        stale = _leaderboard_is_stale(computed_at)
        if row is not None:
            row.pop("computed_at", None)

        result = {
            "row": row,
            "stale": stale,
            "computed_at": computed_at.isoformat() if computed_at else None,
        }
        cache.set(cache_key, result, getattr(settings, "RM_LEADERBOARD_CACHE_TTL_SECONDS", 45))
        return result

