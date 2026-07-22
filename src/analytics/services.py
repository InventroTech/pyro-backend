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

