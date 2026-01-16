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
from user_settings.models import UserSettings
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
        
        Args:
            manager_user_id: UUID of the manager user
            tenant: Tenant instance
            
        Returns:
            Count of reports (direct + indirect, excluding manager)
        """
        logger = logging.getLogger(__name__)
        logger.info(f"[TeamResolver] Getting reports count for manager_user_id: {manager_user_id}, tenant: {tenant.id}")
        
        # Find the manager's TenantMembership record
        try:
            manager_membership = TenantMembership.objects.get(
                tenant=tenant,
                user_id=manager_user_id,
                is_active=True
            )
        except TenantMembership.DoesNotExist:
            logger.warning(f"[TeamResolver] Manager TenantMembership NOT FOUND for user_id: {manager_user_id}, tenant: {tenant.id}")
            return 0
        
        def count_reports_recursive(parent_membership: TenantMembership) -> int:
            """Recursively count all direct and indirect reports."""
            direct_reports = TenantMembership.objects.filter(
                tenant=tenant,
                user_parent_id=parent_membership,
                is_active=True
            )
            count = direct_reports.count()
            logger.info(f"[TeamResolver] Found {count} direct reports for parent {parent_membership.id}")
            
            # Recursively count nested reports
            for membership in direct_reports:
                if membership.user_id:
                    nested_count = count_reports_recursive(membership)
                    count += nested_count
            
            return count
        
        reports_count = count_reports_recursive(manager_membership)
        logger.info(f"[TeamResolver] Total reports count (excluding manager): {reports_count}")
        return reports_count
    
    @staticmethod
    def get_team_user_ids(manager_user_id: str, tenant) -> Set[str]:
        """
        Get all user_ids in the team (manager + all direct and indirect reports).
        
        Args:
            manager_user_id: UUID of the manager user
            tenant: Tenant instance
            
        Returns:
            Set of user_id strings (UUIDs) including manager and all team members
        """
        logger = logging.getLogger(__name__)
        logger.info(f"[TeamResolver] Starting team resolution for manager_user_id: {manager_user_id}, tenant: {tenant.id}")
        
        team_ids = {str(manager_user_id)}
        
        # First, find the manager's TenantMembership record
        try:
            logger.info(f"[TeamResolver] Looking up manager TenantMembership - tenant: {tenant.id}, user_id: {manager_user_id}, is_active: True")
            manager_membership = TenantMembership.objects.get(
                tenant=tenant,
                user_id=manager_user_id,
                is_active=True
            )
            logger.info(f"[TeamResolver] Found manager TenantMembership - id: {manager_membership.id}, user_id: {manager_membership.user_id}, email: {manager_membership.email}")
        except TenantMembership.DoesNotExist:
            logger.warning(f"[TeamResolver] Manager TenantMembership NOT FOUND for user_id: {manager_user_id}, tenant: {tenant.id}")
            logger.warning(f"[TeamResolver] Available TenantMemberships for this tenant: {list(TenantMembership.objects.filter(tenant=tenant).values_list('user_id', 'email', 'is_active'))}")
            # Manager not found in TenantMembership, return just the manager
            return team_ids
        
        def get_direct_reports(parent_membership: TenantMembership) -> Set[str]:
            """Recursively get all direct and indirect reports."""
            logger.info(f"[TeamResolver] Getting direct reports for parent membership id: {parent_membership.id}, user_id: {parent_membership.user_id}")
            
            # Get all TenantMembership records that have this parent
            direct_reports = TenantMembership.objects.filter(
                tenant=tenant,
                user_parent_id=parent_membership,
                is_active=True
            ).select_related('user_parent_id')
            
            logger.info(f"[TeamResolver] Found {direct_reports.count()} direct reports for parent {parent_membership.id}")
            
            report_ids = set()
            for membership in direct_reports:
                logger.info(f"[TeamResolver] Processing report - membership id: {membership.id}, user_id: {membership.user_id}, email: {membership.email}, user_parent_id: {membership.user_parent_id_id if membership.user_parent_id else None}")
                if membership.user_id:
                    user_id_str = str(membership.user_id)
                    report_ids.add(user_id_str)
                    logger.info(f"[TeamResolver] Added user_id to reports: {user_id_str}")
                    # Recursively get their reports
                    nested_reports = get_direct_reports(membership)
                    if nested_reports:
                        logger.info(f"[TeamResolver] Found {len(nested_reports)} nested reports for {user_id_str}")
                    report_ids.update(nested_reports)
                else:
                    logger.warning(f"[TeamResolver] Report membership {membership.id} has no user_id")
            
            return report_ids
        
        # Get all team members recursively
        nested_ids = get_direct_reports(manager_membership)
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
        logger = logging.getLogger(__name__)
        logger.info(f"[TeamMetricsService] _get_base_queryset called")
        logger.info(f"[TeamMetricsService] tenant: {self.tenant.id}, team_user_ids: {self.team_user_ids}")
        logger.info(f"[TeamMetricsService] date range: {start_date} to {end_date}")
        
        # Get team user IDs (excluding manager if provided)
        team_user_ids_to_use = self._get_team_user_ids_excluding_manager(manager_user_id) if manager_user_id else self.team_user_ids
        
        # Filter by payload->>'user_id' directly using JSONB field lookup
        queryset = EventLog.objects.filter(
            tenant=self.tenant,
            event__in=TRACKED_EVENTS,
            payload__user_id__in=list(team_user_ids_to_use)
        )
        
        base_count = queryset.count()
        logger.info(f"[TeamMetricsService] Base queryset (before date filter) count: {base_count}")
        logger.info(f"[TeamMetricsService] TRACKED_EVENTS: {TRACKED_EVENTS}")
        
        # Log actual dates of events to help debug
        if base_count > 0:
            event_dates = list(queryset.values_list('timestamp__date', flat=True).distinct().order_by('timestamp__date'))
            logger.info(f"[TeamMetricsService] Actual event dates in database: {event_dates}")
            # Log a sample event with full details
            sample = queryset.first()
            if sample:
                logger.info(f"[TeamMetricsService] Sample event - id: {sample.id}, event: {sample.event}, timestamp: {sample.timestamp}, date: {sample.timestamp.date()}, payload_user_id: {sample.payload.get('user_id')}")
        else:
            logger.warning(f"[TeamMetricsService] No events found for team members at all (before date filter)")
        
        # Convert IST dates to UTC datetime ranges for accurate filtering
        if start_date:
            utc_start, _ = get_utc_datetime_range_for_ist_date(start_date)
            queryset = queryset.filter(timestamp__gte=utc_start)
            logger.info(f"[TeamMetricsService] After start_date filter (IST: {start_date}, UTC: {utc_start}): {queryset.count()}")
        if end_date:
            _, utc_end = get_utc_datetime_range_for_ist_date(end_date)
            queryset = queryset.filter(timestamp__lte=utc_end)
            logger.info(f"[TeamMetricsService] After end_date filter (IST: {end_date}, UTC: {utc_end}): {queryset.count()}")
        
        # Log sample events to see what we're getting
        sample_events = queryset[:5].values('id', 'event', 'timestamp', 'payload__user_id', 'record_id')
        logger.info(f"[TeamMetricsService] Sample events (first 5): {list(sample_events)}")
        
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
        Averages the per-user averages.
        """
        logger = logging.getLogger(__name__)
        logger.info(f"[TeamMetricsService] Calculating team average time spent for {start_date} to {end_date} (excluding manager)")
        
        team_user_ids_to_use = self._get_team_user_ids_excluding_manager(manager_user_id) if manager_user_id else self.team_user_ids
        
        user_averages = []
        for user_id in team_user_ids_to_use:
            avg = self.get_average_time_spent_per_user(str(user_id), start_date, end_date)
            if avg is not None:
                user_averages.append(avg)
        
        if not user_averages:
            logger.warning(f"[TeamMetricsService] No user averages found for team average calculation")
            return None
        
        team_avg = sum(user_averages) / len(user_averages)
        logger.info(f"[TeamMetricsService] Team average time spent: {team_avg:.2f}s (from {len(user_averages)} users)")
        
        return team_avg
    
    def get_trail_target(self, manager_user_id: Optional[str] = None) -> int:
        """
        Calculate trail target by summing daily_target from user_settings for all team members (excluding manager).
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
        
        # Sum daily_target from user_settings for these tenant_memberships
        # Note: user_settings can have multiple entries per tenant_membership (one per key)
        # We take the maximum daily_target per tenant_membership to avoid double-counting,
        # then sum across all memberships
        from django.db.models import Sum, Max
        
        # Get the maximum daily_target per tenant_membership (in case there are multiple entries with different keys)
        # Then sum them up across all memberships
        membership_targets = UserSettings.objects.filter(
            tenant=self.tenant,
            tenant_membership_id__in=memberships,
            daily_target__isnull=False
        ).values('tenant_membership_id').annotate(
            max_target=Max('daily_target')
        ).aggregate(
            total=Sum('max_target')
        )['total'] or 0
        
        target_sum = int(membership_targets)
        
        logger.info(f"[TeamMetricsService] Trail target calculated: {target_sum}")
        return int(target_sum)
    
    def get_allotted_leads(self, manager_user_id: Optional[str] = None) -> int:
        """
        Calculate allotted leads by summing daily_limit from user_settings for all team members (excluding manager).
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
        
        # Sum daily_limit from user_settings for these tenant_memberships
        # Note: user_settings can have multiple entries per tenant_membership (one per key)
        # We take the maximum daily_limit per tenant_membership to avoid double-counting,
        # then sum across all memberships
        from django.db.models import Sum, Max
        
        # Get the maximum daily_limit per tenant_membership (in case there are multiple entries with different keys)
        # Then sum them up across all memberships
        membership_limits = UserSettings.objects.filter(
            tenant=self.tenant,
            tenant_membership_id__in=memberships,
            daily_limit__isnull=False
        ).values('tenant_membership_id').annotate(
            max_limit=Max('daily_limit')
        ).aggregate(
            total=Sum('max_limit')
        )['total'] or 0
        
        limit_sum = int(membership_limits)
        
        logger.info(f"[TeamMetricsService] Allotted leads calculated: {limit_sum}")
        return int(limit_sum)
    
    def get_overview(self, target_date: date, manager_user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get overview metrics for a specific date.
        
        Args:
            target_date: Date to get metrics for
            manager_user_id: Optional manager user_id to calculate reports count (excludes manager from total_team_size)
        """
        # Calculate total_team_size: count of reports only (excluding manager)
        if manager_user_id:
            # Use TeamResolver to get accurate count of reports
            total_team_size = TeamResolver.get_reports_count(manager_user_id, self.tenant)
        else:
            # Fallback: exclude manager from team_user_ids count
            # This assumes manager is always in team_user_ids
            total_team_size = max(0, len(self.team_user_ids) - 1)
        
        return {
            'attendance': self.get_attendance(target_date, manager_user_id),
            'total_team_size': total_team_size,  # Count of reports only (excluding manager)
            'calls_made': self.get_calls_made(target_date, target_date, manager_user_id),
            'trials_activated': self.get_trials_activated(target_date, target_date, manager_user_id),
            'connected_to_trial_ratio': self.get_connected_to_trial_ratio(target_date, target_date, manager_user_id),
            'average_time_spent_seconds': self.get_average_time_spent(target_date, target_date, manager_user_id),
            'trail_target': self.get_trail_target(manager_user_id),  # Sum of daily_target from user_settings
            'allotted_leads': self.get_allotted_leads(manager_user_id),  # Sum of daily_limit from user_settings
        }
    
    def get_member_breakdown(self, start_date: Optional[date] = None, end_date: Optional[date] = None, manager_user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get per-member metrics breakdown (excluding manager if manager_user_id is provided)."""
        logger = logging.getLogger(__name__)
        logger.info(f"[TeamMetricsService] get_member_breakdown called for {start_date} to {end_date} (excluding manager: {manager_user_id})")
        
        queryset = self._get_base_queryset(start_date, end_date, manager_user_id)
        total_events = queryset.count()
        logger.info(f"[TeamMetricsService] Total events in queryset: {total_events}")
        
        if total_events == 0:
            logger.warning(f"[TeamMetricsService] No events found! Checking if events exist at all...")
            # Check if there are ANY events for this tenant
            all_events = EventLog.objects.filter(tenant=self.tenant).count()
            logger.info(f"[TeamMetricsService] Total EventLog entries for tenant: {all_events}")
            
            # Check if there are events for team user_ids (without date filter)
            team_events = EventLog.objects.filter(
                tenant=self.tenant,
                payload__user_id__in=list(self.team_user_ids)
            ).count()
            logger.info(f"[TeamMetricsService] Total events for team user_ids (any date): {team_events}")
            
            # Check if there are events for the date range (any user)
            date_events = EventLog.objects.filter(
                tenant=self.tenant,
                timestamp__date__gte=start_date if start_date else date.today(),
                timestamp__date__lte=end_date if end_date else date.today()
            ).count()
            logger.info(f"[TeamMetricsService] Total events for date range (any user): {date_events}")
        
        # Initialize user_metrics with ALL team members (even if they have no events), excluding manager
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
        
        logger.info(f"[TeamMetricsService] Initialized user_metrics for {len(user_metrics)} team members")
        
        # Get counts per user per event type
        member_metrics = queryset.values('payload__user_id', 'event').annotate(
            count=Count('id')
        ).order_by('payload__user_id', 'event')
        
        logger.info(f"[TeamMetricsService] Member metrics query returned {member_metrics.count()} rows")
        
        # Log the raw metrics
        for metric in list(member_metrics)[:10]:  # Log first 10
            logger.info(f"[TeamMetricsService] Metric: user_id={metric['payload__user_id']}, event={metric['event']}, count={metric['count']}")
        
        # Aggregate by user - update existing entries with event counts
        for metric in member_metrics:
            user_id = metric['payload__user_id']
            # Ensure user_id is in user_metrics (should already be, but safety check)
            if user_id not in user_metrics:
                logger.warning(f"[TeamMetricsService] Found event for user_id {user_id} not in team_user_ids, adding anyway")
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
        
        logger.info(f"[TeamMetricsService] Fetched {len(user_id_to_email)} user emails from TenantMembership for {len(all_user_ids)} user_ids")
        
        # Fetch daily_target from user_settings for these memberships
        membership_ids = [m['id'] for m in memberships]
        from django.db.models import Max
        user_settings = UserSettings.objects.filter(
            tenant=self.tenant,
            tenant_membership_id__in=membership_ids,
            daily_target__isnull=False
        ).values('tenant_membership_id').annotate(
            max_daily_target=Max('daily_target')
        )
        
        # Create a mapping of user_id to daily_target
        user_id_to_daily_target = {}
        for setting in user_settings:
            membership_id = setting['tenant_membership_id']
            user_id_str = membership_id_to_user_id.get(membership_id)
            if user_id_str:
                user_id_to_daily_target[user_id_str] = setting['max_daily_target']
        
        logger.info(f"[TeamMetricsService] Fetched daily_target for {len(user_id_to_daily_target)} users from user_settings")
        
        # Calculate per-user metrics: attendance, connected_to_trial_ratio, and average_time_spent
        # Add email, daily_target, and calculated metrics to each member's data
        result = []
        manager_user_id_str = str(manager_user_id) if manager_user_id else None
        
        for user_id_str, metrics in user_metrics.items():
            # Exclude manager from results
            if manager_user_id_str and user_id_str == manager_user_id_str:
                logger.info(f"[TeamMetricsService] Excluding manager user_id: {user_id_str} from member breakdown")
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
            
            # Calculate per-user average time spent (returns 0.0 if no data)
            avg_time = self.get_average_time_spent_per_user(user_id_str, start_date, end_date)
            member_data['average_time_spent_seconds'] = avg_time if avg_time is not None else 0.0
            
            result.append(member_data)
        
        logger.info(f"[TeamMetricsService] Final member breakdown: {len(result)} members (manager excluded)")
        for member in result:
            logger.info(f"[TeamMetricsService] Member: {member}")
        
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

