"""
Job Processor Service

Handles execution of background jobs from the queue.
Provides worker loop, job locking, processing, and retry logic.
"""
import logging
import time
import threading
from datetime import datetime
from datetime import timedelta
from typing import Optional, Dict, Any, Sequence
from zoneinfo import ZoneInfo

from django.conf import settings

from django.utils import timezone
from django.db import transaction, close_old_connections, connections
from django.db.models import Q, F
from django.db.utils import InterfaceError, OperationalError
from core.models import EntityTypeDiscoverySyncState

from .models import BackgroundJob, JobStatus, JobType
from .queue_service import get_queue_service
from .job_handlers import get_handler_registry
from .scheduler_locks import (
    SCHEDULER_LOCK_DISPATCH_SYNC,
    SCHEDULER_LOCK_LEAD_CRON,
    SCHEDULER_LOCK_SNOOZED_MIDNIGHT,
    scheduler_lock,
)
from .purge_scheduler import tenant_should_enqueue_purge
from .tenant_jobs import enqueue_for_all_tenants, iter_active_tenant_ids

logger = logging.getLogger(__name__)

# Interval (seconds) between enqueueing lead cron jobs from the worker (no external cron needed)
LEAD_CRON_ENQUEUE_INTERVAL = 900  # 15 minutes

# Support ticket dump processing (replaces Supabase process-dumped-tickets cron)
SUPPORT_TICKET_DUMP_ENQUEUE_INTERVAL = 300  # 5 minutes
PROCESS_DUMPED_TICKETS_LOCAL_CHECK_INTERVAL = 30  # avoid checking DB on every loop tick
PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME = "process_dumped_tickets_scheduler"

# Entity type discovery from records.
ENTITY_TYPE_DISCOVERY_ENQUEUE_INTERVAL = 300  # 5 minutes
ENTITY_TYPE_DISCOVERY_LOCAL_CHECK_INTERVAL = 30  # avoid checking DB on every loop tick
ENTITY_TYPE_DISCOVERY_SCHEDULER_JOB_NAME = "entity_type_discovery_scheduler"

# How often the worker enqueues log retention (object_history, event_logs, rule_exec_logs)
LOG_RETENTION_ENQUEUE_INTERVAL = 86400  # 24 hours
LOG_RETENTION_LOCAL_CHECK_INTERVAL = 30  # avoid checking DB on every loop tick
LOG_RETENTION_SCHEDULER_JOB_NAME = "purge_old_log_tables_scheduler"

# Enqueue ``snoozed_to_not_connected_midnight`` at exactly this clock minute in ``TIME_ZONE`` (UTC).
# 23:55 keeps ``NOW()`` on the same calendar date as same-day ``next_call_at`` (e.g. 31 Mar snoozes
# flip on 31 Mar, not after midnight when the date rolls to the next day).
SNOOZED_TO_NOT_CONNECTED_ENQUEUE_HOUR = 23
SNOOZED_TO_NOT_CONNECTED_ENQUEUE_MINUTE = 55

# Enqueue ``sync_dispatch_to_records`` at these UTC hours on the configured minute.
# Airbyte refreshes the source sheet every 8 hours; we run 5 minutes after each refresh.
DISPATCH_SYNC_ENQUEUE_HOURS = (0, 8, 16)
DISPATCH_SYNC_ENQUEUE_MINUTE = 5


class JobProcessor:
    """Processes jobs from the BackgroundJob queue"""
    
    def __init__(
        self,
        worker_id: str = "default",
        *,
        job_types: Optional[Sequence[str]] = None,
        exclude_job_types: Optional[Sequence[str]] = None,
    ):
        """
        Initialize the job processor.
        
        Args:
            worker_id: Unique identifier for this worker instance
            job_types: When set, only lock jobs of these types (dedicated worker).
            exclude_job_types: When set, skip these job types (general worker pool).
        """
        self.worker_id = worker_id
        self._job_types = tuple(job_types) if job_types else None
        self._exclude_job_types = tuple(exclude_job_types) if exclude_job_types else None
        self._stop_event = threading.Event()
        self._handler_registry = get_handler_registry()
        # Last time we enqueued lead cron jobs (unassign snoozed, release after 12h)
        self._last_lead_cron_enqueue_at = None
        # UTC calendar date we last enqueued snoozed→NOT_CONNECTED job (see TIME_ZONE)
        self._last_snoozed_midnight_enqueue_date = None
        # Last time we enqueued purge_old_log_tables
        self._last_log_retention_enqueue_at = None
        # (date, hour) of the last sync_dispatch_to_records enqueue — keys
        # the once-per-window guard.
        self._last_dispatch_sync_enqueue_bucket = None
        # Last time we enqueued process_dumped_tickets for pending dump rows
        self._last_support_ticket_dump_enqueue_at = None
        # Last time we checked whether entity type discovery should be enqueued
        self._last_entity_type_discovery_enqueue_at = None
        self._run_schedulers = True
        # Circuit breaker state for connection errors
        self._connection_error_count = 0
        self._last_connection_error_time = None
        self._circuit_breaker_threshold = 5  # Open circuit after 5 consecutive errors
        self._circuit_breaker_timeout = 60  # Wait 60 seconds before retry
    
    def _is_circuit_breaker_open(self) -> bool:
        """
        Check if circuit breaker is open (too many connection errors).
        
        Returns:
            True if circuit breaker is open, False otherwise
        """
        if self._connection_error_count < self._circuit_breaker_threshold:
            return False
        
        # Check if timeout has passed
        if self._last_connection_error_time:
            time_since_error = (timezone.now() - self._last_connection_error_time).total_seconds()
            if time_since_error >= self._circuit_breaker_timeout:
                # Reset circuit breaker after timeout
                self._connection_error_count = 0
                self._last_connection_error_time = None
                logger.info(f"[Worker {self.worker_id}] Circuit breaker reset after timeout")
                return False
        
        return True
    
    def _record_connection_error(self):
        """Record a connection error for circuit breaker."""
        self._connection_error_count += 1
        self._last_connection_error_time = timezone.now()
        if self._connection_error_count >= self._circuit_breaker_threshold:
            logger.warning(
                f"[Worker {self.worker_id}] Circuit breaker opened after {self._connection_error_count} "
                f"consecutive connection errors. Will retry after {self._circuit_breaker_timeout}s"
            )
    
    def _reset_connection_error_count(self):
        """Reset connection error count on successful operation."""
        if self._connection_error_count > 0:
            logger.debug(f"[Worker {self.worker_id}] Resetting connection error count (was {self._connection_error_count})")
            self._connection_error_count = 0
            self._last_connection_error_time = None
    
    def _log_connection_pool_stats(self):
        """Log connection pool statistics for monitoring."""
        try:
            db_conn = connections['default']
            # Check if connection is open
            conn_open = hasattr(db_conn, 'connection') and db_conn.connection is not None
            conn_max_age = db_conn.settings_dict.get('CONN_MAX_AGE', 'N/A')
            # Log basic connection info (without exposing sensitive data)
            logger.debug(
                f"[Worker {self.worker_id}] Connection pool stats: "
                f"connection_open={conn_open}, conn_max_age={conn_max_age}"
            )
        except Exception as e:
            # Don't fail on stats logging
            logger.debug(f"[Worker {self.worker_id}] Could not log connection stats: {e}")
    
    def lock_and_fetch_job(self, tenant_id: Optional[str] = None) -> Optional[BackgroundJob]:
        """
        Atomically lock and fetch the next pending job.
        Uses database-level locking to prevent race conditions.
        
        Args:
            tenant_id: Optional tenant ID to filter jobs
            
        Returns:
            BackgroundJob instance if found, None otherwise
        """
        # Check circuit breaker before attempting connection
        if self._is_circuit_breaker_open():
            logger.debug(f"[Worker {self.worker_id}] Circuit breaker open, skipping job fetch")
            return None
        
        # Close old connections before attempting new operation
        close_old_connections()
        
        now = timezone.now()
        
        # Build query for pending jobs
        query = Q(
            status=JobStatus.PENDING,
            attempts__lt=F('max_attempts')
        )
        
        # Filter by scheduled_at if set
        query &= (Q(scheduled_at__isnull=True) | Q(scheduled_at__lte=now))
        
        if tenant_id:
            query &= Q(tenant_id=tenant_id)

        if self._job_types:
            query &= Q(job_type__in=self._job_types)
        if self._exclude_job_types:
            query &= ~Q(job_type__in=self._exclude_job_types)

        # Use select_for_update to lock the row
        try:
            with transaction.atomic():
                job = (
                    BackgroundJob.objects
                    .select_for_update(skip_locked=True)  # Skip already locked rows
                    .filter(query)
                    .order_by('-priority', 'created_at')
                    .first()
                )
                
                if job:
                    # Lock the job and increment attempts atomically
                    BackgroundJob.objects.filter(pk=job.pk).update(
                        status=JobStatus.PROCESSING,
                        locked_by=self.worker_id,
                        locked_at=now,
                        attempts=F('attempts') + 1
                    )
                    job.refresh_from_db()
                    logger.debug(f"[Worker {self.worker_id}] Locked job {job.id}")
                
                # Reset error count on successful operation
                self._reset_connection_error_count()
                # Log connection stats periodically for monitoring
                if logger.isEnabledFor(logging.DEBUG):
                    self._log_connection_pool_stats()
                return job
        except (InterfaceError, OperationalError) as e:
            # Database connection issues are recoverable in long-running workers.
            # Close old connections so Django can establish a fresh one on next use.
            self._record_connection_error()
            logger.error(
                f"[Worker {self.worker_id}] Database error while locking job: {e}",
                exc_info=True,
            )
            close_old_connections()
            return None
        except Exception as e:
            logger.error(f"[Worker {self.worker_id}] Error locking job: {e}", exc_info=True)
            return None
    
    def process_job(self, job: BackgroundJob) -> tuple[bool, str]:
        """
        Process a single job.
        
        Args:
            job: The BackgroundJob instance to process
            
        Returns:
            Tuple of (success: bool, error_message: str)
            If successful, returns (True, "")
            If failed, returns (False, error_message)
        """
        import time
        start_time = time.time()
        
        try:
            logger.info(f"[Worker {self.worker_id}] Processing job {job.id}: {job.job_type}")
            
            # Get handler from registry
            handler = self._handler_registry.get_handler(job.job_type)
            
            # Process the job - handlers should raise exceptions on failure
            # Handlers will set job.result themselves
            success = handler.process(job)
            
            execution_time = time.time() - start_time
            
            if success:
                # Ensure result has execution metadata if handler didn't add it
                if job.result:
                    if isinstance(job.result, dict):
                        if "execution_time_seconds" not in job.result:
                            job.result["execution_time_seconds"] = round(execution_time, 3)
                        if "worker_id" not in job.result:
                            job.result["worker_id"] = self.worker_id
                    # If result is not a dict, wrap it
                    elif not isinstance(job.result, dict):
                        job.result = {
                            "success": True,
                            "result": job.result,
                            "execution_time_seconds": round(execution_time, 3),
                            "worker_id": self.worker_id,
                            "timestamp": timezone.now().isoformat()
                        }
                else:
                    # No result set by handler, create a basic one
                    job.result = {
                        "success": True,
                        "execution_time_seconds": round(execution_time, 3),
                        "worker_id": self.worker_id,
                        "timestamp": timezone.now().isoformat()
                    }
                
                return (True, "")
            else:
                # Handler returned False without raising exception (shouldn't happen with updated handlers)
                error_msg = f"Handler returned False for job type {job.job_type}"
                logger.warning(f"[Worker {self.worker_id}] {error_msg}")
                
                # Store failure result
                job.result = {
                    "success": False,
                    "error": error_msg,
                    "execution_time_seconds": round(execution_time, 3),
                    "worker_id": self.worker_id,
                    "timestamp": timezone.now().isoformat()
                }
                
                return (False, error_msg)
                
        except KeyError as e:
            error_msg = f"Unknown job type: {job.job_type}"
            logger.error(f"[Worker {self.worker_id}] {error_msg} for job {job.id}")
            
            execution_time = time.time() - start_time
            job.result = {
                "success": False,
                "error": error_msg,
                "execution_time_seconds": round(execution_time, 3),
                "worker_id": self.worker_id,
                "timestamp": timezone.now().isoformat()
            }
            
            return (False, error_msg)
        except Exception as e:
            error_msg = str(e)
            logger.error(
                f"[Worker {self.worker_id}] Error processing job {job.id}: {error_msg}",
                exc_info=True
            )
            
            execution_time = time.time() - start_time
            # Store error result if handler didn't set one
            if not job.result:
                job.result = {
                    "success": False,
                    "error": error_msg,
                    "error_type": type(e).__name__,
                    "execution_time_seconds": round(execution_time, 3),
                    "worker_id": self.worker_id,
                    "timestamp": timezone.now().isoformat()
                }
            
            return (False, error_msg)
    
    def mark_job_complete(self, job: BackgroundJob, result: Optional[Dict[str, Any]] = None):
        """
        Mark a job as completed.
        
        Args:
            job: The BackgroundJob instance
            result: Optional result data to store (if not already set in job.result)
        """
        job.status = JobStatus.COMPLETED
        job.completed_at = timezone.now()
        job.locked_by = None
        job.locked_at = None
        
        # Store result - prefer the one passed, but use job.result if it's already set
        if result is not None:
            job.result = result
        # If result is None but job.result is already set (by handler), keep it
        # If both are None, that's fine - we'll store None for debugging
        
        job.save(update_fields=['status', 'completed_at', 'locked_by', 'locked_at', 'result'])
        logger.info(
            f"[Worker {self.worker_id}] Completed job {job.id} "
            f"(result stored: {job.result is not None})"
        )
    
    def mark_job_failed(self, job: BackgroundJob, error: str):
        """
        Mark a job as failed or schedule for retry.
        
        Args:
            job: The BackgroundJob instance
            error: Error message
        """
        job.last_error = error[:1000]  # Truncate long errors
        
        # Check if we should retry
        if job.attempts < job.max_attempts:
            # Schedule for retry
            handler = self._handler_registry.get_handler(job.job_type)
            retry_delay = handler.get_retry_delay(job.attempts)
            job.scheduled_at = timezone.now() + timedelta(seconds=retry_delay)
            job.status = JobStatus.RETRYING
            logger.info(
                f"[Worker {self.worker_id}] Job {job.id} will retry "
                f"(attempt {job.attempts}/{job.max_attempts}) after {retry_delay}s"
            )
            # Reset to PENDING for retry
            job.status = JobStatus.PENDING
        else:
            job.status = JobStatus.FAILED
            logger.error(
                f"[Worker {self.worker_id}] Job {job.id} failed permanently "
                f"after {job.attempts} attempts"
            )
        
        job.locked_by = None
        job.locked_at = None
        job.save(update_fields=['status', 'last_error', 'locked_by', 'locked_at', 'scheduled_at'])
    
    def cleanup_stale_locks(self, stale_threshold_minutes: int = 5):
        """
        Reset jobs that have been PROCESSING for too long (likely from crashed workers).

        Jobs that already exhausted ``max_attempts`` are marked ``FAILED`` instead of
        being reset to ``PENDING``, which would otherwise create unrunnable zombies.
        
        Args:
            stale_threshold_minutes: Minutes after which a lock is considered stale
            
        Returns:
            Number of stale jobs updated
        """
        threshold = timezone.now() - timedelta(minutes=stale_threshold_minutes)

        stale_jobs = BackgroundJob.objects.filter(
            status=JobStatus.PROCESSING,
            locked_at__lt=threshold,
        )

        failed_count = stale_jobs.filter(attempts__gte=F("max_attempts")).update(
            status=JobStatus.FAILED,
            locked_by=None,
            locked_at=None,
        )
        reset_count = stale_jobs.filter(attempts__lt=F("max_attempts")).update(
            status=JobStatus.PENDING,
            locked_by=None,
            locked_at=None,
        )

        count = failed_count + reset_count
        if failed_count > 0:
            logger.warning(
                f"[Worker {self.worker_id}] Marked {failed_count} exhausted stale jobs "
                f"as FAILED"
            )
        if reset_count > 0:
            logger.warning(
                f"[Worker {self.worker_id}] Reset {reset_count} stale jobs "
                f"that were stuck in PROCESSING"
            )

        return count

    def cleanup_exhausted_pending_jobs(self) -> int:
        """
        Mark ``PENDING`` jobs with ``attempts >= max_attempts`` as ``FAILED``.

        These are zombie rows: the worker will never pick them up
        (``attempts__lt=max_attempts``), but they still look active to schedulers.
        """
        count = BackgroundJob.objects.filter(
            status=JobStatus.PENDING,
            attempts__gte=F("max_attempts"),
        ).update(
            status=JobStatus.FAILED,
            locked_by=None,
            locked_at=None,
        )
        if count > 0:
            logger.warning(
                f"[Worker {self.worker_id}] Marked {count} exhausted PENDING jobs "
                f"as FAILED"
            )
        return count

    def _maybe_enqueue_process_dumped_tickets(self):
        """
        Every SUPPORT_TICKET_DUMP_ENQUEUE_INTERVAL seconds, enqueue
        process_dumped_tickets for each tenant with unprocessed dump rows.

        Uses :class:`core.models.EntityTypeDiscoverySyncState` (keyed by
        ``PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME``) so the 5-minute cadence
        is consistent across Gunicorn workers and survives process restarts.
        """
        now = timezone.now()
        if self._last_support_ticket_dump_enqueue_at is not None:
            elapsed = (now - self._last_support_ticket_dump_enqueue_at).total_seconds()
            if elapsed < PROCESS_DUMPED_TICKETS_LOCAL_CHECK_INTERVAL:
                return
        self._last_support_ticket_dump_enqueue_at = now

        try:
            with transaction.atomic():
                scheduler_state, _created = (
                    EntityTypeDiscoverySyncState.objects.select_for_update().get_or_create(
                        job_name=PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME,
                        defaults={
                            "last_processed_updated_at": None,
                            "last_processed_record_id": 0,
                        },
                    )
                )

                if scheduler_state.last_success_at is not None:
                    elapsed = (now - scheduler_state.last_success_at).total_seconds()
                    if elapsed < SUPPORT_TICKET_DUMP_ENQUEUE_INTERVAL:
                        logger.debug(
                            "[Worker %s] process_dumped_tickets scheduler throttled "
                            "(elapsed=%.1fs)",
                            self.worker_id,
                            elapsed,
                        )
                        return

                from support_ticket.views import enqueue_process_dumped_tickets_for_pending_dumps

                result = enqueue_process_dumped_tickets_for_pending_dumps()
                scheduler_state.last_success_at = now
                scheduler_state.last_error = None
                scheduler_state.updated_at = now
                scheduler_state.save(
                    update_fields=["last_success_at", "last_error", "updated_at"]
                )

            enqueued = result.get("enqueued") or []
            if enqueued:
                logger.info(
                    f"[Worker {self.worker_id}] Enqueued process_dumped_tickets for "
                    f"{len(enqueued)} tenant(s)"
                )
            else:
                logger.debug(
                    f"[Worker {self.worker_id}] process_dumped_tickets tick: "
                    "no tenants with pending dumps (or jobs already active)"
                )
        except Exception as e:
            EntityTypeDiscoverySyncState.objects.update_or_create(
                job_name=PROCESS_DUMPED_TICKETS_SCHEDULER_JOB_NAME,
                defaults={"last_error": str(e)[:1000]},
            )
            logger.warning(
                f"[Worker {self.worker_id}] Failed to enqueue process_dumped_tickets: {e}",
                exc_info=True,
            )

    def _maybe_enqueue_entity_type_discovery(self):
        """
        Periodically enqueue one global entity type discovery job.
        """
        now = timezone.now()
        if self._last_entity_type_discovery_enqueue_at is not None:
            elapsed = (now - self._last_entity_type_discovery_enqueue_at).total_seconds()
            if elapsed < ENTITY_TYPE_DISCOVERY_LOCAL_CHECK_INTERVAL:
                return
        self._last_entity_type_discovery_enqueue_at = now

        try:
            with transaction.atomic():
                scheduler_state, _created = (
                    EntityTypeDiscoverySyncState.objects.select_for_update().get_or_create(
                        job_name=ENTITY_TYPE_DISCOVERY_SCHEDULER_JOB_NAME,
                        defaults={
                            "last_processed_updated_at": None,
                            "last_processed_record_id": 0,
                        },
                    )
                )

                if scheduler_state.last_success_at is not None:
                    elapsed = (now - scheduler_state.last_success_at).total_seconds()
                    if elapsed < ENTITY_TYPE_DISCOVERY_ENQUEUE_INTERVAL:
                        logger.info(
                            "[Worker %s] Entity type discovery scheduler throttled "
                            "(elapsed=%.1fs)",
                            self.worker_id,
                            elapsed,
                        )
                        return

                active_exists = BackgroundJob.objects.filter(
                    job_type=JobType.DISCOVER_ENTITY_TYPES,
                    status__in=[JobStatus.PENDING, JobStatus.PROCESSING, JobStatus.RETRYING],
                ).exists()
                if active_exists:
                    logger.debug(
                        f"[Worker {self.worker_id}] Entity type discovery job already active"
                    )
                    return

                queue = get_queue_service()
                queue.enqueue_job(
                    job_type=JobType.DISCOVER_ENTITY_TYPES,
                    payload={"batch_size": 1000},
                    priority=-1,
                    max_attempts=3,
                )
                scheduler_state.last_success_at = now
                scheduler_state.last_error = None
                scheduler_state.updated_at = now
                scheduler_state.save(update_fields=["last_success_at", "last_error", "updated_at"])
            logger.info(f"[Worker {self.worker_id}] Enqueued entity type discovery")
        except Exception as e:
            EntityTypeDiscoverySyncState.objects.update_or_create(
                job_name=ENTITY_TYPE_DISCOVERY_SCHEDULER_JOB_NAME,
                defaults={"last_error": str(e)[:1000]},
            )
            logger.warning(
                f"[Worker {self.worker_id}] Failed to enqueue entity type discovery: {e}",
                exc_info=True,
            )

    def _maybe_enqueue_lead_cron_jobs(self):
        """
        Every LEAD_CRON_ENQUEUE_INTERVAL seconds, enqueue unassign_snoozed_leads,
        release_leads_after_12h, and close_stale_self_trial_support_tickets so the worker runs them
        without needing external cron.
        """
        now = timezone.now()
        if self._last_lead_cron_enqueue_at is not None:
            elapsed = (now - self._last_lead_cron_enqueue_at).total_seconds()
            if elapsed < LEAD_CRON_ENQUEUE_INTERVAL:
                return
        self._last_lead_cron_enqueue_at = now
        try:
            with scheduler_lock(SCHEDULER_LOCK_LEAD_CRON) as acquired:
                if not acquired:
                    return
                queue = get_queue_service()
                enqueue_for_all_tenants(
                    queue, job_type=JobType.UNASSIGN_SNOOZED_LEADS, payload={}, priority=0
                )
                enqueue_for_all_tenants(
                    queue, job_type=JobType.RELEASE_LEADS_AFTER_12H, payload={}, priority=0
                )
                enqueue_for_all_tenants(
                    queue,
                    job_type=JobType.CLOSE_STALE_SELF_TRIAL_SUPPORT_TICKETS,
                    payload={"days": 15},
                    priority=0,
                )
            logger.debug(
                f"[Worker {self.worker_id}] Enqueued lead maintenance jobs per tenant "
                f"(unassign_snoozed_leads, release_leads_after_12h, close_stale_self_trial_support_tickets)"
            )
        except Exception as e:
            logger.warning(
                f"[Worker {self.worker_id}] Failed to enqueue lead cron jobs: {e}",
                exc_info=True,
            )

    def _maybe_enqueue_snoozed_to_not_connected_midnight(self):
        """
        Once per calendar day (in ``TIME_ZONE``), enqueue snoozed_to_not_connected_midnight
        only when local time is exactly the configured hour:minute (e.g. 23:55).

        End-of-day enqueue aligns ``NOW()`` date with same-calendar-day ``next_call_at`` in the handler.
        """
        tz_name = settings.TIME_ZONE
        try:
            tz = ZoneInfo(tz_name)
        except Exception as e:
            logger.warning(
                f"[Worker {self.worker_id}] Invalid TIME_ZONE={tz_name!r}: {e}"
            )
            return

        local_now = datetime.now(tz)
        if (
            local_now.hour != SNOOZED_TO_NOT_CONNECTED_ENQUEUE_HOUR
            or local_now.minute != SNOOZED_TO_NOT_CONNECTED_ENQUEUE_MINUTE
        ):
            return

        today = local_now.date()
        if self._last_snoozed_midnight_enqueue_date == today:
            return

        try:
            with scheduler_lock(SCHEDULER_LOCK_SNOOZED_MIDNIGHT) as acquired:
                if not acquired:
                    return
                queue = get_queue_service()
                enqueue_for_all_tenants(
                    queue,
                    job_type=JobType.SNOOZED_TO_NOT_CONNECTED_MIDNIGHT,
                    payload={},
                    priority=0,
                )
                self._last_snoozed_midnight_enqueue_date = today
            logger.info(
                f"[Worker {self.worker_id}] Enqueued snoozed_to_not_connected_midnight "
                f"(local_date={today}, tz={tz_name}, at={SNOOZED_TO_NOT_CONNECTED_ENQUEUE_HOUR:02d}:"
                f"{SNOOZED_TO_NOT_CONNECTED_ENQUEUE_MINUTE:02d})"
            )
        except Exception as e:
            logger.warning(
                f"[Worker {self.worker_id}] Failed to enqueue snoozed_to_not_connected_midnight: {e}",
                exc_info=True,
            )

    def _maybe_enqueue_dispatch_sync(self):
        """
        Enqueue ``sync_dispatch_to_records`` at exactly DISPATCH_SYNC_ENQUEUE_MINUTE
        on each hour in DISPATCH_SYNC_ENQUEUE_HOURS (UTC). The (date, hour) bucket
        guard means the worker enqueues at most once per 8-hour window even if the
        loop ticks several times within that minute.
        """
        now = timezone.now()
        if (
            now.hour not in DISPATCH_SYNC_ENQUEUE_HOURS
            or now.minute != DISPATCH_SYNC_ENQUEUE_MINUTE
        ):
            return

        bucket = (now.date(), now.hour)
        if self._last_dispatch_sync_enqueue_bucket == bucket:
            return

        try:
            with scheduler_lock(SCHEDULER_LOCK_DISPATCH_SYNC) as acquired:
                if not acquired:
                    return
                queue = get_queue_service()
                queue.enqueue_job(
                    job_type=JobType.SYNC_DISPATCH_TO_RECORDS,
                    payload={},
                    priority=0,
                )
                self._last_dispatch_sync_enqueue_bucket = bucket
            logger.info(
                f"[Worker {self.worker_id}] Enqueued sync_dispatch_to_records "
                f"(utc_date={now.date()}, hour={now.hour:02d}:"
                f"{DISPATCH_SYNC_ENQUEUE_MINUTE:02d})"
            )
        except Exception as e:
            logger.warning(
                f"[Worker {self.worker_id}] Failed to enqueue sync_dispatch_to_records: {e}",
                exc_info=True,
            )

    def _maybe_enqueue_log_retention(self):
        """
        Enqueue :data:`~background_jobs.models.JobType.PURGE_OLD_LOG_TABLES` at most
        once per :data:`LOG_RETENTION_ENQUEUE_INTERVAL` so old audit/log rows are removed
        without external cron. Finished job rows (COMPLETED/FAILED) in ``background_jobs``
        are pruned; active queue states are not removed.

        Uses :class:`core.models.EntityTypeDiscoverySyncState` for cross-process throttle
        and skips tenants with an active purge job or a successful purge in the last
        :data:`LOG_RETENTION_ENQUEUE_INTERVAL` (unless ``has_more`` continuation is pending).
        """
        now = timezone.now()
        if self._last_log_retention_enqueue_at is not None:
            elapsed = (now - self._last_log_retention_enqueue_at).total_seconds()
            if elapsed < LOG_RETENTION_LOCAL_CHECK_INTERVAL:
                return
        self._last_log_retention_enqueue_at = now

        try:
            with transaction.atomic():
                scheduler_state, _created = (
                    EntityTypeDiscoverySyncState.objects.select_for_update().get_or_create(
                        job_name=LOG_RETENTION_SCHEDULER_JOB_NAME,
                        defaults={
                            "last_processed_updated_at": None,
                            "last_processed_record_id": 0,
                        },
                    )
                )

                if scheduler_state.last_success_at is not None:
                    elapsed = (now - scheduler_state.last_success_at).total_seconds()
                    if elapsed < LOG_RETENTION_ENQUEUE_INTERVAL:
                        logger.debug(
                            "[Worker %s] purge_old_log_tables scheduler throttled "
                            "(elapsed=%.1fs)",
                            self.worker_id,
                            elapsed,
                        )
                        return

                days = int(getattr(settings, "LOG_RETENTION_DAYS", 30))
                chunk_size = int(getattr(settings, "LOG_RETENTION_CHUNK_SIZE", 500))
                max_chunks_per_table = int(
                    getattr(settings, "LOG_RETENTION_MAX_CHUNKS_PER_TABLE", 20)
                )
                payload = {
                    "days": days,
                    "chunk_size": chunk_size,
                    "max_chunks_per_table": max_chunks_per_table,
                }
                queue = get_queue_service()
                enqueued = 0
                skipped = 0
                for tid in iter_active_tenant_ids():
                    tid_str = str(tid)
                    if not tenant_should_enqueue_purge(
                        tid_str,
                        now=now,
                        interval_seconds=LOG_RETENTION_ENQUEUE_INTERVAL,
                    ):
                        skipped += 1
                        continue
                    queue.enqueue_job(
                        job_type=JobType.PURGE_OLD_LOG_TABLES,
                        payload=payload,
                        priority=0,
                        tenant_id=tid_str,
                    )
                    enqueued += 1

                scheduler_state.last_success_at = now
                scheduler_state.last_error = None
                scheduler_state.updated_at = now
                scheduler_state.save(
                    update_fields=["last_success_at", "last_error", "updated_at"]
                )

            logger.info(
                f"[Worker {self.worker_id}] purge_old_log_tables scheduler: "
                f"enqueued={enqueued} skipped={skipped} (days={days})"
            )
        except Exception as e:
            EntityTypeDiscoverySyncState.objects.update_or_create(
                job_name=LOG_RETENTION_SCHEDULER_JOB_NAME,
                defaults={"last_error": str(e)[:1000]},
            )
            logger.warning(
                f"[Worker {self.worker_id}] Failed to enqueue purge_old_log_tables: {e}",
                exc_info=True,
            )

    def process_next_job(self, tenant_id: Optional[str] = None) -> bool:
        """
        Process the next available job.
        
        Args:
            tenant_id: Optional tenant ID to filter jobs
            
        Returns:
            True if a job was processed, False if no jobs available
        """
        job = self.lock_and_fetch_job(tenant_id)
        
        if not job:
            return False
        
        try:
            success, error_msg = self.process_job(job)
            
            if success:
                # Result should already be set in job.result by the handler or process_job
                # Always save the result, even if it's None (for debugging)
                self.mark_job_complete(job, result=job.result)
            else:
                # Use the actual error message from the handler
                # Result should already be set in job.result by process_job
                self.mark_job_failed(job, error_msg or "Job processing failed")
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            self.mark_job_failed(job, error_msg)
            return True  # We did process it, just failed
    
    def start_worker_loop(
        self,
        poll_interval: float = 1.0,
        batch_size: int = 10,
        stale_cleanup_interval: int = 10,
        run_schedulers: bool = True,
    ):
        """
        Start the worker loop in the current thread.
        This will run continuously until stop() is called.
        
        Args:
            poll_interval: Seconds to wait between polls when no jobs available
            batch_size: Number of jobs to process per batch
            stale_cleanup_interval: Number of iterations between stale lock cleanups
            run_schedulers: When False, only process jobs (no cron enqueue ticks)
        """
        self._run_schedulers = run_schedulers
        logger.info(
            f"[Worker {self.worker_id}] Starting background job processor "
            f"(schedulers={'on' if run_schedulers else 'off'})"
        )
        
        consecutive_empty_polls = 0
        max_empty_polls = 10  # After 10 empty polls, increase wait time
        iteration_count = 0
        consecutive_errors = 0
        max_consecutive_errors = 3  # Exponential backoff after 3 errors
        
        while not self._stop_event.is_set():
            try:
                # Check circuit breaker before proceeding
                if self._is_circuit_breaker_open():
                    wait_time = self._circuit_breaker_timeout
                    logger.debug(
                        f"[Worker {self.worker_id}] Circuit breaker open, waiting {wait_time}s before retry"
                    )
                    self._stop_event.wait(wait_time)
                    continue
                
                # Ensure we don't hold on to stale/closed DB connections in a long-running worker.
                close_old_connections()
                jobs_processed = 0
                
                # Process a batch of jobs
                for _ in range(batch_size):
                    if self._stop_event.is_set():
                        break
                    
                    if self.process_next_job():
                        jobs_processed += 1
                        consecutive_empty_polls = 0
                        consecutive_errors = 0  # Reset error count on success
                    else:
                        # No more jobs available, break inner loop
                        break
                
                # Cleanup stale locks periodically
                iteration_count += 1
                if iteration_count >= stale_cleanup_interval:
                    self.cleanup_stale_locks()
                    self.cleanup_exhausted_pending_jobs()
                    iteration_count = 0

                if self._run_schedulers:
                    # Every 5 min: process support_ticket_dump → support_ticket + records
                    self._maybe_enqueue_process_dumped_tickets()
                    # Every 5 min: discover tenant entity types and fields from changed records
                    self._maybe_enqueue_entity_type_discovery()
                    # Periodically enqueue lead cron jobs (unassign snoozed, release after 12h) so no external cron is needed
                    self._maybe_enqueue_lead_cron_jobs()
                    # Daily at 23:55 exact minute (TIME_ZONE): SNOOZED → NOT_CONNECTED
                    self._maybe_enqueue_snoozed_to_not_connected_midnight()
                    # Every 8 hours at :05 UTC (5 min after Airbyte sync): dispatch sheet → records
                    self._maybe_enqueue_dispatch_sync()
                    # Periodic purge of object_history, event_logs, rule_exec_logs, finished background_jobs
                    self._maybe_enqueue_log_retention()

                if jobs_processed > 0:
                    logger.debug(
                        f"[Worker {self.worker_id}] Processed {jobs_processed} job(s)"
                    )
                    consecutive_empty_polls = 0
                    consecutive_errors = 0
                else:
                    consecutive_empty_polls += 1
                    # Adaptive polling: wait longer if no jobs for a while
                    wait_time = poll_interval * min(consecutive_empty_polls, max_empty_polls)
                    self._stop_event.wait(wait_time)
                    
            except (InterfaceError, OperationalError) as e:
                # Database connection has likely been closed by the server (e.g. idle timeout).
                # Reset Django's connection state so the next iteration can obtain a fresh one.
                consecutive_errors += 1
                self._record_connection_error()
                
                logger.error(
                    f"[Worker {self.worker_id}] Database connection error in worker loop "
                    f"(consecutive errors: {consecutive_errors}): {e}",
                    exc_info=True,
                )
                close_old_connections()
                
                # Exponential backoff: wait longer with each consecutive error
                if consecutive_errors >= max_consecutive_errors:
                    backoff_time = min(poll_interval * (2 ** (consecutive_errors - max_consecutive_errors + 1)), 60)
                    logger.warning(
                        f"[Worker {self.worker_id}] Exponential backoff: waiting {backoff_time}s "
                        f"after {consecutive_errors} consecutive errors"
                    )
                    self._stop_event.wait(backoff_time)
                else:
                    # Wait a bit before retrying on error
                    self._stop_event.wait(poll_interval * 2)
                    
            except Exception as e:
                logger.error(
                    f"[Worker {self.worker_id}] Error in worker loop: {e}",
                    exc_info=True
                )
                # Wait a bit before retrying on error
                self._stop_event.wait(poll_interval * 2)
        
        logger.info(f"[Worker {self.worker_id}] Background job processor stopped")
    
    def stop(self):
        """Stop the worker loop"""
        self._stop_event.set()

