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
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo

from django.conf import settings

from django.utils import timezone
from django.db import transaction, close_old_connections, connections
from django.db.models import Q, F
from django.db.utils import InterfaceError, OperationalError

from .models import BackgroundJob, JobStatus, JobType
from .queue_service import get_queue_service
from .job_handlers import get_handler_registry

logger = logging.getLogger(__name__)

# Interval (seconds) between enqueueing lead cron jobs from the worker (no external cron needed)
LEAD_CRON_ENQUEUE_INTERVAL = 900  # 15 minutes


class JobProcessor:
    """Processes jobs from the BackgroundJob queue"""
    
    def __init__(self, worker_id: str = "default"):
        """
        Initialize the job processor.
        
        Args:
            worker_id: Unique identifier for this worker instance
        """
        self.worker_id = worker_id
        self._stop_event = threading.Event()
        self._handler_registry = get_handler_registry()
        # Last time we enqueued lead cron jobs (unassign snoozed, release after 12h)
        self._last_lead_cron_enqueue_at = None
        # UTC calendar date we last enqueued snoozed→NOT_CONNECTED job (see TIME_ZONE)
        self._last_snoozed_midnight_enqueue_date = None
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
        
        Args:
            stale_threshold_minutes: Minutes after which a lock is considered stale
            
        Returns:
            Number of stale jobs reset
        """
        threshold = timezone.now() - timedelta(minutes=stale_threshold_minutes)
        
        stale_jobs = BackgroundJob.objects.filter(
            status=JobStatus.PROCESSING,
            locked_at__lt=threshold
        )
        
        count = stale_jobs.update(
            status=JobStatus.PENDING,
            locked_by=None,
            locked_at=None
        )
        
        if count > 0:
            logger.warning(
                f"[Worker {self.worker_id}] Reset {count} stale jobs "
                f"that were stuck in PROCESSING"
            )
        
        return count

    def _maybe_enqueue_lead_cron_jobs(self):
        """
        Every LEAD_CRON_ENQUEUE_INTERVAL seconds, enqueue unassign_snoozed_leads,
        release_leads_after_12h, and close_stale_subscription_leads so the worker runs them
        without needing external cron.
        """
        now = timezone.now()
        if self._last_lead_cron_enqueue_at is not None:
            elapsed = (now - self._last_lead_cron_enqueue_at).total_seconds()
            if elapsed < LEAD_CRON_ENQUEUE_INTERVAL:
                return
        try:
            queue = get_queue_service()
            queue.enqueue_job(job_type=JobType.UNASSIGN_SNOOZED_LEADS, payload={}, priority=0)
            queue.enqueue_job(job_type=JobType.RELEASE_LEADS_AFTER_12H, payload={}, priority=0)
            queue.enqueue_job(
                job_type=JobType.CLOSE_STALE_SUBSCRIPTION_LEADS,
                payload={"days": 15},
                priority=0,
            )
            self._last_lead_cron_enqueue_at = now
            logger.debug(
                f"[Worker {self.worker_id}] Enqueued lead maintenance jobs "
                f"(unassign_snoozed_leads, release_leads_after_12h, close_stale_subscription_leads)"
            )
        except Exception as e:
            logger.warning(
                f"[Worker {self.worker_id}] Failed to enqueue lead cron jobs: {e}",
                exc_info=True,
            )

    def _maybe_enqueue_snoozed_to_not_connected_midnight(self):
        """
        Once per UTC calendar day during hour 00:00–00:59, enqueue snoozed_to_not_connected_midnight.

        Uses ``TIME_ZONE`` (UTC) for wall-clock, consistent with stored datetimes.
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
        if local_now.hour != 0:
            return

        today = local_now.date()
        if self._last_snoozed_midnight_enqueue_date == today:
            return

        try:
            queue = get_queue_service()
            queue.enqueue_job(
                job_type=JobType.SNOOZED_TO_NOT_CONNECTED_MIDNIGHT,
                payload={},
                priority=0,
            )
            self._last_snoozed_midnight_enqueue_date = today
            logger.info(
                f"[Worker {self.worker_id}] Enqueued snoozed_to_not_connected_midnight "
                f"(local_date={today}, tz={tz_name})"
            )
        except Exception as e:
            logger.warning(
                f"[Worker {self.worker_id}] Failed to enqueue snoozed_to_not_connected_midnight: {e}",
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
        stale_cleanup_interval: int = 10
    ):
        """
        Start the worker loop in the current thread.
        This will run continuously until stop() is called.
        
        Args:
            poll_interval: Seconds to wait between polls when no jobs available
            batch_size: Number of jobs to process per batch
            stale_cleanup_interval: Number of iterations between stale lock cleanups
        """
        logger.info(f"[Worker {self.worker_id}] Starting background job processor")
        
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
                    iteration_count = 0

                # Periodically enqueue lead cron jobs (unassign snoozed, release after 12h) so no external cron is needed
                self._maybe_enqueue_lead_cron_jobs()
                # Daily at local midnight: SNOOZED → NOT_CONNECTED
                self._maybe_enqueue_snoozed_to_not_connected_midnight()

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

