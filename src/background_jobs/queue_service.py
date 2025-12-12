"""
Queue Service Interface

High-level API for enqueueing and managing background jobs.
"""
import logging
import pickle
import base64
import inspect
from typing import Optional, Dict, Any, Callable
from datetime import datetime
from django.utils import timezone
from .models import BackgroundJob, JobStatus, JobType
from .job_handlers import get_handler_registry

logger = logging.getLogger(__name__)


class QueueService:
    """
    High-level service for managing background jobs.
    Provides a clean API for enqueueing jobs and checking their status.
    """
    
    def __init__(self):
        self._handler_registry = get_handler_registry()
    
    def enqueue_job(
        self,
        job_type: str,
        payload: Dict[str, Any],
        priority: int = 0,
        scheduled_at: Optional[datetime] = None,
        tenant_id: Optional[str] = None,
        max_attempts: Optional[int] = None
    ) -> BackgroundJob:
        """
        Enqueue a new background job.
        
        Args:
            job_type: The type of job (must be a valid JobType choice)
            payload: Job-specific data dictionary
            priority: Job priority (higher = more priority, default=0)
            scheduled_at: Optional datetime for delayed execution
            tenant_id: Optional tenant ID
            max_attempts: Optional max retry attempts (defaults to 3)
            
        Returns:
            The created BackgroundJob instance
            
        Raises:
            ValueError: If job_type is invalid or payload validation fails
        """
        # Validate job type
        if not self._handler_registry.has_handler(job_type):
            raise ValueError(f"Invalid job type: {job_type}. No handler registered.")
        
        # Validate payload using handler
        handler = self._handler_registry.get_handler(job_type)
        if hasattr(handler, 'validate_payload'):
            if not handler.validate_payload(payload):
                raise ValueError(f"Invalid payload for job type {job_type}")
        
        # Create the job
        job = BackgroundJob.objects.create(
            job_type=job_type,
            status=JobStatus.PENDING,
            priority=priority,
            payload=payload,
            scheduled_at=scheduled_at,
            tenant_id=tenant_id,
            max_attempts=max_attempts or 3
        )
        
        logger.info(
            f"Enqueued job {job.id}: type={job_type} priority={priority} "
            f"tenant={tenant_id}"
        )
        
        return job
    
    def get_job_status(self, job_id: int) -> Dict[str, Any]:
        """
        Get the status of a job.
        
        Args:
            job_id: The job ID
            
        Returns:
            Dictionary with job status information
            
        Raises:
            BackgroundJob.DoesNotExist: If job not found
        """
        job = BackgroundJob.objects.get(pk=job_id)
        
        return {
            "id": job.id,
            "job_type": job.job_type,
            "status": job.status,
            "priority": job.priority,
            "attempts": job.attempts,
            "max_attempts": job.max_attempts,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "scheduled_at": job.scheduled_at.isoformat() if job.scheduled_at else None,
            "last_error": job.last_error,
            "result": job.result,
            "locked_by": job.locked_by,
            "locked_at": job.locked_at.isoformat() if job.locked_at else None,
        }
    
    def retry_failed_job(self, job_id: int) -> BackgroundJob:
        """
        Manually retry a failed job by resetting it to PENDING.
        
        Args:
            job_id: The job ID to retry
            
        Returns:
            The updated BackgroundJob instance
            
        Raises:
            BackgroundJob.DoesNotExist: If job not found
            ValueError: If job is not in a retryable state
        """
        job = BackgroundJob.objects.get(pk=job_id)
        
        if job.status not in [JobStatus.FAILED, JobStatus.RETRYING]:
            raise ValueError(
                f"Job {job_id} is in status {job.status}, cannot retry. "
                f"Only FAILED or RETRYING jobs can be retried."
            )
        
        # Reset job for retry
        job.status = JobStatus.PENDING
        job.attempts = 0  # Reset attempts for manual retry
        job.last_error = None
        job.scheduled_at = None
        job.locked_by = None
        job.locked_at = None
        job.save(update_fields=[
            'status', 'attempts', 'last_error', 'scheduled_at',
            'locked_by', 'locked_at'
        ])
        
        logger.info(f"Manually retrying job {job_id}")
        
        return job
    
    def queue_function(
        self,
        func: Callable,
        *args,
        priority: int = 0,
        scheduled_at: Optional[datetime] = None,
        tenant_id: Optional[str] = None,
        max_attempts: Optional[int] = None,
        **kwargs
    ) -> BackgroundJob:
        """
        Queue a function to be executed asynchronously.
        
        This is a convenience method that allows you to queue any function
        without defining a custom handler. The function and its arguments
        are serialized and stored in the job payload.
        
        Example:
            def time_taking_function(user_id, data):
                # Do something time-consuming
                return result
            
            queue_service = get_queue_service()
            job = queue_service.queue_function(
                time_taking_function,
                user_id=123,
                data={"key": "value"},
                priority=5
            )
        
        Args:
            func: The function to execute
            *args: Positional arguments to pass to the function
            priority: Job priority (higher = more priority, default=0)
            scheduled_at: Optional datetime for delayed execution
            tenant_id: Optional tenant ID
            max_attempts: Optional max retry attempts (defaults to 3)
            **kwargs: Keyword arguments to pass to the function
            
        Returns:
            The created BackgroundJob instance
            
        Raises:
            ValueError: If function cannot be serialized or arguments are invalid
        """
        # Try to get function module and name
        func_module = inspect.getmodule(func)
        func_name = func.__name__
        
        # Check if function is from a module (not a lambda/closure)
        if func_module and hasattr(func_module, '__name__'):
            module_name = func_module.__name__
            # Use module-based approach
            payload = {
                "function_module": module_name,
                "function_name": func_name,
                "args": args,
                "kwargs": kwargs,
            }
        else:
            # Function is a lambda, closure, or not from a module
            # Serialize the entire function using pickle
            try:
                pickled_func = pickle.dumps(func)
                pickled_b64 = base64.b64encode(pickled_func).decode('utf-8')
                payload = {
                    "function_module": None,
                    "function_name": None,
                    "function_pickle": pickled_b64,
                    "args": args,
                    "kwargs": kwargs,
                }
                logger.warning(
                    f"Function {func_name} is not from a module, using pickle serialization. "
                    f"This may have limitations with closures and certain function types."
                )
            except Exception as e:
                raise ValueError(
                    f"Cannot serialize function {func_name}: {str(e)}. "
                    f"Make sure the function is defined in a module, not as a lambda or closure."
                ) from e
        
        # Validate that args and kwargs are JSON-serializable
        try:
            import json
            json.dumps({"args": args, "kwargs": kwargs})
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"Function arguments must be JSON-serializable: {str(e)}"
            ) from e
        
        # Enqueue as execute_function job type
        return self.enqueue_job(
            job_type=JobType.EXECUTE_FUNCTION,
            payload=payload,
            priority=priority,
            scheduled_at=scheduled_at,
            tenant_id=tenant_id,
            max_attempts=max_attempts
        )


# Global service instance
_queue_service = None


def get_queue_service() -> QueueService:
    """Get the global queue service instance"""
    global _queue_service
    if _queue_service is None:
        _queue_service = QueueService()
    return _queue_service

