"""
Job Handler Plugin System

This module provides the plugin-based handler system for processing different types
of background jobs. Each job type has its own handler that implements the JobHandler interface.
"""
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any
from django.utils import timezone
from .models import BackgroundJob
from support_ticket.services import MixpanelService

logger = logging.getLogger(__name__)


class JobHandler(ABC):
    """
    Abstract base class for all job handlers.
    
    Each job type should have a corresponding handler that implements this interface.
    """
    
    @abstractmethod
    def process(self, job: BackgroundJob) -> bool:
        """
        Process the job.
        
        Args:
            job: The BackgroundJob instance to process
            
        Returns:
            True if job completed successfully, False otherwise
        """
        pass
    
    @abstractmethod
    def get_retry_delay(self, attempt: int) -> int:
        """
        Get the retry delay in seconds for a given attempt number.
        Implements exponential backoff.
        
        Args:
            attempt: The attempt number (1-indexed, so attempt 1 = first retry)
            
        Returns:
            Delay in seconds before retrying
        """
        pass
    
    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        """
        Optional validation of job payload.
        Override this in subclasses if validation is needed.
        
        Args:
            payload: The job payload dictionary
            
        Returns:
            True if payload is valid, False otherwise
        """
        return True


class MixpanelJobHandler(JobHandler):
    """
    Handler for sending Mixpanel events.
    """
    
    def process(self, job: BackgroundJob) -> bool:
        """
        Process a Mixpanel event job.
        
        Expected payload:
        {
            "user_id": str,
            "event_name": str,
            "properties": dict
        }
        """
        logger.info(f"Mixpanel Job Payload: {job.payload}") 
        payload = job.payload
        user_id = payload.get("user_id")
        event_name = payload.get("event_name")
        properties = payload.get("properties", {})
        
        if not user_id or not event_name:
            error_msg = f"Invalid Mixpanel job payload: missing user_id or event_name (user_id={user_id}, event_name={event_name})"
            logger.error(f"Invalid Mixpanel job payload for job {job.id}: {error_msg}")
            raise ValueError(error_msg)
        
        try:
            import time
            start_time = time.time()
            
            service = MixpanelService()
            success = service.send_to_mixpanel_sync(
                str(user_id),
                str(event_name),
                properties,
            )
            
            execution_time = time.time() - start_time
            
            if success:
                # Store result with debugging information
                job.result = {
                    "success": True,
                    "event_name": event_name,
                    "user_id": str(user_id),
                    "properties": properties,
                    "execution_time_seconds": round(execution_time, 3),
                    "timestamp": timezone.now().isoformat()
                }
                
                logger.info(
                    f"Mixpanel event sent successfully for job {job.id}: "
                    f"event='{event_name}' user_id={user_id}"
                )
                return True
            else:
                # Determine the specific reason for failure
                import os
                if not os.environ.get("MIXPANEL_TOKEN"):
                    error_msg = "MIXPANEL_TOKEN not configured"
                else:
                    error_msg = "Mixpanel API call returned unsuccessful response"
                
                # Store failure result
                job.result = {
                    "success": False,
                    "event_name": event_name,
                    "user_id": str(user_id),
                    "error": error_msg,
                    "execution_time_seconds": round(execution_time, 3),
                    "timestamp": timezone.now().isoformat()
                }
                
                logger.warning(
                    f"Mixpanel event returned False for job {job.id}: "
                    f"event='{event_name}' user_id={user_id} - {error_msg}"
                )
                # Raise exception with descriptive message so it gets saved to job.last_error
                raise Exception(error_msg)
                
        except Exception as e:
            logger.error(
                f"Mixpanel event failed for job {job.id}: event='{event_name}' error={e}",
                exc_info=True
            )
            raise
    
    def get_retry_delay(self, attempt: int) -> int:
        """
        Exponential backoff: 1s, 10s, 60s
        """
        delays = [1, 10, 60]
        return delays[min(attempt - 1, len(delays) - 1)]
    
    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        """Validate Mixpanel job payload"""
        required_fields = ["user_id", "event_name"]
        for field in required_fields:
            if field not in payload:
                logger.error(f"Missing required field '{field}' in Mixpanel job payload")
                return False
        return True


class WebhookJobHandler(JobHandler):
    """
    Handler for sending webhook requests.
    """
    
    def process(self, job: BackgroundJob) -> bool:
        """
        Process a webhook job.
        
        Expected payload:
        {
            "url": str,
            "payload": dict (optional, defaults to empty dict)
        }
        """
        import requests
        
        payload = job.payload
        url = payload.get("url")
        webhook_payload = payload.get("payload", {})
        
        if not url:
            error_msg = f"Invalid webhook job payload: missing URL"
            logger.error(f"Invalid webhook job payload for job {job.id}: {error_msg}")
            raise ValueError(error_msg)
        
        try:
            import time
            
            start_time = time.time()
            
            response = requests.post(
                url,
                json=webhook_payload,
                timeout=10,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
            execution_time = time.time() - start_time
            
            # Store result with debugging information
            job.result = {
                "success": True,
                "url": url,
                "status_code": response.status_code,
                "response_size": len(response.content) if hasattr(response, 'content') else None,
                "execution_time_seconds": round(execution_time, 3),
                "timestamp": timezone.now().isoformat()
            }
            
            logger.info(
                f"Webhook sent successfully for job {job.id}: "
                f"url={url} status={response.status_code}"
            )
            return True
            
        except requests.exceptions.Timeout as e:
            error_msg = f"Webhook request timed out after 10s: {url}"
            logger.error(f"Webhook failed for job {job.id}: {error_msg}")
            raise Exception(error_msg) from e
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Webhook connection failed: {url} - {str(e)}"
            logger.error(f"Webhook failed for job {job.id}: {error_msg}")
            raise Exception(error_msg) from e
        except requests.exceptions.HTTPError as e:
            # response.raise_for_status() raises HTTPError, so response should be available
            status_code = getattr(e.response, 'status_code', 'unknown')
            response_text = getattr(e.response, 'text', '')[:200] if hasattr(e.response, 'text') else str(e)
            error_msg = f"Webhook HTTP error {status_code}: {url} - {response_text}"
            logger.error(f"Webhook failed for job {job.id}: {error_msg}")
            raise Exception(error_msg) from e
        except requests.exceptions.RequestException as e:
            error_msg = f"Webhook request failed: {url} - {str(e)}"
            logger.error(f"Webhook failed for job {job.id}: {error_msg}")
            raise Exception(error_msg) from e
    
    def get_retry_delay(self, attempt: int) -> int:
        """
        Exponential backoff: 2s, 20s, 120s
        """
        delays = [2, 20, 120]
        return delays[min(attempt - 1, len(delays) - 1)]
    
    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        """Validate webhook job payload"""
        if "url" not in payload:
            logger.error("Missing required field 'url' in webhook job payload")
            return False
        return True


class FunctionJobHandler(JobHandler):
    """
    Generic handler for executing any Python function.
    Functions are serialized using pickle and stored in the job payload.
    """
    
    def process(self, job: BackgroundJob) -> bool:
        """
        Process a function execution job.
        
        Expected payload:
        {
            "function_module": str,  # e.g., "myapp.utils"
            "function_name": str,     # e.g., "time_taking_function"
            "args": list,            # Positional arguments (must be JSON-serializable)
            "kwargs": dict,          # Keyword arguments (must be JSON-serializable)
            "function_pickle": str   # Optional: base64-encoded pickled function (for closures/lambdas)
        }
        """
        import pickle
        import base64
        import importlib
        
        payload = job.payload
        function_module = payload.get("function_module")
        function_name = payload.get("function_name")
        args = payload.get("args", [])
        kwargs = payload.get("kwargs", {})
        function_pickle = payload.get("function_pickle")
        
        if not function_pickle and (not function_module or not function_name):
            error_msg = f"Invalid function job payload: missing function_module/function_name or function_pickle"
            logger.error(f"Invalid function job payload for job {job.id}: {error_msg}")
            raise ValueError(error_msg)
        
        try:
            # Get the function
            if function_pickle:
                # Use pickled function (for closures, lambdas, etc.)
                func = pickle.loads(base64.b64decode(function_pickle))
                func_display = f"<pickled function>"
            else:
                # Import and get function from module
                module = importlib.import_module(function_module)
                func = getattr(module, function_name)
                func_display = f"{function_module}.{function_name}"
            
            # Execute the function
            import time
            
            logger.info(
                f"Executing function {func_display} for job {job.id} "
                f"with args={args}, kwargs={kwargs}"
            )
            
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            logger.info(
                f"Function {func_display} completed successfully for job {job.id}"
            )
            
            # Store result with metadata if it's JSON-serializable
            try:
                import json
                json.dumps(result)  # Test if serializable
                # Store result with metadata
                job.result = {
                    "success": True,
                    "function": func_display,
                    "result": result,
                    "execution_time_seconds": round(execution_time, 3),
                    "timestamp": timezone.now().isoformat()
                }
                return True
            except (TypeError, ValueError):
                # Result is not JSON-serializable, but function succeeded
                logger.warning(
                    f"Function result for job {job.id} is not JSON-serializable, "
                    f"but execution succeeded. Result type: {type(result)}"
                )
                # Store metadata with result type information
                try:
                    job.result = {
                        "success": True,
                        "function": func_display,
                        "message": "Function completed successfully",
                        "result_type": str(type(result).__name__),
                        "result_repr": str(result)[:500] if result is not None else None,  # Truncate long strings
                        "execution_time_seconds": round(execution_time, 3),
                        "timestamp": timezone.now().isoformat()
                    }
                except Exception as e:
                    # Fallback if even string conversion fails
                    job.result = {
                        "success": True,
                        "function": func_display,
                        "message": "Function completed but result could not be serialized",
                        "execution_time_seconds": round(execution_time, 3),
                        "timestamp": timezone.now().isoformat()
                    }
                return True
                
        except ImportError as e:
            error_msg = f"Failed to import module '{function_module}': {str(e)}"
            logger.error(f"Function execution failed for job {job.id}: {error_msg}")
            raise Exception(error_msg) from e
        except AttributeError as e:
            error_msg = f"Function '{function_name}' not found in module '{function_module}': {str(e)}"
            logger.error(f"Function execution failed for job {job.id}: {error_msg}")
            raise Exception(error_msg) from e
        except Exception as e:
            error_msg = f"Function execution error: {str(e)}"
            logger.error(
                f"Function execution failed for job {job.id}: {error_msg}",
                exc_info=True
            )
            raise Exception(error_msg) from e
    
    def get_retry_delay(self, attempt: int) -> int:
        """
        Exponential backoff: 5s, 30s, 120s
        """
        delays = [5, 30, 120]
        return delays[min(attempt - 1, len(delays) - 1)]
    
    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        """Validate function job payload"""
        if "function_module" not in payload or "function_name" not in payload:
            logger.error("Missing required fields 'function_module' or 'function_name' in function job payload")
            return False
        return True


class LeadScoringJobHandler(JobHandler):
    """
    Handler for bulk lead scoring jobs.
    Processes all leads for a tenant and applies scoring rules.
    """
    
    def process(self, job: BackgroundJob) -> bool:
        """
        Process a lead scoring job.
        
        Expected payload:
        {
            "entity_type": "lead",  # Optional, defaults to 'lead'
            "batch_size": 100       # Optional, defaults to 100
        }
        """
        from django.db import transaction
        from crm_records.models import Record
        from crm_records.scoring import calculate_and_update_lead_score
        
        payload = job.payload
        entity_type = payload.get("entity_type", "lead")
        batch_size = payload.get("batch_size", 100)
        tenant_id = job.tenant_id
        
        if not tenant_id:
            error_msg = "Lead scoring job requires tenant_id"
            logger.error(f"Invalid lead scoring job {job.id}: {error_msg}")
            raise ValueError(error_msg)
        
        logger.info(
            f"Starting lead scoring job {job.id} for tenant {tenant_id}, "
            f"entity_type={entity_type}, batch_size={batch_size}"
        )
        
        # Get all leads for this tenant
        leads = Record.objects.filter(
            tenant_id=tenant_id,
            entity_type=entity_type
        )
        
        total_leads = leads.count()
        updated_count = 0
        total_score_added = 0.0
        processed_count = 0
        
        # Update job result with initial progress
        job.result = {
            "total_leads": total_leads,
            "processed_leads": 0,
            "updated_leads": 0,
            "total_score_added": 0.0,
            "progress_percentage": 0,
            "status": "processing"
        }
        job.save(update_fields=['result'])
        
        # Process leads in batches
        for i in range(0, total_leads, batch_size):
            batch = leads[i:i + batch_size]
            
            with transaction.atomic():
                for lead in batch:
                    try:
                        # Use the utility function to calculate and update score
                        score = calculate_and_update_lead_score(
                            lead,
                            tenant_id=tenant_id,
                            save=True
                        )
                        
                        processed_count += 1
                        
                        if score > 0:
                            updated_count += 1
                            total_score_added += score
                        
                        # Update job result every batch
                        if processed_count % batch_size == 0:
                            progress = int((processed_count / total_leads) * 100) if total_leads > 0 else 0
                            job.result = {
                                "total_leads": total_leads,
                                "processed_leads": processed_count,
                                "updated_leads": updated_count,
                                "total_score_added": total_score_added,
                                "progress_percentage": progress,
                                "status": "processing"
                            }
                            job.save(update_fields=['result'])
                            logger.debug(
                                f"Lead scoring job {job.id} progress: "
                                f"{processed_count}/{total_leads} ({progress}%)"
                            )
                    
                    except Exception as e:
                        logger.error(f"Error scoring lead {lead.id} in job {job.id}: {e}")
                        # Continue with next lead
                        processed_count += 1
                        continue
        
        # Final update
        job.result = {
            "total_leads": total_leads,
            "processed_leads": processed_count,
            "updated_leads": updated_count,
            "total_score_added": total_score_added,
            "progress_percentage": 100,
            "status": "completed"
        }
        job.save(update_fields=['result'])
        
        logger.info(
            f"Lead scoring job {job.id} completed: {updated_count}/{total_leads} leads updated, "
            f"total score: {total_score_added}"
        )
        
        return True
    
    def get_retry_delay(self, attempt: int) -> int:
        """
        Exponential backoff: 10s, 60s, 300s (5 minutes)
        Lead scoring is a heavy operation, so longer delays
        """
        delays = [10, 60, 300]
        return delays[min(attempt - 1, len(delays) - 1)]
    
    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        """Validate lead scoring job payload"""
        # entity_type and batch_size are optional with defaults
        return True


class PrajaJobHandler(JobHandler):
    """
    Handler for sending data to Praja server.
    """
    
    def process(self, job: BackgroundJob) -> bool:
        """
        Process a Praja job.
        
        Expected payload:
        {
            "object_type": "record" or "ticket",
            "object_id": int,
            "data": dict (full object data to send)
        }
        """
        from crm_records.services import PrajaService
        
        payload = job.payload
        object_type = payload.get("object_type")
        object_id = payload.get("object_id")
        data = payload.get("data", {})
        
        if not object_type or not object_id:
            error_msg = f"Invalid Praja job payload: missing object_type or object_id"
            logger.error(f"Invalid Praja job payload for job {job.id}: {error_msg}")
            raise ValueError(error_msg)
        
        try:
            import time
            start_time = time.time()
            
            print(f"\n🚀 [PRAJA JOB] Processing job {job.id} for {object_type} {object_id}...")
            
            praja_service = PrajaService()
            
            if object_type == "record":
                # For records, we need to reconstruct the record object
                from crm_records.models import Record
                try:
                    record = Record.objects.get(id=object_id)
                    print(f"📋 [PRAJA JOB] Found record {object_id}, sending to Praja server...")
                    success = praja_service.send_record_to_praja(record)
                except Record.DoesNotExist:
                    error_msg = f"Record {object_id} not found"
                    print(f"❌ [PRAJA JOB] {error_msg}")
                    logger.error(f"Praja job {job.id} failed: {error_msg}")
                    raise Exception(error_msg)
            elif object_type == "ticket":
                # For tickets, we need to reconstruct the ticket object
                from support_ticket.models import SupportTicket
                try:
                    ticket = SupportTicket.objects.get(id=object_id)
                    print(f"📋 [PRAJA JOB] Found ticket {object_id}, sending to Praja server...")
                    success = praja_service.send_ticket_to_praja(ticket)
                except SupportTicket.DoesNotExist:
                    error_msg = f"SupportTicket {object_id} not found"
                    print(f"❌ [PRAJA JOB] {error_msg}")
                    logger.error(f"Praja job {job.id} failed: {error_msg}")
                    raise Exception(error_msg)
            else:
                error_msg = f"Invalid object_type: {object_type}. Must be 'record' or 'ticket'"
                print(f"❌ [PRAJA JOB] {error_msg}")
                logger.error(f"Praja job {job.id} failed: {error_msg}")
                raise ValueError(error_msg)
            
            if not success:
                error_msg = f"Praja service returned False for {object_type} {object_id}"
                print(f"❌ [PRAJA JOB] {error_msg}")
                logger.error(f"Praja job {job.id} failed: {error_msg}")
                raise Exception(error_msg)
            
            execution_time = time.time() - start_time
            
            # Store result with debugging information
            job.result = {
                "success": True,
                "object_type": object_type,
                "object_id": object_id,
                "execution_time_seconds": round(execution_time, 3),
                "timestamp": timezone.now().isoformat()
            }
            
            print(f"✅ [PRAJA JOB] Job {job.id} completed successfully in {round(execution_time, 3)}s")
            logger.info(
                f"Praja data sent successfully for job {job.id}: "
                f"object_type={object_type} object_id={object_id}"
            )
            return True
            
        except Exception as e:
            error_msg = f"Praja request failed for {object_type} {object_id}: {str(e)}"
            print(f"❌ [PRAJA JOB] Job {job.id} failed: {error_msg}")
            logger.error(f"Praja job {job.id} failed: {error_msg}")
            raise Exception(error_msg) from e
    
    def get_retry_delay(self, attempt: int) -> int:
        """
        Exponential backoff: 5s, 30s, 180s
        """
        delays = [5, 30, 180]
        return delays[min(attempt - 1, len(delays) - 1)]
    
    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        """Validate Praja job payload"""
        if "object_type" not in payload:
            logger.error("Missing required field 'object_type' in Praja job payload")
            return False
        if "object_id" not in payload:
            logger.error("Missing required field 'object_id' in Praja job payload")
            return False
        if payload.get("object_type") not in ["record", "ticket"]:
            logger.error(f"Invalid object_type: {payload.get('object_type')}. Must be 'record' or 'ticket'")
            return False
        return True


class JobHandlerRegistry:
    """
    Registry for job handlers.
    Maps job types to their handler instances.
    """
    
    def __init__(self):
        self._handlers: Dict[str, JobHandler] = {}
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default handlers"""
        from .models import JobType
        
        self.register_handler(JobType.SEND_MIXPANEL_EVENT, MixpanelJobHandler())
        self.register_handler(JobType.SEND_WEBHOOK, WebhookJobHandler())
        self.register_handler(JobType.EXECUTE_FUNCTION, FunctionJobHandler())
        self.register_handler(JobType.SCORE_LEADS, LeadScoringJobHandler())
        # Praja handler removed - now using MixpanelService instead
        # self.register_handler(JobType.SEND_TO_PRAJA, PrajaJobHandler())
    
    def register_handler(self, job_type: str, handler: JobHandler):
        """
        Register a handler for a job type.
        
        Args:
            job_type: The job type string
            handler: The handler instance
        """
        self._handlers[job_type] = handler
        logger.info(f"Registered handler for job type: {job_type}")
    
    def get_handler(self, job_type: str) -> JobHandler:
        """
        Get the handler for a job type.
        
        Args:
            job_type: The job type string
            
        Returns:
            The handler instance
            
        Raises:
            KeyError: If no handler is registered for the job type
        """
        if job_type not in self._handlers:
            raise KeyError(f"No handler registered for job type: {job_type}")
        return self._handlers[job_type]
    
    def has_handler(self, job_type: str) -> bool:
        """Check if a handler exists for a job type"""
        return job_type in self._handlers


# Global registry instance
_handler_registry = JobHandlerRegistry()


def get_handler_registry() -> JobHandlerRegistry:
    """Get the global handler registry"""
    return _handler_registry


