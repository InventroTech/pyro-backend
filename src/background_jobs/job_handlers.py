"""
Job Handler Plugin System

This module provides the plugin-based handler system for processing different types
of background jobs. Each job type has its own handler that implements the JobHandler interface.
"""
import base64
import importlib
import json
import logging
import os
import pickle
import time
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, Dict, Optional, Tuple, Union
from uuid import UUID

import requests
from django.conf import settings
from django.db import transaction
from django.db.models import DateTimeField, F, Func, JSONField
from django.db.models.expressions import RawSQL
from django.db.utils import OperationalError
from django.utils import timezone

from core.models import Tenant
from authz.models import TenantMembership
from crm_records.lead_assignment_tracking import merge_first_assignment_today_anchor
from crm_records.models import Record, PartnerEvent
from crm_records.scoring import get_scoring_rules, score_chunk_sql
from crm_records.services import PrajaService
from support_ticket.models import SupportTicket
from support_ticket.services import MixpanelService, RMAssignedMixpanelService

from .models import BackgroundJob, JobType

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


class RMAssignedMixpanelJobHandler(JobHandler):
    """
    Handler for sending RM assigned events to Mixpanel via rm_assigned endpoint.
    """

    def process(self, job: BackgroundJob) -> bool:
        """
        Process an RM assigned Mixpanel job.

        Expected payload:
        {
            "praja_id": int,
            "rm_email": str
        }
        """
        payload = job.payload
        praja_id = payload.get("praja_id")
        rm_email = payload.get("rm_email")

        if praja_id is None or not rm_email:
            error_msg = (
                f"Invalid RM assigned job payload: missing praja_id or rm_email "
                f"(praja_id={praja_id}, rm_email={bool(rm_email)})"
            )
            logger.error(f"Invalid RM assigned job payload for job {job.id}: {error_msg}")
            raise ValueError(error_msg)

        try:
            start_time = time.time()
            praja_id_int = int(praja_id)
            service = RMAssignedMixpanelService()
            success = service.send_to_mixpanel_sync(praja_id_int, rm_email)
            execution_time = time.time() - start_time

            if success:
                job.result = {
                    "success": True,
                    "praja_id": praja_id_int,
                    "rm_email": rm_email,
                    "execution_time_seconds": round(execution_time, 3),
                    "timestamp": timezone.now().isoformat(),
                }
                logger.info(
                    f"RM assigned event sent successfully for job {job.id}: "
                    f"praja_id={praja_id_int} rm_email={rm_email}"
                )
                return True
            else:
                job.result = {
                    "success": False,
                    "praja_id": praja_id_int,
                    "error": "RMAssignedMixpanelService returned False",
                    "timestamp": timezone.now().isoformat(),
                }
                logger.warning(f"RM assigned event returned False for job {job.id}")
                raise Exception("RMAssignedMixpanelService returned False")
        except Exception as e:
            logger.error(
                f"RM assigned event failed for job {job.id}: praja_id={praja_id} error={e}",
                exc_info=True,
            )
            raise

    def get_retry_delay(self, attempt: int) -> int:
        delays = [1, 10, 60]
        return delays[min(attempt - 1, len(delays) - 1)]

    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        required = ["praja_id", "rm_email"]
        for field in required:
            if field not in payload:
                logger.error(f"Missing required field '{field}' in RM assigned job payload")
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
        payload = job.payload
        url = payload.get("url")
        webhook_payload = payload.get("payload", {})
        
        if not url:
            error_msg = f"Invalid webhook job payload: missing URL"
            logger.error(f"Invalid webhook job payload for job {job.id}: {error_msg}")
            raise ValueError(error_msg)
        
        try:
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
        payload = job.payload or {}
        entity_type = payload.get("entity_type", "lead")
        tenant_id = job.tenant_id
        chunk_size = 10_000
        
        if not tenant_id:
            error_msg = "Lead scoring job requires tenant_id"
            logger.error(f"Invalid lead scoring job {job.id}: {error_msg}")
            raise ValueError(error_msg)
        
        logger.info(
            "Starting lead scoring job %s for tenant=%s entity_type=%s chunk_size=%s",
            job.id,
            tenant_id,
            entity_type,
            chunk_size,
        )
        
        job.result = {
            "total_leads": 0,
            "processed_leads": 0,  # filled by aggregating completed chunk jobs
            "updated_leads": 0,  # filled by aggregating completed chunk jobs
            "total_score_added": 0.0,  # filled by aggregating completed chunk jobs
            "chunk_job_ids": [],
            "total_chunks": 0,
            "progress_percentage": 0,
            "status": "processing",
        }
        job.save(update_fields=["result"])

        # Fetch scoring rules once (fan-out time), then reuse for all chunk jobs.
        rules = get_scoring_rules(entity_type, str(tenant_id))

        from django.db.models import Min, Max
        from .queue_service import get_queue_service
        from crm_records.models import Record

        # Exclude final stages that should never be rescored.
        qs = Record.objects.filter(tenant_id=tenant_id, entity_type=entity_type).extra(
            where=[
                "UPPER(COALESCE(data->>'lead_stage','')) NOT IN ('CLOSED','TRIAL_ACTIVATED','NOT_INTERESTED')"
            ]
        )
        total_leads = qs.count()

        if total_leads == 0:
            job.result = {
                "total_leads": 0,
                "processed_leads": 0,
                "updated_leads": 0,
                "total_score_added": 0.0,
                "chunk_job_ids": [],
                "total_chunks": 0,
                "progress_percentage": 100,
                "status": "completed",
            }
            job.save(update_fields=["result"])
            return True

        agg = qs.aggregate(min_id=Min("id"), max_id=Max("id"))
        min_id = agg.get("min_id")
        max_id = agg.get("max_id")

        # Defensive: should not be None because total_leads > 0.
        if min_id is None or max_id is None:
            job.result = {
                "total_leads": total_leads,
                "processed_leads": 0,
                "updated_leads": 0,
                "total_score_added": 0.0,
                "chunk_job_ids": [],
                "total_chunks": 0,
                "progress_percentage": 100,
                "status": "completed",
                "error": "could not compute id range",
            }
            job.save(update_fields=["result"])
            return True

        queue_service = get_queue_service()

        chunk_job_ids = []
        ranges = list(range(int(min_id), int(max_id) + 1, chunk_size))
        total_chunks = len(ranges)

        for idx, start_id in enumerate(ranges):
            end_id = min(start_id + chunk_size, int(max_id) + 1)

            chunk_job = queue_service.enqueue_job(
                job_type=JobType.SCORE_LEADS_CHUNK,
                payload={
                    "entity_type": entity_type,
                    "id_gte": int(start_id),
                    "id_lt": int(end_id),
                    "rules": rules,
                    "parent_job_id": job.id,
                    "chunk_index": idx,
                    "total_chunks": total_chunks,
                },
                priority=0,
                tenant_id=str(tenant_id),
            )
            chunk_job_ids.append(chunk_job.id)

        job.result = {
            "total_leads": int(total_leads),
            "processed_leads": 0,
            "updated_leads": 0,
            "total_score_added": 0.0,
            "chunk_job_ids": chunk_job_ids,
            "total_chunks": total_chunks,
            "progress_percentage": 0,
            "status": "completed",
        }
        job.save(update_fields=["result"])

        logger.info(
            f"Lead scoring job {job.id} completed: "
            "enqueued %s chunk jobs (tenant=%s, entity_type=%s)",
            total_chunks,
            tenant_id,
            entity_type,
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
        # entity_type and batch_size are optional with defaults
        return True


class LeadScoringChunkJobHandler(JobHandler):
    """
    Handler for scoring a single chunk of lead records (by ID range).
    Runs a set-based SQL UPDATE to compute data.lead_score for all rows in the chunk.
    """

    def process(self, job: BackgroundJob) -> bool:
        payload = job.payload or {}
        tenant_id = job.tenant_id

        if not tenant_id:
            error_msg = "Lead scoring chunk job requires tenant_id"
            logger.error("Invalid lead scoring chunk job %s: %s", job.id, error_msg)
            raise ValueError(error_msg)

        entity_type = payload.get("entity_type", "lead")
        id_gte = payload.get("id_gte")
        id_lt = payload.get("id_lt")
        rules = payload.get("rules", []) or []

        if id_gte is None or id_lt is None:
            raise ValueError(f"Lead scoring chunk job {job.id} requires id_gte and id_lt")

        result = score_chunk_sql(
            tenant_id=str(tenant_id),
            entity_type=entity_type,
            rules=rules,
            id_gte=int(id_gte),
            id_lt=int(id_lt),
        )

        job.result = {
            **result,
            "parent_job_id": payload.get("parent_job_id"),
            "chunk_index": payload.get("chunk_index"),
            "total_chunks": payload.get("total_chunks"),
        }
        job.save(update_fields=["result"])

        return True

    def get_retry_delay(self, attempt: int) -> int:
        # Chunk scoring is still DB-heavy, but retries can be shorter than full fan-out.
        delays = [5, 30, 180]
        return delays[min(attempt - 1, len(delays) - 1)]

    def validate_payload(self, payload: Dict[str, Any]) -> bool:
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
        payload = job.payload
        object_type = payload.get("object_type")
        object_id = payload.get("object_id")
        data = payload.get("data", {})
        
        if not object_type or not object_id:
            error_msg = f"Invalid Praja job payload: missing object_type or object_id"
            logger.error(f"Invalid Praja job payload for job {job.id}: {error_msg}")
            raise ValueError(error_msg)
        
        try:
            start_time = time.time()
            
            print(f"\n🚀 [PRAJA JOB] Processing job {job.id} for {object_type} {object_id}...")
            
            praja_service = PrajaService()
            
            if object_type == "record":
                # For records, we need to reconstruct the record object
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


class PartnerLeadAssignJobHandler(JobHandler):
    """
    Handler for partner-initiated lead assignment (e.g. Halocom work_on_lead).
    Assigns the given record to the user identified by email_id in the tenant.
    Updates PartnerEvent status for audit trail.
    """
    def _update_partner_event(self, partner_event_id: int, status: str, error_message: str = None):
        try:
            PartnerEvent.objects.filter(pk=partner_event_id).update(
                status=status,
                processed_at=timezone.now(),
                error_message=error_message,
                updated_at=timezone.now(),
            )
        except Exception as e:
            logger.warning("[PartnerLeadAssign] Failed to update PartnerEvent %s: %s", partner_event_id, e)

    def process(self, job: BackgroundJob) -> bool:
        payload = job.payload
        tenant_id = payload.get("tenant_id")
        email_id = (payload.get("email_id") or "").strip().lower()
        partner_slug = (payload.get("partner_slug") or "halocom").strip().lower()
        record_id = payload.get("record_id")
        partner_event_id = payload.get("partner_event_id")

        if not tenant_id or not email_id or not record_id:
            error_msg = (
                f"Invalid partner lead assign payload: missing tenant_id, email_id or record_id "
                f"(tenant_id={tenant_id}, email_id={bool(email_id)}, record_id={record_id})"
            )
            logger.error(f"Partner lead assign job {job.id}: {error_msg}")
            if partner_event_id:
                self._update_partner_event(partner_event_id, "failed", error_msg)
            raise ValueError(error_msg)

        try:
            tenant = Tenant.objects.get(id=tenant_id)
        except (Tenant.DoesNotExist, ValueError, TypeError) as e:
            logger.error(f"Partner lead assign job {job.id}: tenant not found: {e}")
            if partner_event_id:
                self._update_partner_event(partner_event_id, "failed", str(e))
            raise ValueError(f"Tenant not found: {tenant_id}") from e

        membership = TenantMembership.objects.filter(
            tenant=tenant,
            email__iexact=email_id,
            is_active=True
        ).first()
        if not membership:
            error_msg = f"No active tenant membership for email {email_id}"
            logger.error(f"Partner lead assign job {job.id}: {error_msg}")
            if partner_event_id:
                self._update_partner_event(partner_event_id, "failed", error_msg)
            raise ValueError(error_msg)

        user_identifier = str(membership.user_id) if membership.user_id else membership.email

        record = Record.objects.filter(
            tenant=tenant,
            entity_type="lead",
            pk=record_id
        ).first()
        if not record:
            error_msg = f"Record {record_id} not found for tenant"
            logger.error(f"Partner lead assign job {job.id}: {error_msg}")
            if partner_event_id:
                self._update_partner_event(partner_event_id, "failed", error_msg)
            raise ValueError(error_msg)

        try:
            with transaction.atomic():
                data = (record.data or {}).copy() if isinstance(record.data, dict) else {}
                previous_assigned_to = data.get("assigned_to")
                is_fresh_assignment = (
                    previous_assigned_to is None
                    or previous_assigned_to == ""
                    or previous_assigned_to == "null"
                    or previous_assigned_to == "None"
                )
                data["assigned_to"] = user_identifier
                data["lead_stage"] = "ASSIGNED"
                data["partner_source"] = partner_slug
                if "call_attempts" not in data or data.get("call_attempts") in (None, "", "null"):
                    data["call_attempts"] = 0
                call_attempts = data.get("call_attempts", 0)
                try:
                    call_attempts_int = int(call_attempts) if call_attempts is not None else 0
                except (TypeError, ValueError):
                    call_attempts_int = 0
                last_call_outcome = (data.get("last_call_outcome") or "").lower()
                lead_stage = (data.get("lead_stage") or "").upper()
                # last_call_outcome in DB is exactly "not_connected"
                is_not_connected_retry = (
                    call_attempts_int > 0
                    or last_call_outcome == "not_connected"
                    or lead_stage == "NOT_CONNECTED"
                )
                if is_fresh_assignment and "first_assigned_at" not in data and not is_not_connected_retry:
                    data["first_assigned_at"] = timezone.now().isoformat()
                    data["first_assigned_to"] = user_identifier
                if is_fresh_assignment:
                    merge_first_assignment_today_anchor(data, timezone.now())
                record.data = data
                record.updated_at = timezone.now()
                record.save(update_fields=["data", "updated_at"])
        except Exception as e:
            if partner_event_id:
                self._update_partner_event(partner_event_id, "failed", str(e))
            raise

        if partner_event_id:
            self._update_partner_event(partner_event_id, "completed")

        logger.info(
            "[PartnerLeadAssign] Assigned record_id=%s to %s partner_slug=%s",
            record_id, user_identifier, partner_slug
        )
        job.result = {
            "success": True,
            "record_id": record_id,
            "assigned_to": user_identifier,
            "partner_slug": partner_slug,
            "timestamp": timezone.now().isoformat(),
        }
        return True

    def get_retry_delay(self, attempt: int) -> int:
        delays = [2, 10, 60]
        return delays[min(attempt - 1, len(delays) - 1)]

    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        for key in ("tenant_id", "email_id", "record_id"):
            if not payload.get(key):
                logger.error(f"Missing required field '{key}' in partner lead assign payload")
                return False
        return True


class UnassignSnoozedLeadsJobHandler(JobHandler):
    """
    Job for SNOOZED leads only. Unassigns when snooze_unassign_at has passed:
    - 48 hours if time was set (call-back-later with time), 12 hours if not.
    Triggers only when: (1) lead has assigned_to, (2) no other event was clicked in between
    (lead_stage still SNOOZED), (3) not all attempts done (call_attempts < 6).
    """

    def process(self, job: BackgroundJob) -> bool:
        from datetime import timedelta

        now = timezone.now()
        one_hour_later = (now + timedelta(hours=1)).isoformat()
        # SNOOZED only = no other event clicked; has assigned_to; snooze_unassign_at passed; attempts not exhausted
        qs = Record.objects.filter(entity_type="lead").extra(
            where=[
                "data->>'lead_stage' = 'SNOOZED'",
                "data->>'assigned_to' IS NOT NULL",
                "TRIM(COALESCE(data->>'assigned_to', '')) != ''",
                "data->>'snooze_unassign_at' IS NOT NULL",
                "data->>'snooze_unassign_at' != ''",
                "(data->>'snooze_unassign_at')::timestamptz <= %s",
                "COALESCE((data->>'call_attempts')::int, 0) < 6",
            ],
            params=[now],
        )
        unassigned_count = 0
        for record in qs:
            data = (record.data or {}).copy() if isinstance(record.data, dict) else {}
            if not data.get("assigned_to"):
                continue
            data["assigned_to"] = None
            data.pop("snooze_unassign_at", None)
            data["next_call_at"] = one_hour_later
            # Do not change call_attempts on unassign
            record.data = data
            record.save(update_fields=["data", "updated_at"])
            unassigned_count += 1
            logger.info(
                "[UnassignSnoozedLeads] Unassigned lead record_id=%s (snooze_unassign_at passed)",
                record.id,
            )
        job.result = {
            "success": True,
            "unassigned_count": unassigned_count,
            "timestamp": timezone.now().isoformat(),
        }
        logger.info("[UnassignSnoozedLeads] Completed: unassigned_count=%s", unassigned_count)
        return True

    def get_retry_delay(self, attempt: int) -> int:
        delays = [60, 300, 900]
        return delays[min(attempt - 1, len(delays) - 1)]


# Same CASE body as GetNextLeadView subscription_time_stamp sort SQL.
_CLOSE_STALE_SUBSCRIPTION_TS_CASE = """
CASE
    WHEN (data->>'subscription_time_stamp') IS NOT NULL
        AND TRIM(COALESCE(data->>'subscription_time_stamp', '')) != ''
        AND LOWER(TRIM(COALESCE(data->>'subscription_time_stamp', ''))) NOT IN ('null', 'none')
    THEN (data->>'subscription_time_stamp')::timestamptz
    ELSE NULL
END
"""


class JsonbSetLeadStageClosed(Func):
    """PostgreSQL jsonb_set: set ``data.lead_stage`` to CLOSED."""

    function = "jsonb_set"
    template = (
        "jsonb_set(COALESCE(%(expressions)s, '{}'::jsonb), '{lead_stage}', "
        "to_jsonb('CLOSED'::text), true)"
    )
    output_field = JSONField()


def _close_stale_subscription_leads_queryset(
    tenant_id: Optional[Union[str, UUID]] = None,
):
    """
    Base queryset: SELF TRIAL leads, not CLOSED, with ``subscription_ts`` from
    ``subscription_time_stamp`` (same rules as the CASE SQL).
    """
    qs = (
        Record.objects.annotate(
            subscription_ts=RawSQL(
                f"({_CLOSE_STALE_SUBSCRIPTION_TS_CASE.strip()})",
                [],
                output_field=DateTimeField(),
            )
        )
        .filter(entity_type="lead", data__lead_status="SELF TRIAL")
        .exclude(data__lead_stage__iexact="CLOSED")
    )
    if tenant_id is not None:
        tid = tenant_id if isinstance(tenant_id, UUID) else UUID(str(tenant_id))
        qs = qs.filter(tenant_id=tid)
    return qs


def _close_stale_subscription_leads_for_tenant(
    tenant_id: Union[str, UUID], days: int
) -> Tuple[int, str]:
    if days < 1:
        raise ValueError("days must be >= 1")
    cutoff = timezone.now() - timedelta(days=days)
    now = timezone.now()
    tid = tenant_id if isinstance(tenant_id, UUID) else UUID(str(tenant_id))
    updated = (
        _close_stale_subscription_leads_queryset(tid)
        .filter(subscription_ts__lte=cutoff)
        .update(data=JsonbSetLeadStageClosed(F("data")), updated_at=now)
    )
    return updated, cutoff.isoformat()


def _close_stale_subscription_leads_all_tenants(days: int) -> Tuple[int, int, str]:
    if days < 1:
        raise ValueError("days must be >= 1")
    cutoff = timezone.now() - timedelta(days=days)
    now = timezone.now()
    n_tenants = Tenant.objects.count()
    total = (
        _close_stale_subscription_leads_queryset(None)
        .filter(subscription_ts__lte=cutoff)
        .update(data=JsonbSetLeadStageClosed(F("data")), updated_at=now)
    )
    return total, n_tenants, cutoff.isoformat()


class CloseStaleSubscriptionLeadsJobHandler(JobHandler):
    """
    Set ``data.lead_stage`` to CLOSED for **SELF TRIAL** leads (``lead_status``) whose
    ``subscription_time_stamp`` is at least ``days`` (default 15) in the past.

    Payload:
    - ``days`` (int, optional, default 15): minimum age of subscription timestamp in days.
    - ``tenant_id`` (str UUID, optional): if set, only that tenant; if omitted, all tenants
      (periodic worker enqueue uses no tenant_id).
    """

    def process(self, job: BackgroundJob) -> bool:
        payload = job.payload or {}
        days = int(payload.get("days") or 15)
        if days < 1:
            raise ValueError("days must be >= 1")

        tid = payload.get("tenant_id")
        if tid:
            updated, cutoff = _close_stale_subscription_leads_for_tenant(tid, days)
            job.result = {
                "success": True,
                "updated": updated,
                "days": days,
                "cutoff": cutoff,
                "tenant_id": str(tid),
                "tenant_scope": "single",
            }
            logger.info(
                "[CloseStaleSubscriptionLeads] tenant=%s updated=%s days=%s",
                tid,
                updated,
                days,
            )
        else:
            total, n_tenants, cutoff = _close_stale_subscription_leads_all_tenants(days)
            job.result = {
                "success": True,
                "updated": total,
                "days": days,
                "cutoff": cutoff,
                "tenants_processed": n_tenants,
                "tenant_scope": "all",
            }
            logger.info(
                "[CloseStaleSubscriptionLeads] all tenants updated=%s tenants=%s days=%s",
                total,
                n_tenants,
                days,
            )
        return True

    def get_retry_delay(self, attempt: int) -> int:
        delays = [60, 300, 900]
        return delays[min(attempt - 1, len(delays) - 1)]

    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        try:
            days = int((payload or {}).get("days") or 15)
        except (TypeError, ValueError):
            return False
        return days >= 1


class SnoozedToNotConnectedMidnightJobHandler(JobHandler):
    """
    For **SALES LEAD** leads in **SNOOZED**: set ``lead_stage`` to NOT_CONNECTED and apply the
    same unassignment as ``UnassignSnoozedLeadsJobHandler`` — clear ``assigned_to``, drop
    ``snooze_unassign_at`` (call_attempts unchanged). Does not modify ``next_call_at``.

    Only considers leads whose callback time ``next_call_at`` falls on **today's calendar date**
    in ``SNOOZED_RESET_TIMEZONE`` (same wall-clock as the midnight enqueue). Leads snoozed until a
    **future** calendar day (e.g. tomorrow) are left unchanged.

    Runs once per local calendar day at 00:00 (see job processor + ``SNOOZED_RESET_TIMEZONE``).
    """

    def process(self, job: BackgroundJob) -> bool:
        tz_name = getattr(settings, "SNOOZED_RESET_TIMEZONE", settings.TIME_ZONE)
        qs = Record.objects.filter(
            entity_type="lead",
            data__lead_stage="SNOOZED",
            data__lead_status="SALES LEAD",
        ).extra(
            where=[
                """
                (data->>'next_call_at') IS NOT NULL
                AND TRIM(COALESCE(data->>'next_call_at', '')) != ''
                AND LOWER(TRIM(COALESCE(data->>'next_call_at', ''))) NOT IN ('null', 'none')
                AND (timezone(%s, (data->>'next_call_at')::timestamptz))::date
                    = (timezone(%s, NOW()))::date
                """,
            ],
            params=[tz_name, tz_name],
        )
        updated = 0
        for record in qs.iterator(chunk_size=500):
            data = (record.data or {}).copy() if isinstance(record.data, dict) else {}
            if data.get("lead_stage") != "SNOOZED" or data.get("lead_status") != "SALES LEAD":
                continue
            data["lead_stage"] = "NOT_CONNECTED"
            data["assigned_to"] = None
            data.pop("snooze_unassign_at", None)
            record.data = data
            record.save(update_fields=["data", "updated_at"])
            updated += 1
            logger.debug(
                "[SnoozedToNotConnectedMidnight] record_id=%s: NOT_CONNECTED + unassigned (SALES LEAD)",
                record.id,
            )
        job.result = {
            "success": True,
            "updated": updated,
            "timestamp": timezone.now().isoformat(),
        }
        logger.info("[SnoozedToNotConnectedMidnight] completed updated=%s rows", updated)
        return True

    def get_retry_delay(self, attempt: int) -> int:
        delays = [60, 300, 900]
        return delays[min(attempt - 1, len(delays) - 1)]


class ReleaseLeadsAfter12hJobHandler(JobHandler):
    """
    Job for NOT_CONNECTED leads only (segregation bucket).

    Business rule: "Has it been 12 hours since the lead was assigned (this stint)?"
    We use ``data.first_assigned_today_at`` set on every unassigned→assigned transition.
    When ``first_assigned_today_at + 12 hours <= now``, clear ``assigned_to`` only; keep
    ``lead_stage`` = NOT_CONNECTED.

    Legacy: rows with no ``first_assigned_today_at`` still honor ``not_connected_unassign_at``
    if present (pre-migration data).

    Does not remove ``first_assigned_at`` / ``first_assigned_to`` (daily limit tracking).
    """

    def process(self, job: BackgroundJob) -> bool:
        from datetime import timedelta

        now = timezone.now()
        one_hour_later = (now + timedelta(hours=1)).isoformat()
        qs = Record.objects.filter(entity_type="lead").extra(
            where=[
                "UPPER(COALESCE(data->>'lead_stage','')) = 'NOT_CONNECTED'",
                "data->>'assigned_to' IS NOT NULL",
                "TRIM(COALESCE(data->>'assigned_to', '')) != ''",
                "COALESCE((data->>'call_attempts')::int, 0) < 6",
                """
                (
                    (
                        data->>'first_assigned_today_at' IS NOT NULL
                        AND TRIM(COALESCE(data->>'first_assigned_today_at', '')) != ''
                        AND (data->>'first_assigned_today_at')::timestamptz + interval '12 hours' <= %s
                    )
                    OR (
                        (
                            data->>'first_assigned_today_at' IS NULL
                            OR TRIM(COALESCE(data->>'first_assigned_today_at', '')) = ''
                        )
                        AND data->>'not_connected_unassign_at' IS NOT NULL
                        AND TRIM(COALESCE(data->>'not_connected_unassign_at', '')) != ''
                        AND (data->>'not_connected_unassign_at')::timestamptz <= %s
                    )
                )
                """,
            ],
            params=[now, now],
        )
        released_count = 0
        for record in qs:
            data = (record.data or {}).copy() if isinstance(record.data, dict) else {}
            # Only clear assigned_to; do not change lead_stage (stays NOT_CONNECTED).
            data["assigned_to"] = None
            data["next_call_at"] = one_hour_later
            data.pop("not_connected_unassign_at", None)
            data.pop("first_assigned_today_at", None)
            data.pop("first_assignment_today_date", None)
            # Do not change call_attempts on unassign; do not remove first_assigned_at or first_assigned_to.
            record.data = data
            record.save(update_fields=["data", "updated_at"])
            released_count += 1
            logger.info(
                "[ReleaseLeadsAfter12h] Released lead record_id=%s (NOT_CONNECTED, 12h since assignment anchor): cleared assigned_to only, stage unchanged",
                record.id,
            )
        job.result = {
            "success": True,
            "released_count": released_count,
            "timestamp": timezone.now().isoformat(),
        }
        logger.info("[ReleaseLeadsAfter12h] Completed: released_count=%s", released_count)
        return True

    def get_retry_delay(self, attempt: int) -> int:
        delays = [60, 300, 900]
        return delays[min(attempt - 1, len(delays) - 1)]


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
        self.register_handler(JobType.SEND_MIXPANEL_EVENT, MixpanelJobHandler())
        self.register_handler(JobType.SEND_RM_ASSIGNED_EVENT, RMAssignedMixpanelJobHandler())
        self.register_handler(JobType.SEND_WEBHOOK, WebhookJobHandler())
        self.register_handler(JobType.EXECUTE_FUNCTION, FunctionJobHandler())
        self.register_handler(JobType.SCORE_LEADS, LeadScoringJobHandler())
        self.register_handler(JobType.SCORE_LEADS_CHUNK, LeadScoringChunkJobHandler())
        self.register_handler(JobType.PARTNER_LEAD_ASSIGN, PartnerLeadAssignJobHandler())
        self.register_handler(JobType.UNASSIGN_SNOOZED_LEADS, UnassignSnoozedLeadsJobHandler())
        self.register_handler(JobType.RELEASE_LEADS_AFTER_12H, ReleaseLeadsAfter12hJobHandler())
        self.register_handler(
            JobType.CLOSE_STALE_SUBSCRIPTION_LEADS, CloseStaleSubscriptionLeadsJobHandler()
        )
        self.register_handler(
            JobType.SNOOZED_TO_NOT_CONNECTED_MIDNIGHT,
            SnoozedToNotConnectedMidnightJobHandler(),
        )
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


