import logging
import subprocess
import os
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import BasePermission, IsAuthenticated

from background_jobs.models import JobType
from background_jobs.queue_service import get_queue_service

logger = logging.getLogger(__name__)


class IsAuthenticatedOrCronSecret(BasePermission):
    """
    Allow request if user is authenticated OR if valid X-Cron-Secret is sent (CRON_SECRET from env).
    Enables automated cron/schedulers to call cron endpoints without a user session.
    """

    def has_permission(self, request, view):
        if IsAuthenticated().has_permission(request, view):
            return True
        cron_secret = os.environ.get("CRON_SECRET")
        if not cron_secret:
            return False
        header_secret = (
            request.headers.get("X-Cron-Secret")
            or request.META.get("HTTP_X_CRON_SECRET", "")
        )
        return header_secret == cron_secret


class CopyScriptView(APIView):
    """
    Class-based view to run Python scripts via API call.
    Similar structure to analytics views.
    """
    permission_classes = [IsAuthenticated]

    def post(self, request):
        """
        Execute a Python script with the provided arguments.
        
        Expected request body:
        {
            "script_path": "script_name.py",
            "script_args": ["--arg1", "value1", "--arg2", "value2"]
        }
        """
        try:
            # Get script path from request
            script_path = request.data.get('script_path')
            script_args = request.data.get('script_args', [])
            
            if not script_path:
                return Response(
                    {'error': 'script_path is required'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Check if script exists
            if not os.path.exists(script_path):
                # Try with scripts directory relative to current working directory
                scripts_path = os.path.join(os.getcwd(), 'scripts', os.path.basename(script_path))
                if os.path.exists(scripts_path):
                    script_path = scripts_path
                else:
                    return Response(
                        {'error': f'Script not found: {script_path}'},
                        status=status.HTTP_404_NOT_FOUND
                    )
            
            # Run the Python script
            logger.info(f"Running Python script: {script_path}")
            
            # Execute the script with proper environment for Unicode and performance
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['PYTHONUTF8'] = '1'
            env['BATCH_SIZE'] = '1'  # Process 1 ticket per batch (one by one)
            
            result = subprocess.run(
                ['python', script_path] + script_args,
                capture_output=True,
                text=True,
                encoding='utf-8',
                env=env,
                timeout=1800  # 30 minutes timeout
            )
            
            # Prepare response
            response_data = {
                'message': 'Script executed successfully',
                'script_path': script_path,
                'return_code': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
            if result.returncode == 0:
                logger.info(f"Script {script_path} executed successfully")
                return Response(response_data, status=status.HTTP_200_OK)
            else:
                logger.error(f"Script {script_path} failed with return code {result.returncode}")
                return Response(response_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
                
        except subprocess.TimeoutExpired:
            logger.error(f"Script {script_path} timed out")
            return Response(
                {'error': 'Script execution timed out'},
                status=status.HTTP_408_REQUEST_TIMEOUT
            )
        except Exception as e:
            logger.error(f"Error running script: {str(e)}")
            return Response(
                {'error': 'Internal server error', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class UnassignSnoozedLeadsCronView(APIView):
    """
    Cron endpoint: enqueue one unassign_snoozed_leads job so the background worker
    unassigns SNOOZED leads whose snooze_unassign_at has passed (48h if time was set, 12h if not).
    Call this periodically (e.g. every 15 min or hourly).
    Allowed: authenticated user OR request with X-Cron-Secret header matching CRON_SECRET env.
    """
    permission_classes = [IsAuthenticatedOrCronSecret]

    def post(self, request):
        try:
            queue = get_queue_service()
            job = queue.enqueue_job(
                job_type=JobType.UNASSIGN_SNOOZED_LEADS,
                payload={},
                priority=0,
            )
            logger.info(f"[Cron] Enqueued unassign_snoozed_leads job id={job.id}")
            return Response(
                {"ok": True, "job_id": job.id, "message": "Unassign snoozed leads job enqueued"},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logger.exception(f"[Cron] Failed to enqueue unassign_snoozed_leads job: {e}")
            return Response(
                {"ok": False, "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class ReleaseLeadsAfter12hCronView(APIView):
    """
    Cron endpoint: enqueue one release_leads_after_12h job.

    NOT_CONNECTED leads still assigned: if ``first_assigned_today_at + 12h`` has passed,
    clear ``assigned_to`` only (stage stays NOT_CONNECTED); ``next_call_at`` set ~1h later.
    Call periodically (e.g. every 15 min or hourly).
    Allowed: authenticated user OR request with X-Cron-Secret header matching CRON_SECRET env.
    """
    permission_classes = [IsAuthenticatedOrCronSecret]

    def post(self, request):
        try:
            queue = get_queue_service()
            job = queue.enqueue_job(
                job_type=JobType.RELEASE_LEADS_AFTER_12H,
                payload={},
                priority=0,
            )
            logger.info(f"[Cron] Enqueued release_leads_after_12h job id={job.id}")
            return Response(
                {"ok": True, "job_id": job.id, "message": "Release leads after 12h job enqueued"},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logger.exception(f"[Cron] Failed to enqueue release_leads_after_12h job: {e}")
            return Response(
                {"ok": False, "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            ) 

class SyncEntitySchemasCronView(APIView):
    """
    Cron endpoint: enqueue one sync_entity_schemas job.
    
    Reads all new records for active tenants and updates the Entity schema 
    snapshots so the system knows what fields exist in the dynamic JSON data.
    Call periodically (e.g. every 10 min or hourly).
    Allowed: authenticated user OR request with X-Cron-Secret header matching CRON_SECRET env.
    """
    permission_classes = [IsAuthenticatedOrCronSecret]

    def post(self, request):
        try:
            queue = get_queue_service()
            job = queue.enqueue_job(
                # MAKE SURE you add SYNC_ENTITY_SCHEMAS to your JobType class!
                job_type=JobType.SYNC_ENTITY_SCHEMAS, 
                payload={},
                priority=0,
            )
            logger.info(f"[Cron] Enqueued sync_entity_schemas job id={job.id}")
            return Response(
                {"ok": True, "job_id": job.id, "message": "Sync entity schemas job enqueued"},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logger.exception(f"[Cron] Failed to enqueue sync_entity_schemas job: {e}")
            return Response(
                {"ok": False, "error": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )