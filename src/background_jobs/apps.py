import os
import threading
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class BackgroundJobsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'background_jobs'
    verbose_name = 'Background Jobs'
    
    def ready(self):
        """
        Start background job worker thread when Django is ready.
        This runs once per Django process (each Gunicorn worker will have its own thread).
        """
        # Only start in the main process, not in migrations or other commands
        if os.environ.get('RUN_MAIN') != 'true':
            return
        
        # Skip if running migrations or other management commands
        import sys
        if 'migrate' in sys.argv or 'makemigrations' in sys.argv:
            return
        
        try:
            from .job_processor import JobProcessor
            import socket
            
            # Create unique worker ID based on hostname and process
            worker_id = f"{socket.gethostname()}-{os.getpid()}"
            processor = JobProcessor(worker_id=worker_id)
            
            # Start worker in a daemon thread
            worker_thread = threading.Thread(
                target=processor.start_worker_loop,
                kwargs={
                    'poll_interval': 1.0,  # Check every second
                    'batch_size': 10,      # Process up to 10 jobs per batch
                },
                daemon=True,  # Daemon thread will stop when main process stops
                name='BackgroundJobWorker'
            )
            worker_thread.start()
            
            logger.info(f"Started background job worker thread: {worker_id}")
            
        except Exception as e:
            logger.error(f"Failed to start background job worker: {e}", exc_info=True)


