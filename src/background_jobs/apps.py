import os
import sys
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
        
        Note: With Gunicorn --preload, this runs in the master process.
        The actual worker threads are started in gunicorn_config.py post_fork hook.
        """
        # Skip if running migrations or other management commands
        if len(sys.argv) > 1 and any(cmd in sys.argv for cmd in ['migrate', 'makemigrations', 'collectstatic', 'shell', 'test']):
            return
        
        # If we're running under Gunicorn with --preload, skip here
        # The worker will be started in gunicorn_config.py post_fork hook instead
        if 'gunicorn' in ' '.join(sys.argv) or 'gunicorn' in os.environ.get('_', ''):
            # Worker will be started in gunicorn_config.py post_fork hook
            logger.info("Running under Gunicorn - worker thread will be started in post_fork hook")
            print("[BACKGROUND_JOBS] Running under Gunicorn - worker thread will be started in post_fork hook", flush=True)
            return
        
        # For development server, only start in the main process (not the reloader)
        if os.environ.get('RUN_MAIN') != 'true':
            return
        
        # Start worker thread for development server
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
            
            log_msg = f"Started background job worker thread: {worker_id}"
            logger.info(log_msg)
            print(f"[BACKGROUND_JOBS] {log_msg}", flush=True)
            
        except Exception as e:
            error_msg = f"Failed to start background job worker: {e}"
            logger.error(error_msg, exc_info=True)
            print(f"[BACKGROUND_JOBS] ERROR: {error_msg}", flush=True, file=sys.stderr)


