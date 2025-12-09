"""
Gunicorn configuration file for background job worker startup.

This ensures the background job worker thread starts in each Gunicorn worker process,
not in the master process (important when using --preload flag).
"""

import os
import sys
import threading
import logging
import socket

logger = logging.getLogger('background_jobs')


def post_fork(server, worker):
    """
    Called after a worker has been forked.
    This is where we start the background job worker thread in each Gunicorn worker.
    """
    try:
        # Import here to avoid issues with Django not being ready
        from background_jobs.job_processor import JobProcessor
        
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
            daemon=True,
            name='BackgroundJobWorker'
        )
        worker_thread.start()
        
        # Use both logger and print to ensure visibility on Render
        log_msg = f"Started background job worker thread in Gunicorn worker: {worker_id}"
        logger.info(log_msg)
        print(f"[BACKGROUND_JOBS] {log_msg}", flush=True)
        
    except Exception as e:
        error_msg = f"Failed to start background job worker in Gunicorn worker: {e}"
        logger.error(error_msg, exc_info=True)
        print(f"[BACKGROUND_JOBS] ERROR: {error_msg}", flush=True, file=sys.stderr)
