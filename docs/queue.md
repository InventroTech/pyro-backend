# Background Job Queue System

## Overview

The background job queue system is a standalone Django app (`background_jobs`) that provides a reliable, extensible way to process asynchronous tasks. It uses PostgreSQL as the queue backend, eliminating the need for external infrastructure while providing durability and reliability.

**Note:** This is a separate app from `crm_records` and can be used by any Django app in the project.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Django Application (Render)                            │
│                                                          │
│  ┌──────────────┐    ┌──────────────────┐              │
│  │ Any App      │───▶│ BackgroundJob    │              │
│  │ (crm_records)│    │ (PostgreSQL)     │              │
│  │              │    │ background_jobs  │              │
│  └──────────────┘    └──────────────────┘              │
│                          ▲                                │
│                          │                                │
│  ┌───────────────────────┘                                │
│  │ Background Worker Thread                              │
│  │ (Started in BackgroundJobsConfig.ready())           │
│  │ - Polls every 1-2 seconds                            │
│  │ - Processes jobs via JobProcessor                    │
│  │ - Handles retries with exponential backoff           │
│  └───────────────────────────────────────────────────────┘
└─────────────────────────────────────────────────────────┘
```

**App Structure:** The job queue system is in the `background_jobs` app, separate from `crm_records` and can be used by any Django app.

### Key Components

1. **BackgroundJob Model**: Database table storing all queued jobs
2. **Job Handlers**: Plugin-based system for processing different job types
3. **Job Processor**: Core logic for fetching, locking, and processing jobs
4. **Queue Service**: High-level API for enqueueing jobs
5. **Worker Thread**: Background thread that continuously processes jobs

## Features

- **Database-backed queue**: Uses PostgreSQL (no external dependencies)
- **Plugin-based handlers**: Easy to add new job types
- **Priority support**: Process high-priority jobs first
- **Automatic retries**: Exponential backoff with configurable max attempts
- **Dead-letter queue**: Failed jobs stored for inspection
- **Worker locking**: Prevents duplicate processing
- **Stale lock cleanup**: Automatically recovers from crashed workers
- **Tenant isolation**: Jobs are scoped by tenant

## Usage

### Enqueueing a Job

```python
from background_jobs.queue_service import get_queue_service
from background_jobs.models import JobType

queue_service = get_queue_service()

# Enqueue a Mixpanel event
job = queue_service.enqueue_job(
    job_type=JobType.SEND_MIXPANEL_EVENT,
    payload={
        "user_id": "123",
        "event_name": "user_signup",
        "properties": {"plan": "premium"}
    },
    priority=0,  # Normal priority
    tenant_id="tenant-uuid"  # Optional
)

print(f"Job queued with ID: {job.id}")
```

### Checking Job Status

```python
from background_jobs.queue_service import get_queue_service

queue_service = get_queue_service()
status = queue_service.get_job_status(job_id=123)

print(f"Status: {status['status']}")
print(f"Attempts: {status['attempts']}/{status['max_attempts']}")
if status['last_error']:
    print(f"Error: {status['last_error']}")
```

### Retrying a Failed Job

```python
from background_jobs.queue_service import get_queue_service

queue_service = get_queue_service()
retried_job = queue_service.retry_failed_job(job_id=123)
```

## Adding New Job Types

To add a new job type, follow these steps:

### 1. Add JobType Choice

Edit `src/background_jobs/models.py`:

```python
class JobType(models.TextChoices):
    # ... existing types ...
    SEND_EMAIL = "send_email", "Send Email"
```

### 2. Create Job Handler

Create a new handler in `src/background_jobs/job_handlers.py`:

```python
class EmailJobHandler(JobHandler):
    """Handler for sending emails"""
    
    def process(self, job: BackgroundJob) -> bool:
        payload = job.payload
        to_email = payload.get("to")
        subject = payload.get("subject")
        body = payload.get("body")
        
        # Your email sending logic here
        try:
            send_email(to_email, subject, body)
            return True
        except Exception as e:
            logger.error(f"Email failed: {e}")
            raise
    
    def get_retry_delay(self, attempt: int) -> int:
        """Exponential backoff: 5s, 30s, 120s"""
        delays = [5, 30, 120]
        return delays[min(attempt - 1, len(delays) - 1)]
    
    def validate_payload(self, payload: Dict[str, Any]) -> bool:
        required = ["to", "subject", "body"]
        return all(field in payload for field in required)
```

### 3. Register Handler

The handler is automatically registered in `JobHandlerRegistry._register_default_handlers()`. Add your handler:

```python
def _register_default_handlers(self):
    from .models import JobType
    self.register_handler(JobType.SEND_MIXPANEL_EVENT, MixpanelJobHandler())
    self.register_handler(JobType.SEND_WEBHOOK, WebhookJobHandler())
    self.register_handler(JobType.SEND_EMAIL, EmailJobHandler())  # Add this
```

### 4. Use the New Job Type

```python
queue_service.enqueue_job(
    job_type=JobType.SEND_EMAIL,
    payload={
        "to": "user@example.com",
        "subject": "Welcome",
        "body": "Thanks for signing up!"
    }
)
```

## Priority Guidelines

Job priorities are integers where higher numbers = higher priority:

- **10**: Critical/urgent (user-facing, time-sensitive)
- **5**: High priority (important business logic)
- **0**: Normal (default, most jobs)
- **-5**: Low priority (background cleanup, analytics)

## Retry Strategy

### Default Behavior

- **Max attempts**: 3 (configurable per job)
- **Exponential backoff**: Handler-specific delays
- **After max attempts**: Job moves to FAILED status (dead-letter queue)

### Retry Delays by Handler

- **Mixpanel**: 1s, 10s, 60s
- **Webhook**: 2s, 20s, 120s
- **Custom handlers**: Define your own delays

### Manual Retry

Failed jobs can be manually retried via:
- Django admin interface
- API endpoint: `POST /api/jobs/<job_id>/retry/`
- `queue_service.retry_failed_job(job_id)`

## Monitoring

### API Endpoints

- `GET /jobs/status/` - Queue statistics (pending, processing, failed counts)
- `GET /jobs/<job_id>/` - Job details
- `POST /jobs/<job_id>/retry/` - Retry a single failed job
- `POST /jobs/bulk-retry/` - Retry multiple failed jobs (request body: `{"job_ids": [1, 2, 3]}`)
- `GET /jobs/failed/` - List failed jobs with pagination

### API Endpoints for Job Management

All job management is done via REST API endpoints (no Django admin):
- View job details: `GET /api/jobs/<job_id>/`
- Check queue status: `GET /api/jobs/status/`
- List failed jobs: `GET /api/jobs/failed/`
- Retry failed job: `POST /api/jobs/<job_id>/retry/`

### Logging

The system logs important events:
- Job enqueueing
- Job processing start/completion
- Retry attempts
- Failures with error messages
- Stale lock cleanup

Check logs for patterns like:
```
[Worker hostname-12345] Processing job 1: send_mixpanel_event
[Worker hostname-12345] Completed job 1
```

## Worker Configuration

The worker thread is automatically started when Django starts. Configuration is in `src/background_jobs/apps.py`:

- **Poll interval**: 1.0 seconds (time between polls when no jobs)
- **Batch size**: 10 jobs per batch
- **Stale cleanup**: Every 10 iterations

To customize, modify the `start_worker_loop()` call in `BackgroundJobsConfig.ready()`.

## Troubleshooting

### Jobs Stuck in PROCESSING

If jobs are stuck in PROCESSING status (likely from a crashed worker):

1. The worker automatically cleans up stale locks every 10 iterations
2. Stale threshold: 5 minutes (configurable)
3. Manual cleanup: Jobs will be reset to PENDING on next worker iteration

### High Database Load

If polling causes high database load:

1. Increase `poll_interval` in worker configuration
2. Reduce `batch_size` if processing is slow
3. Consider migrating to Redis queue (see Migration Path)

### Jobs Not Processing

Check:
1. Worker thread is running (check logs for "Started background job worker thread")
2. Jobs are in PENDING status (not FAILED or RETRYING)
3. `scheduled_at` is not in the future
4. No database connection issues

## Migration Path to Redis

The system is designed to be easily migratable to Redis in the future:

1. Create `QueueBackend` abstract base class
2. Implement `RedisQueueBackend` using Redis lists/sorted sets
3. Add `QUEUE_BACKEND` setting (postgresql/redis)
4. Swap backend implementation without changing job handlers

This migration would be transparent to job handlers and the QueueService API.

## Best Practices

1. **Idempotency**: Design jobs to be safe if run multiple times
2. **Payload validation**: Always validate payloads in handlers
3. **Error handling**: Catch and log specific exceptions
4. **Priority**: Use appropriate priorities (don't overuse high priority)
5. **Monitoring**: Regularly check failed jobs and queue status
6. **Testing**: Test handlers with various failure scenarios

## Performance Considerations

### Current Scale

The PostgreSQL queue works well for:
- Up to ~10,000 jobs/day
- Small to medium job payloads (< 1MB)
- Moderate concurrency (1-10 workers)



## Security

- Jobs are tenant-scoped (automatic isolation)
- Admin endpoints require authentication (`IsTenantAuthenticated`)
- Job payloads are stored as JSON (validate inputs)
- Worker threads run with same permissions as Django process

## Future Enhancements

Potential improvements:
- Scheduled/recurring jobs (cron-like)
- Job dependencies (job B waits for job A)
- Job result storage and retrieval
- Metrics and monitoring dashboard
- WebSocket notifications for job completion
- Job cancellation support

