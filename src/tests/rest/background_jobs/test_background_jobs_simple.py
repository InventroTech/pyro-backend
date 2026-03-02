"""
Simple standalone unit test for background jobs.


Note: When run via Django's test runner (manage.py test), Django will still attempt
to set up a test database, but the tests themselves don't use it - all operations are mocked.
"""
import uuid
from unittest import TestCase
from unittest.mock import Mock, patch
from datetime import datetime

from background_jobs.models import JobStatus, JobType
from background_jobs.queue_service import QueueService
from background_jobs.job_processor import JobProcessor
from background_jobs.job_handlers import get_handler_registry


class SimpleBackgroundJobsTest(TestCase):
    """
    Simple standalone test for background jobs.
    All database operations are mocked - no actual database required.
    Tests core functionality: enqueue, process, complete, retry.
    """
    
    def setUp(self):
        """Set up test fixtures with mocks"""
        self.queue_service = QueueService()
        self.processor = JobProcessor(worker_id="test-worker")
        self.mock_tenant_id = str(uuid.uuid4())
    
    @patch('background_jobs.queue_service.BackgroundJob.objects')
    def test_enqueue_job_creates_with_correct_params(self, mock_objects):
        """Test that enqueue_job creates a job with correct parameters"""
        # Mock the create method
        mock_job = Mock()
        mock_job.id = 1
        mock_job.job_type = JobType.SEND_MIXPANEL_EVENT
        mock_job.status = JobStatus.PENDING
        mock_job.priority = 5
        mock_job.tenant_id = self.mock_tenant_id
        mock_job.payload = {"user_id": "123", "event_name": "test"}
        mock_job.max_attempts = 3
        mock_objects.create.return_value = mock_job
        
        # Enqueue a job
        job = self.queue_service.enqueue_job(
            job_type=JobType.SEND_MIXPANEL_EVENT,
            payload={"user_id": "123", "event_name": "test"},
            priority=5,
            tenant_id=self.mock_tenant_id
        )
        
        # Verify create was called with correct parameters
        mock_objects.create.assert_called_once()
        call_kwargs = mock_objects.create.call_args[1]
        self.assertEqual(call_kwargs['job_type'], JobType.SEND_MIXPANEL_EVENT)
        self.assertEqual(call_kwargs['status'], JobStatus.PENDING)
        self.assertEqual(call_kwargs['priority'], 5)
        self.assertEqual(call_kwargs['payload'], {"user_id": "123", "event_name": "test"})
        self.assertEqual(call_kwargs['max_attempts'], 3)
    
    @patch('background_jobs.queue_service.BackgroundJob.objects')
    def test_enqueue_job_validates_job_type(self, mock_objects):
        """Test that enqueue_job validates job type"""
        # Mock handler registry to not have the handler
        with patch.object(self.queue_service._handler_registry, 'has_handler', return_value=False):
            with self.assertRaises(ValueError) as cm:
                self.queue_service.enqueue_job(
                    job_type="invalid_type",
                    payload={}
                )
            self.assertIn("Invalid job type", str(cm.exception))
    
    @patch('background_jobs.job_processor.close_old_connections')
    @patch('background_jobs.job_processor.transaction')
    @patch('background_jobs.job_processor.BackgroundJob.objects')
    def test_lock_and_fetch_job_locks_correctly(self, mock_objects, mock_transaction, _mock_close):
        """Test that lock_and_fetch_job locks and updates job correctly"""
        mock_transaction.atomic.return_value.__enter__ = Mock(return_value=None)
        mock_transaction.atomic.return_value.__exit__ = Mock(return_value=False)

        mock_job = Mock()
        mock_job.id = 1
        mock_job.status = JobStatus.PENDING
        mock_job.attempts = 0
        mock_job.max_attempts = 3
        mock_job.scheduled_at = None
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.first.return_value = mock_job
        mock_objects.select_for_update.return_value = mock_query
        
        mock_objects.filter.return_value.update.return_value = 1
        
        locked_job = self.processor.lock_and_fetch_job()
        
        self.assertIsNotNone(locked_job)
        self.assertEqual(locked_job.id, 1)
        mock_objects.filter.assert_called()
    
    @patch('background_jobs.job_processor.close_old_connections')
    @patch('background_jobs.job_processor.transaction')
    @patch('background_jobs.job_processor.BackgroundJob.objects')
    def test_lock_and_fetch_job_returns_none_when_no_jobs(self, mock_objects, mock_transaction, _mock_close):
        """Test that lock_and_fetch_job returns None when no jobs available"""
        mock_transaction.atomic.return_value.__enter__ = Mock(return_value=None)
        mock_transaction.atomic.return_value.__exit__ = Mock(return_value=False)

        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.first.return_value = None
        mock_objects.select_for_update.return_value = mock_query
        
        locked_job = self.processor.lock_and_fetch_job()
        self.assertIsNone(locked_job)
    
    @patch('background_jobs.job_processor.get_handler_registry')
    def test_process_job_success(self, mock_registry):
        """Test processing a job successfully"""
        # Setup mock handler
        mock_handler = Mock()
        mock_handler.process.return_value = True
        mock_registry_instance = Mock()
        mock_registry_instance.get_handler.return_value = mock_handler
        mock_registry.return_value = mock_registry_instance
        
        # Replace the processor's handler registry
        self.processor._handler_registry = mock_registry_instance
        
        # Create mock job
        mock_job = Mock()
        mock_job.id = 1
        mock_job.job_type = JobType.SEND_MIXPANEL_EVENT
        mock_job.payload = {"user_id": "123", "event_name": "test"}
        mock_job.result = None
        
        # Process job
        success, error = self.processor.process_job(mock_job)
        
        # Verify success
        self.assertTrue(success)
        self.assertEqual(error, "")
        mock_handler.process.assert_called_once_with(mock_job)
    
    @patch('background_jobs.job_processor.get_handler_registry')
    def test_process_job_failure(self, mock_registry):
        """Test processing a job that fails"""
        # Setup mock handler to raise exception
        mock_handler = Mock()
        mock_handler.process.side_effect = Exception("Test error")
        mock_registry_instance = Mock()
        mock_registry_instance.get_handler.return_value = mock_handler
        mock_registry.return_value = mock_registry_instance
        
        # Replace the processor's handler registry
        self.processor._handler_registry = mock_registry_instance
        
        # Create mock job
        mock_job = Mock()
        mock_job.id = 1
        mock_job.job_type = JobType.SEND_MIXPANEL_EVENT
        mock_job.payload = {"user_id": "123", "event_name": "test"}  # Valid payload
        mock_job.result = None
        
        # Process job
        success, error = self.processor.process_job(mock_job)
        
        # Verify failure
        self.assertFalse(success)
        self.assertIn("Test error", error)
        self.assertIsNotNone(mock_job.result)
        self.assertFalse(mock_job.result.get("success", True))
    
    @patch('background_jobs.job_processor.timezone')
    def test_mark_job_complete(self, mock_timezone):
        """Test marking a job as completed"""
        # Setup timezone mock
        mock_timezone.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
        
        # Create mock job
        mock_job = Mock()
        mock_job.status = JobStatus.PROCESSING
        mock_job.locked_by = "test-worker"
        mock_job.locked_at = datetime(2024, 1, 1, 11, 0, 0)
        mock_job.result = None
        mock_job.save = Mock()
        
        # Mark as complete
        result = {"success": True, "data": "test"}
        self.processor.mark_job_complete(mock_job, result)
        
        # Verify job was updated
        self.assertEqual(mock_job.status, JobStatus.COMPLETED)
        self.assertIsNotNone(mock_job.completed_at)
        self.assertIsNone(mock_job.locked_by)
        self.assertIsNone(mock_job.locked_at)
        self.assertEqual(mock_job.result, result)
        mock_job.save.assert_called_once()
    
    @patch('background_jobs.job_processor.get_handler_registry')
    @patch('background_jobs.job_processor.timezone')
    def test_mark_job_failed_with_retry(self, mock_timezone, mock_registry):
        """Test marking a job as failed when retries are available"""
        # Setup mocks
        mock_timezone.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
        mock_handler = Mock()
        mock_handler.get_retry_delay.return_value = 5
        mock_registry_instance = Mock()
        mock_registry_instance.get_handler.return_value = mock_handler
        mock_registry.return_value = mock_registry_instance
        
        # Replace the processor's handler registry
        self.processor._handler_registry = mock_registry_instance
        
        # Create mock job with proper job_type as string
        mock_job = Mock()
        mock_job.job_type = JobType.SEND_MIXPANEL_EVENT  # Must be a string, not Mock
        mock_job.status = JobStatus.PROCESSING
        mock_job.attempts = 1
        mock_job.max_attempts = 3
        mock_job.locked_by = "test-worker"
        mock_job.locked_at = datetime(2024, 1, 1, 11, 0, 0)
        mock_job.save = Mock()
        
        # Mark as failed
        self.processor.mark_job_failed(mock_job, "Test error")
        
        # Verify job was scheduled for retry
        self.assertEqual(mock_job.status, JobStatus.PENDING)
        self.assertIsNotNone(mock_job.scheduled_at)
        self.assertIn("Test error", mock_job.last_error)
        self.assertIsNone(mock_job.locked_by)
        mock_job.save.assert_called_once()
    
    @patch('background_jobs.job_processor.get_handler_registry')
    @patch('background_jobs.job_processor.timezone')
    def test_mark_job_failed_no_retries(self, mock_timezone, mock_registry):
        """Test marking a job as failed when no retries left"""
        # Setup mocks
        mock_timezone.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
        mock_handler = Mock()
        mock_handler.get_retry_delay.return_value = 5
        mock_registry_instance = Mock()
        mock_registry_instance.get_handler.return_value = mock_handler
        mock_registry.return_value = mock_registry_instance
        
        # Replace the processor's handler registry
        self.processor._handler_registry = mock_registry_instance
        
        # Create mock job with max attempts reached and proper job_type
        mock_job = Mock()
        mock_job.job_type = JobType.SEND_MIXPANEL_EVENT  # Must be a string, not Mock
        mock_job.status = JobStatus.PROCESSING
        mock_job.attempts = 3
        mock_job.max_attempts = 3
        mock_job.locked_by = "test-worker"
        mock_job.locked_at = datetime(2024, 1, 1, 11, 0, 0)
        mock_job.save = Mock()
        
        # Mark as failed
        self.processor.mark_job_failed(mock_job, "Test error")
        
        # Verify job was marked as FAILED (not retrying)
        self.assertEqual(mock_job.status, JobStatus.FAILED)
        self.assertIn("Test error", mock_job.last_error)
        self.assertIsNone(mock_job.locked_by)
        mock_job.save.assert_called_once()
    
    def test_job_handler_registry(self):
        """Test that job handlers are properly registered"""
        registry = get_handler_registry()
        
        # Verify default handlers exist
        self.assertTrue(registry.has_handler(JobType.SEND_MIXPANEL_EVENT))
        self.assertTrue(registry.has_handler(JobType.SEND_WEBHOOK))
        self.assertTrue(registry.has_handler(JobType.EXECUTE_FUNCTION))
        
        # Verify we can get handlers
        handler = registry.get_handler(JobType.SEND_MIXPANEL_EVENT)
        self.assertIsNotNone(handler)
        # Verify handler has required methods
        self.assertTrue(hasattr(handler, 'process'))
        self.assertTrue(hasattr(handler, 'get_retry_delay'))
    
    @patch('background_jobs.job_processor.JobProcessor.lock_and_fetch_job')
    @patch('background_jobs.job_processor.JobProcessor.process_job')
    @patch('background_jobs.job_processor.JobProcessor.mark_job_complete')
    def test_process_next_job_success(self, mock_mark_complete, mock_process, mock_lock):
        """Test processing next job successfully"""
        # Setup mocks
        mock_job = Mock()
        mock_job.result = {"success": True}
        mock_lock.return_value = mock_job
        mock_process.return_value = (True, "")
        
        # Process next job
        result = self.processor.process_next_job()
        
        # Verify it processed
        self.assertTrue(result)
        mock_lock.assert_called_once()
        mock_process.assert_called_once_with(mock_job)
        # mark_job_complete is called with result= keyword argument
        mock_mark_complete.assert_called_once_with(mock_job, result=mock_job.result)
    
    @patch('background_jobs.job_processor.JobProcessor.lock_and_fetch_job')
    def test_process_next_job_no_jobs(self, mock_lock):
        """Test processing when no jobs available"""
        mock_lock.return_value = None
        
        result = self.processor.process_next_job()
        
        self.assertFalse(result)
        mock_lock.assert_called_once()
    
    @patch('background_jobs.job_processor.timezone')
    @patch('background_jobs.job_processor.BackgroundJob.objects')
    def test_cleanup_stale_locks(self, mock_objects, mock_timezone):
        """Test cleaning up stale job locks"""
        # Setup timezone mock
        from datetime import timedelta
        mock_timezone.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
        
        # Mock stale jobs query
        mock_stale_jobs = Mock()
        mock_stale_jobs.update.return_value = 2  # 2 stale jobs found
        mock_objects.filter.return_value = mock_stale_jobs
        
        # Cleanup stale locks
        count = self.processor.cleanup_stale_locks(stale_threshold_minutes=5)
        
        # Verify cleanup was called
        self.assertEqual(count, 2)
        mock_objects.filter.assert_called_once()
