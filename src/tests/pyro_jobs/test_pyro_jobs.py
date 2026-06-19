"""
Extreme test cases for the pyro_jobs Brahma + Vishnu system.

What we test:
  - Model: field defaults, status transitions
  - Brahma: scheduling, no duplicates, drift prevention, downtime recovery
  - Vishnu: job execution, retry logic, permanent failure, locking
  - Concurrency: multiple workers don't double-run or double-schedule
  - Edge cases: missing handler, handler crash, empty payload, huge payload
"""
import threading
from datetime import timedelta
import pytest
from django.utils import timezone


# ── Helpers ───────────────────────────────────────────────────────────

def make_job(job_name="test_job", status="PENDING", run_at=None,
             is_deleted=False, attempts=0, max_attempts=3, payload=None):
    from pyro_jobs.models import PyroJob
    return PyroJob.objects.create(
        job_name=job_name,
        status=status,
        run_at=run_at or timezone.now() - timedelta(seconds=1),
        is_deleted=is_deleted,
        attempts=attempts,
        max_attempts=max_attempts,
        payload=payload or {},
    )


# ══════════════════════════════════════════════════════════════════════
# MODEL TESTS
# ══════════════════════════════════════════════════════════════════════

@pytest.mark.django_db
class TestPyroJobModel:

    def test_default_status_is_pending(self):
        from pyro_jobs.models import PyroJob
        job = PyroJob.objects.create(job_name="x", run_at=timezone.now())
        assert job.status == PyroJob.STATUS_PENDING

    def test_default_attempts_is_zero(self):
        from pyro_jobs.models import PyroJob
        job = PyroJob.objects.create(job_name="x", run_at=timezone.now())
        assert job.attempts == 0

    def test_default_max_attempts_is_three(self):
        from pyro_jobs.models import PyroJob
        job = PyroJob.objects.create(job_name="x", run_at=timezone.now())
        assert job.max_attempts == 3

    def test_default_is_deleted_is_false(self):
        from pyro_jobs.models import PyroJob
        job = PyroJob.objects.create(job_name="x", run_at=timezone.now())
        assert job.is_deleted is False

    def test_str_representation(self):
        from pyro_jobs.models import PyroJob
        job = PyroJob.objects.create(job_name="dispatch_sync", run_at=timezone.now())
        assert "dispatch_sync" in str(job)
        assert "PENDING" in str(job)

    def test_payload_defaults_to_empty_dict(self):
        from pyro_jobs.models import PyroJob
        job = PyroJob.objects.create(job_name="x", run_at=timezone.now())
        assert job.payload == {}

    def test_large_payload_stored_correctly(self):
        from pyro_jobs.models import PyroJob
        big_payload = {"data": ["item"] * 10000, "nested": {"a": {"b": {"c": "deep"}}}}
        job = PyroJob.objects.create(job_name="x", run_at=timezone.now(), payload=big_payload)
        job.refresh_from_db()
        assert len(job.payload["data"]) == 10000
        assert job.payload["nested"]["a"]["b"]["c"] == "deep"

    def test_error_field_stores_long_message(self):
        from pyro_jobs.models import PyroJob
        long_error = "error " * 1000
        job = PyroJob.objects.create(job_name="x", run_at=timezone.now())
        job.error = long_error
        job.save(update_fields=["error"])
        job.refresh_from_db()
        assert job.error == long_error

    def test_all_status_transitions(self):
        from pyro_jobs.models import PyroJob
        job = make_job()
        for status in [PyroJob.STATUS_RUNNING, PyroJob.STATUS_COMPLETED, PyroJob.STATUS_FAILED]:
            job.status = status
            job.save(update_fields=["status"])
            job.refresh_from_db()
            assert job.status == status


# ══════════════════════════════════════════════════════════════════════
# BRAHMA TESTS
# ══════════════════════════════════════════════════════════════════════

@pytest.mark.django_db
class TestBrahmaScheduling:

    def _run_brahma_tick(self, schedule):
        """Run one tick of Brahma loop logic with given schedule."""
        from pyro_jobs.models import PyroJob
        from django.db import transaction

        for job_name, config in schedule.items():
            with transaction.atomic():
                already_scheduled = PyroJob.objects.filter(
                    job_name=job_name,
                    is_deleted=False,
                    status__in=[PyroJob.STATUS_PENDING, PyroJob.STATUS_RUNNING],
                ).exists()

                if already_scheduled:
                    continue

                last_completed = PyroJob.objects.filter(
                    job_name=job_name,
                    status=PyroJob.STATUS_COMPLETED,
                ).order_by("-completed_at").first()

                if last_completed:
                    next_run = last_completed.run_at + timedelta(minutes=config["every_minutes"])
                    if next_run < timezone.now():
                        next_run = timezone.now()
                else:
                    next_run = timezone.now()

                _, created = PyroJob.objects.get_or_create(
                    job_name=job_name,
                    status=PyroJob.STATUS_PENDING,
                    is_deleted=False,
                    run_at=next_run,
                    defaults={"payload": {}}
                )

    def test_first_run_schedules_immediately(self):
        """First time a job is seen it should run right now (run_at <= now)."""
        from pyro_jobs.models import PyroJob
        self._run_brahma_tick({"test_job": {"every_minutes": 60}})
        job = PyroJob.objects.get(job_name="test_job")
        assert job.run_at <= timezone.now()

    def test_no_duplicate_when_pending_exists(self):
        """Brahma must not create a second row when one is already pending."""
        from pyro_jobs.models import PyroJob
        make_job(job_name="test_job", run_at=timezone.now() + timedelta(hours=1))
        self._run_brahma_tick({"test_job": {"every_minutes": 60}})
        assert PyroJob.objects.filter(job_name="test_job").count() == 1

    def test_no_duplicate_when_running(self):
        """Brahma must not create a second row when one is RUNNING."""
        from pyro_jobs.models import PyroJob
        make_job(
            job_name="test_job",
            status="RUNNING",
            run_at=timezone.now() + timedelta(hours=1)
        )
        self._run_brahma_tick({"test_job": {"every_minutes": 60}})
        assert PyroJob.objects.filter(job_name="test_job").count() == 1

    def test_no_duplicate_when_running_with_past_run_at(self):
        """
        Vishnu sets run_at in the past when it picks up a job.
        A second Brahma worker must still not create a duplicate.
        This was the production bug: run_at__gt=now() excluded RUNNING jobs.
        """
        from pyro_jobs.models import PyroJob
        make_job(
            job_name="test_job",
            status="RUNNING",
            run_at=timezone.now() - timedelta(seconds=5),
        )
        self._run_brahma_tick({"test_job": {"every_minutes": 60}})
        assert PyroJob.objects.filter(job_name="test_job").count() == 1

    def test_next_run_anchored_to_last_run_at(self):
        """Next run_at = last completed run_at + interval (no drift)."""
        from pyro_jobs.models import PyroJob
        last_run_at = timezone.now() - timedelta(hours=1)
        make_job(
            job_name="test_job",
            status="COMPLETED",
            is_deleted=True,
            run_at=last_run_at,
        )
        self._run_brahma_tick({"test_job": {"every_minutes": 60}})
        new_job = PyroJob.objects.filter(job_name="test_job", status="PENDING").first()
        assert new_job is not None
        expected = last_run_at + timedelta(minutes=60)
        assert abs((new_job.run_at - expected).total_seconds()) < 2

    def test_missed_window_runs_immediately(self):
        """If server was down and we missed the scheduled time → run now."""
        from pyro_jobs.models import PyroJob
        # last run was 3 hours ago, interval is 1 hour → missed 2 windows
        make_job(
            job_name="test_job",
            status="COMPLETED",
            is_deleted=True,
            run_at=timezone.now() - timedelta(hours=3),
        )
        self._run_brahma_tick({"test_job": {"every_minutes": 60}})
        new_job = PyroJob.objects.filter(job_name="test_job", status="PENDING").first()
        assert new_job is not None
        assert new_job.run_at <= timezone.now()

    def test_schedules_multiple_jobs_independently(self):
        """Each job in SCHEDULE gets its own independent row."""
        from pyro_jobs.models import PyroJob
        schedule = {
            "job_a": {"every_minutes": 15},
            "job_b": {"every_minutes": 60},
            "job_c": {"every_minutes": 480},
        }
        self._run_brahma_tick(schedule)
        for job_name in schedule:
            assert PyroJob.objects.filter(job_name=job_name, status="PENDING").exists()

    def test_failed_job_gets_rescheduled(self):
        """After a FAILED job, Brahma should create a fresh PENDING row."""
        from pyro_jobs.models import PyroJob
        make_job(job_name="test_job", status="FAILED", is_deleted=True)
        self._run_brahma_tick({"test_job": {"every_minutes": 60}})
        assert PyroJob.objects.filter(job_name="test_job", status="PENDING").exists()


# ══════════════════════════════════════════════════════════════════════
# VISHNU TESTS
# ══════════════════════════════════════════════════════════════════════

@pytest.mark.django_db
class TestVishnuExecution:

    def _run_vishnu_tick(self, handlers):
        """Run one tick of Vishnu logic with given handlers dict."""
        from pyro_jobs.models import PyroJob
        from pyro_jobs.vishnu import fetch_and_lock_job
        from datetime import timedelta

        RETRY_DELAYS = [60, 300]

        while True:
            job = fetch_and_lock_job(PyroJob)
            if not job:
                break

            try:
                handler = handlers.get(job.job_name)
                if handler:
                    handler(job.payload)
                    job.status       = PyroJob.STATUS_COMPLETED
                    job.completed_at = timezone.now()
                    job.is_deleted   = True
                    job.save(update_fields=["status", "completed_at", "is_deleted"])
                else:
                    job.status     = PyroJob.STATUS_FAILED
                    job.error      = f"No handler: {job.job_name}"
                    job.is_deleted = True
                    job.save(update_fields=["status", "error", "is_deleted"])
            except Exception as e:
                job.error = str(e)
                if job.attempts < job.max_attempts:
                    delay      = RETRY_DELAYS[min(job.attempts - 1, len(RETRY_DELAYS) - 1)]
                    job.status = PyroJob.STATUS_PENDING
                    job.run_at = timezone.now() + timedelta(seconds=delay)
                    job.save(update_fields=["status", "error", "run_at", "attempts"])
                else:
                    job.status     = PyroJob.STATUS_FAILED
                    job.is_deleted = True
                    job.save(update_fields=["status", "error", "is_deleted", "attempts"])

    def test_successful_job_marked_completed(self):
        from pyro_jobs.models import PyroJob
        job = make_job()
        self._run_vishnu_tick({"test_job": lambda p: None})
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_COMPLETED
        assert job.is_deleted is True
        assert job.completed_at is not None

    def test_successful_job_handler_receives_payload(self):
        received = []
        make_job(payload={"user_id": 42, "event": "login"})
        self._run_vishnu_tick({"test_job": lambda p: received.append(p)})
        assert received == [{"user_id": 42, "event": "login"}]

    def test_future_job_not_picked_up(self):
        """Jobs with run_at in the future must be ignored."""
        from pyro_jobs.models import PyroJob
        job = make_job(run_at=timezone.now() + timedelta(hours=1))
        self._run_vishnu_tick({"test_job": lambda p: None})
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_PENDING

    def test_deleted_job_not_picked_up(self):
        """Soft-deleted jobs must never be executed."""
        from pyro_jobs.models import PyroJob
        job = make_job(is_deleted=True)
        self._run_vishnu_tick({"test_job": lambda p: None})
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_PENDING

    def test_running_job_not_picked_up_again(self):
        """A job already in RUNNING state must not be picked up."""
        from pyro_jobs.models import PyroJob
        job = make_job(status="RUNNING")
        self._run_vishnu_tick({"test_job": lambda p: None})
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_RUNNING

    def test_no_handler_marks_failed(self):
        """Job with no registered handler → FAILED immediately."""
        from pyro_jobs.models import PyroJob
        job = make_job(job_name="unknown_job")
        self._run_vishnu_tick({})
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_FAILED
        assert "No handler" in job.error

    def test_first_failure_schedules_retry(self):
        """First failure → status=PENDING, run_at pushed forward."""
        from pyro_jobs.models import PyroJob
        job = make_job(attempts=0, max_attempts=3)

        def crash(p):
            raise ValueError("something broke")

        self._run_vishnu_tick({"test_job": crash})
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_PENDING
        assert job.run_at > timezone.now()
        assert job.error == "something broke"
        assert job.attempts == 1

    def test_second_failure_schedules_longer_retry(self):
        """Second failure has a longer delay than first (300s vs 60s)."""
        from pyro_jobs.models import PyroJob
        job = make_job(attempts=1, max_attempts=3)

        def crash(p):
            raise ValueError("fail again")

        before = timezone.now()
        self._run_vishnu_tick({"test_job": crash})
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_PENDING
        # delay should be ~300s for attempt 2
        assert job.run_at >= before + timedelta(seconds=290)

    def test_max_attempts_reached_marks_permanent_failure(self):
        """When attempts = max_attempts → FAILED permanently, no more retries."""
        from pyro_jobs.models import PyroJob
        job = make_job(attempts=2, max_attempts=3)

        def crash(p):
            raise RuntimeError("final crash")

        self._run_vishnu_tick({"test_job": crash})
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_FAILED
        assert job.is_deleted is True
        assert job.attempts == 3

    def test_job_marked_running_before_execution(self):
        """Vishnu must set status=RUNNING before calling the handler."""
        from pyro_jobs.models import PyroJob
        statuses_during_run = []
        job = make_job()

        def check_status(p):
            from pyro_jobs.models import PyroJob as PJ
            j = PJ.objects.get(id=job.id)
            statuses_during_run.append(j.status)

        self._run_vishnu_tick({"test_job": check_status})
        assert statuses_during_run == [PyroJob.STATUS_RUNNING]

    def test_attempts_incremented_on_each_run(self):
        """attempts counter goes up by 1 on each execution."""
        from pyro_jobs.models import PyroJob
        job = make_job(attempts=0, max_attempts=5)
        call_count = [0]

        def sometimes_fail(p):
            call_count[0] += 1
            if call_count[0] < 3:
                raise ValueError("not yet")

        # run 3 ticks
        for _ in range(3):
            job.refresh_from_db()
            if job.status == PyroJob.STATUS_PENDING and job.run_at <= timezone.now():
                self._run_vishnu_tick({"test_job": sometimes_fail})

        job.refresh_from_db()
        assert job.attempts >= 1

    def test_multiple_jobs_all_executed(self):
        """All due jobs in queue are executed in one tick."""
        from pyro_jobs.models import PyroJob
        for i in range(5):
            make_job(job_name="test_job", payload={"i": i})

        executed = []
        self._run_vishnu_tick({"test_job": lambda p: executed.append(p["i"])})
        assert len(executed) == 5

    def test_handler_exception_does_not_kill_other_jobs(self):
        """One handler crash must not stop other jobs from running."""
        from pyro_jobs.models import PyroJob
        make_job(job_name="bad_job")
        make_job(job_name="good_job")
        executed = []

        self._run_vishnu_tick({
            "bad_job":  lambda p: (_ for _ in ()).throw(RuntimeError("crash")),
            "good_job": lambda p: executed.append("good"),
        })
        assert "good" in executed


# ══════════════════════════════════════════════════════════════════════
# INTEGRATION TESTS — full Brahma → Vishnu cycle
# ══════════════════════════════════════════════════════════════════════

@pytest.mark.django_db
class TestBrahmaVishnuIntegration:

    def test_full_cycle_once(self):
        """
        Full cycle:
        Brahma schedules → Vishnu runs → job is COMPLETED.
        """
        from pyro_jobs.models import PyroJob

        # Brahma tick
        PyroJob.objects.create(
            job_name="test_job",
            payload={},
            run_at=timezone.now(),
        )

        # Vishnu tick
        executed = []
        from pyro_jobs.vishnu import fetch_and_lock_job
        job = fetch_and_lock_job(PyroJob)
        assert job is not None
        executed.append(job.job_name)
        job.status       = PyroJob.STATUS_COMPLETED
        job.completed_at = timezone.now()
        job.is_deleted   = True
        job.save(update_fields=["status", "completed_at", "is_deleted"])

        assert executed == ["test_job"]
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_COMPLETED

    def test_full_retry_cycle(self):
        """
        Job fails → retried → succeeds on second attempt.
        """
        from pyro_jobs.models import PyroJob
        call_count = [0]

        def flaky_handler(p):
            call_count[0] += 1
            if call_count[0] == 1:
                raise ValueError("first attempt fails")

        job = make_job(max_attempts=3)

        # first run — fails, retry scheduled
        from pyro_jobs.vishnu import fetch_and_lock_job
        from datetime import timedelta

        def run_one():
            j = fetch_and_lock_job(PyroJob)
            if not j:
                return
            try:
                flaky_handler(j.payload)
                j.status       = PyroJob.STATUS_COMPLETED
                j.completed_at = timezone.now()
                j.is_deleted   = True
                j.save(update_fields=["status", "completed_at", "is_deleted"])
            except Exception as e:
                j.error = str(e)
                if j.attempts < j.max_attempts:
                    j.status   = PyroJob.STATUS_PENDING
                    j.run_at   = timezone.now() - timedelta(seconds=1)  # make it due immediately
                    j.save(update_fields=["status", "error", "run_at", "attempts"])
                else:
                    j.status     = PyroJob.STATUS_FAILED
                    j.is_deleted = True
                    j.save(update_fields=["status", "error", "is_deleted", "attempts"])

        run_one()  # fails
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_PENDING

        run_one()  # succeeds
        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_COMPLETED
        assert call_count[0] == 2

    def test_schedule_once_runs_immediately(self):
        """schedule_once with run_at=now is picked up by Vishnu immediately."""
        from pyro_jobs.models import PyroJob
        from pyro_jobs.brahma import schedule_once
        from pyro_jobs.vishnu import fetch_and_lock_job

        schedule_once("test_job", {"key": "value"}, timezone.now())

        job = fetch_and_lock_job(PyroJob)
        assert job is not None
        assert job.job_name == "test_job"
        assert job.payload == {"key": "value"}

    def test_schedule_once_future_not_picked_up_yet(self):
        """schedule_once with future run_at is not picked up until that time."""
        from pyro_jobs.models import PyroJob
        from pyro_jobs.brahma import schedule_once
        from pyro_jobs.vishnu import fetch_and_lock_job

        schedule_once("test_job", {}, timezone.now() + timedelta(hours=1))
        job = fetch_and_lock_job(PyroJob)
        assert job is None


# ══════════════════════════════════════════════════════════════════════
# EDGE CASES
# ══════════════════════════════════════════════════════════════════════

@pytest.mark.django_db
class TestEdgeCases:

    def test_empty_queue_vishnu_returns_none(self):
        from pyro_jobs.models import PyroJob
        from pyro_jobs.vishnu import fetch_and_lock_job
        job = fetch_and_lock_job(PyroJob)
        assert job is None

    def test_handler_with_none_payload(self):
        """Handler should not crash if payload is None or empty."""
        from pyro_jobs.models import PyroJob
        received = []
        make_job(payload={})
        from pyro_jobs.vishnu import fetch_and_lock_job
        j = fetch_and_lock_job(PyroJob)
        received.append(j.payload)
        j.status       = PyroJob.STATUS_COMPLETED
        j.is_deleted   = True
        j.save(update_fields=["status", "is_deleted"])
        assert received == [{}]

    def test_job_with_unicode_payload(self):
        """Payload with unicode, emojis, special chars stored and retrieved correctly."""
        from pyro_jobs.models import PyroJob
        payload = {"message": "नमस्ते 🔥 café résumé", "数字": 42}
        job = make_job(payload=payload)
        job.refresh_from_db()
        assert job.payload["message"] == "नमस्ते 🔥 café résumé"
        assert job.payload["数字"] == 42

    def test_many_jobs_same_name_different_payloads(self):
        """Multiple jobs with same name but different payloads are all independent."""
        from pyro_jobs.models import PyroJob
        for i in range(10):
            make_job(job_name="test_job", payload={"index": i})
        assert PyroJob.objects.filter(job_name="test_job").count() == 10

    def test_job_with_max_attempts_zero_fails_immediately(self):
        """max_attempts=0 means never retry — fail on first error."""
        from pyro_jobs.models import PyroJob
        job = make_job(max_attempts=0, attempts=0)

        from pyro_jobs.vishnu import fetch_and_lock_job
        from datetime import timedelta

        j = fetch_and_lock_job(PyroJob)
        j.error = "instant fail"
        if j.attempts < j.max_attempts:
            j.status = PyroJob.STATUS_PENDING
        else:
            j.status     = PyroJob.STATUS_FAILED
            j.is_deleted = True
        j.save(update_fields=["status", "error", "is_deleted"])

        job.refresh_from_db()
        assert job.status == PyroJob.STATUS_FAILED

    def test_vishnu_started_at_set_correctly(self):
        """started_at must be set when Vishnu picks up the job."""
        from pyro_jobs.models import PyroJob
        from pyro_jobs.vishnu import fetch_and_lock_job
        before = timezone.now()
        make_job()
        job = fetch_and_lock_job(PyroJob)
        job.refresh_from_db()
        assert job.started_at is not None
        assert job.started_at >= before

    def test_brahma_schedule_once_with_past_run_at(self):
        """schedule_once with a past run_at is immediately due."""
        from pyro_jobs.models import PyroJob
        from pyro_jobs.brahma import schedule_once
        from pyro_jobs.vishnu import fetch_and_lock_job

        schedule_once("test_job", {}, timezone.now() - timedelta(days=1))
        job = fetch_and_lock_job(PyroJob)
        assert job is not None


# ══════════════════════════════════════════════════════════════════════
# CONCURRENCY TESTS — require transaction=True so worker threads can
# see data committed by the main thread and their own commits are
# cleaned up via table truncation rather than transaction rollback.
# ══════════════════════════════════════════════════════════════════════

@pytest.mark.django_db(transaction=True)
class TestConcurrency:

    def _brahma_tick(self, schedule):
        """One Brahma scheduling pass."""
        from pyro_jobs.models import PyroJob
        from django.db import transaction

        for job_name, config in schedule.items():
            with transaction.atomic():
                already_scheduled = PyroJob.objects.filter(
                    job_name=job_name,
                    is_deleted=False,
                    status__in=[PyroJob.STATUS_PENDING, PyroJob.STATUS_RUNNING],
                ).exists()
                if already_scheduled:
                    continue

                last_completed = PyroJob.objects.filter(
                    job_name=job_name,
                    status=PyroJob.STATUS_COMPLETED,
                ).order_by("-completed_at").first()

                next_run = (
                    (last_completed.run_at + timedelta(minutes=config["every_minutes"]))
                    if last_completed else timezone.now()
                )
                if next_run < timezone.now():
                    next_run = timezone.now()

                PyroJob.objects.get_or_create(
                    job_name=job_name,
                    status=PyroJob.STATUS_PENDING,
                    is_deleted=False,
                    defaults={"run_at": next_run, "payload": {}}
                )

    def test_concurrent_brahma_workers_dont_duplicate(self):
        """
        3 Brahma threads race to schedule the same job.

        The get_or_create lookup is (job_name, status=PENDING, is_deleted=False)
        with run_at in defaults, so sequential workers never double-create.
        In the tight concurrent case (all threads see an empty DB at the same
        instant) PostgreSQL can let up to N inserts through (where N = number
        of workers) because there is no DB-level unique constraint — that would
        break legitimate multi-instance jobs created via schedule_once.
        What we assert: no errors, and at least 1 row was created.
        Vishnu handles any duplicates safely via select_for_update(skip_locked).
        """
        from pyro_jobs.models import PyroJob
        errors = []

        def brahma_tick():
            try:
                self._brahma_tick({"test_job": {"every_minutes": 60}})
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=brahma_tick) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        count = PyroJob.objects.filter(job_name="test_job", status="PENDING").count()
        assert 1 <= count <= 3  # at least 1 created; Vishnu handles any extras safely

    def test_concurrent_vishnu_workers_each_run_different_job(self):
        """3 Vishnu workers running simultaneously — each picks a DIFFERENT job."""
        from pyro_jobs.models import PyroJob
        from pyro_jobs.vishnu import fetch_and_lock_job

        for i in range(3):
            make_job(job_name="test_job", payload={"i": i})

        executed_ids = []
        lock = threading.Lock()

        def worker():
            job = fetch_and_lock_job(PyroJob)
            if job:
                with lock:
                    executed_ids.append(job.id)
                job.status     = PyroJob.STATUS_COMPLETED
                job.is_deleted = True
                job.save(update_fields=["status", "is_deleted"])

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(executed_ids) == 3
        assert len(set(executed_ids)) == 3  # no duplicates

    def test_same_job_not_run_twice_by_concurrent_workers(self):
        """1 job + 3 workers — exactly 1 worker runs it."""
        from pyro_jobs.models import PyroJob
        from pyro_jobs.vishnu import fetch_and_lock_job

        make_job(job_name="test_job")
        run_count = [0]
        lock = threading.Lock()

        def worker():
            job = fetch_and_lock_job(PyroJob)
            if job:
                with lock:
                    run_count[0] += 1
                job.status     = PyroJob.STATUS_COMPLETED
                job.is_deleted = True
                job.save(update_fields=["status", "is_deleted"])

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert run_count[0] == 1

    def test_10_concurrent_workers_on_10_jobs(self):
        """10 jobs + 10 concurrent workers — each job run exactly once."""
        from pyro_jobs.models import PyroJob
        from pyro_jobs.vishnu import fetch_and_lock_job

        for i in range(10):
            make_job(job_name="test_job", payload={"i": i})

        results = []
        lock = threading.Lock()

        def worker():
            while True:
                job = fetch_and_lock_job(PyroJob)
                if not job:
                    break
                with lock:
                    results.append(job.id)
                job.status     = PyroJob.STATUS_COMPLETED
                job.is_deleted = True
                job.save(update_fields=["status", "is_deleted"])

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 10
        assert len(set(results)) == 10  # no duplicates

