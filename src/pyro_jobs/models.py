from django.db import models


class PyroJob(models.Model):

    STATUS_PENDING   = "PENDING"
    STATUS_RUNNING   = "RUNNING"
    STATUS_COMPLETED = "COMPLETED"
    STATUS_FAILED    = "FAILED"

    STATUS_CHOICES = [
        (STATUS_PENDING,   "Pending"),
        (STATUS_RUNNING,   "Running"),
        (STATUS_COMPLETED, "Completed"),
        (STATUS_FAILED,    "Failed"),
    ]

    job_name     = models.CharField(max_length=100)
    payload      = models.JSONField(default=dict)
    run_at       = models.DateTimeField()
    status       = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING)
    is_deleted   = models.BooleanField(default=False)
    attempts     = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=3)
    result       = models.JSONField(null=True, blank=True)
    error        = models.TextField(null=True, blank=True)
    started_at   = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at   = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pyro_job"

    def __str__(self):
        return f"{self.job_name} | {self.status} | attempts={self.attempts}/{self.max_attempts}"
