from django.urls import path
from .views import (
    JobQueueStatusView, JobDetailView, RetryJobView, 
    FailedJobsView, BulkRetryJobsView
)


urlpatterns = [
    path("status/", JobQueueStatusView.as_view(), name="job-queue-status"),
    path("<int:job_id>/", JobDetailView.as_view(), name="job-detail"),
    path("<int:job_id>/retry/", RetryJobView.as_view(), name="job-retry"),
    path("bulk-retry/", BulkRetryJobsView.as_view(), name="bulk-retry-jobs"),
    path("failed/", FailedJobsView.as_view(), name="failed-jobs"),
]


