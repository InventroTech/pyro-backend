from django.urls import path

from .views import (
    PyroEnqueueJobView,
    PyroJobDetailView,
    PyroJobTypesView,
    PyroRerunJobView,
)


urlpatterns = [
    path("types/", PyroJobTypesView.as_view(), name="pyro-job-types"),
    path("enqueue/", PyroEnqueueJobView.as_view(), name="pyro-job-enqueue"),
    path("<int:job_id>/", PyroJobDetailView.as_view(), name="pyro-job-detail"),
    path("<int:job_id>/rerun/", PyroRerunJobView.as_view(), name="pyro-job-rerun"),
]
