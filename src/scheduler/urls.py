from django.urls import path
from .views import ScheduleCallView, RecordOutcomeView

app_name = "scheduler"
urlpatterns = [
    path("schedule/call/", ScheduleCallView.as_view(), name="schedule-call"),
    path("tasks/<int:task_id>/outcome/", RecordOutcomeView.as_view(), name="record-outcome"),
]