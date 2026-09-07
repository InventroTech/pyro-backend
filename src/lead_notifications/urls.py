from django.urls import path

from .views import InAppNotificationListView, InAppNotificationMarkReadView

app_name = "lead_notifications"

urlpatterns = [
    path("", InAppNotificationListView.as_view(), name="list"),
    path("<int:pk>/read/", InAppNotificationMarkReadView.as_view(), name="mark-read"),
]
