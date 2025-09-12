from django.urls import path
from .views import LinkUserUidView

urlpatterns = [
    path("link-user-uid/", LinkUserUidView.as_view(), name="link_user_uid"),
]