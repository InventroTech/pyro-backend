from django.urls import path
from accounts.views import LegacyUserCreateView

urlpatterns = [
    path("users/legacy/create/", LegacyUserCreateView.as_view(), name="legacy-user-create"),
    
]
