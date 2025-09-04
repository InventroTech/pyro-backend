from django.urls import path
from accounts.views import LegacyUserCreateView, AssigneesByRoleView

urlpatterns = [
    path("users/legacy/create/", LegacyUserCreateView.as_view(), name="legacy-user-create"),
    path('users/assignees-by-role/', AssigneesByRoleView.as_view(), name='assignees-by-role'),
    
]
