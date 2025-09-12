from django.urls import path
from .views import LinkUserUidView, LegacyUserCreateView, AssigneesByRoleView

urlpatterns = [
    path("users/legacy/create/", LegacyUserCreateView.as_view(), name="legacy-user-create"),
    path('users/assignees-by-role/', AssigneesByRoleView.as_view(), name='assignees-by-role'),
    path("link-user-uid/", LinkUserUidView.as_view(), name="link_user_ uid"),

]