from django.urls import path
from .views import CopyScriptView, UnassignSnoozedLeadsCronView, ReleaseLeadsAfter12hCronView

urlpatterns = [
    path('run-script/', CopyScriptView.as_view(), name='run_python_script'),
    path('unassign-snoozed-leads/', UnassignSnoozedLeadsCronView.as_view(), name='unassign_snoozed_leads'),
    path('release-leads-after-12h/', ReleaseLeadsAfter12hCronView.as_view(), name='release_leads_after_12h'),
] 