from django.urls import path
from .views import CopyScriptView

urlpatterns = [
    path('run-script/', CopyScriptView.as_view(), name='run_python_script'),
] 