from django.urls import path
from .views import RunPythonScriptView

urlpatterns = [
    path('run-script/', RunPythonScriptView.as_view(), name='run_python_script'),
] 