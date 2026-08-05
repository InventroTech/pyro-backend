import os
from django.apps import AppConfig


class PyroJobsConfig(AppConfig):
    name = "pyro_jobs"
    verbose_name = "Pyro Jobs"

    def ready(self):
        import sys
        if "pytest" in sys.modules or "test" in sys.argv:
            return
        if os.environ.get("RUN_MAIN") == "true" or os.environ.get("DJANGO_ENV") != "development":
            from pyro_jobs.pyro_job_creator import start_pyro_job_creator
            from pyro_jobs.pyro_job_executor import start_pyro_job_executor
            start_pyro_job_creator()
            start_pyro_job_executor()
