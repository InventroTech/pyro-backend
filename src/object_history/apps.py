from django.apps import AppConfig


class ObjectHistoryConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "object_history"
    verbose_name = "Object History"

    def ready(self):
        # Import registrations so register() calls run on startup.
        from . import registrations  # noqa: F401

        return super().ready()
from django.apps import AppConfig


class ObjectHistoryConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "object_history"
    verbose_name = "Object History"

    def ready(self):
        # Lazily import registry so registrations run at startup without
        # introducing hard import requirements during migrations.
        try:
            from . import registrations  # noqa: F401
        except ImportError:
            # Registrations file may not exist during partial deployments.
            pass