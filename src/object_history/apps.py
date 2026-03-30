from django.apps import AppConfig


class ObjectHistoryConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "object_history"
    verbose_name = "Object History"

    def ready(self):
        try:
            from . import registrations  # noqa: F401
        except ImportError:
            pass
        return super().ready()
