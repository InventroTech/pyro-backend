from django.apps import AppConfig


class LeadNotificationsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "lead_notifications"
    verbose_name = "Lead notifications"

    def ready(self) -> None:
        import lead_notifications.signals  # noqa: F401
