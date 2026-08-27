from django.apps import AppConfig


class EmailProtocolConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'email_protocol'

    def ready(self):
        # Ensure models are imported for migrations / admin discovery.
        from . import models  # noqa: F401
