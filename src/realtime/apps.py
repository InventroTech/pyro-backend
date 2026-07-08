import logging
import os
import sys

from django.apps import AppConfig

logger = logging.getLogger(__name__)


class RealtimeConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "realtime"
    verbose_name = "Realtime"

    def ready(self) -> None:
        import realtime.signals  # noqa: F401

        self._warn_if_pg_notify_trigger_missing()

        if len(sys.argv) > 1 and any(
            cmd in sys.argv
            for cmd in [
                "migrate",
                "makemigrations",
                "collectstatic",
                "shell",
                "test",
            ]
        ):
            return

        argv = " ".join(sys.argv)
        under_daphne = "daphne" in argv
        under_runasgi = "runasgi" in argv
        under_runserver_main = "runserver" in argv and os.environ.get("RUN_MAIN") == "true"

        if not under_daphne and not under_runasgi and not under_runserver_main:
            return

        try:
            from .pg_listener import start_pg_listener

            start_pg_listener()
        except Exception:
            logger.exception("Failed to start PostgreSQL realtime listener")

    @staticmethod
    def _warn_if_pg_notify_trigger_missing() -> None:
        try:
            from django.db import connection

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM pg_trigger WHERE tgname = %s LIMIT 1",
                    ["records_pyro_notify_change"],
                )
                if cursor.fetchone():
                    return
            logger.warning(
                "Realtime: direct SQL/Supabase table edits will NOT auto-update the UI "
                "until you run: python manage.py migrate crm_records 0037"
            )
        except Exception:
            logger.debug(
                "Could not verify pg_notify trigger records_pyro_notify_change",
                exc_info=True,
            )
