from __future__ import annotations

import json
import logging
import select
import threading
import time

import psycopg2
import psycopg2.extensions
from django.conf import settings

logger = logging.getLogger(__name__)

PG_NOTIFY_CHANNEL = "pyro_record_changed"
REALTIME_ENTITY_TYPES = frozenset({"lead", "support_ticket"})

_listener_started = False
_listener_lock = threading.Lock()


def _listen_port() -> int:
    """Session-mode port for LISTEN (Supabase txn pooler 6543 does not relay NOTIFY)."""
    explicit = getattr(settings, "REALTIME_PG_NOTIFY_PORT", None)
    if explicit:
        return int(explicit)

    db_port = int(settings.DATABASES["default"].get("PORT") or 5432)
    if db_port == 6543:
        return 5432
    return db_port


def _connection_params() -> dict:
    db = settings.DATABASES["default"]
    params = {
        "dbname": db["NAME"],
        "user": db["USER"],
        "password": db["PASSWORD"],
        "host": db["HOST"],
        "port": _listen_port(),
    }
    sslmode = (db.get("OPTIONS") or {}).get("sslmode")
    if sslmode:
        params["sslmode"] = sslmode
    return params


def _handle_payload(payload: str) -> None:
    from crm_records.models import Record

    from .broadcast import broadcast_record_updated

    data = json.loads(payload)
    record_id = data.get("id")
    entity_type = data.get("entity_type")
    if not record_id or entity_type not in REALTIME_ENTITY_TYPES:
        return

    logger.info(
        "Postgres NOTIFY %s record_id=%s entity_type=%s",
        PG_NOTIFY_CHANNEL,
        record_id,
        entity_type,
    )

    record = Record.objects.filter(pk=record_id).first()
    if not record:
        logger.warning("NOTIFY for unknown record_id=%s", record_id)
        return

    broadcast_record_updated(record)


def _listen_loop() -> None:
    while True:
        conn = None
        db = settings.DATABASES["default"]
        listen_host = db["HOST"]
        listen_port = _listen_port()
        try:
            conn = psycopg2.connect(**_connection_params())
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(f"LISTEN {PG_NOTIFY_CHANNEL};")
            logger.info(
                "Listening for database changes on %s:%s channel %s",
                listen_host,
                listen_port,
                PG_NOTIFY_CHANNEL,
            )

            while True:
                if select.select([conn], [], [], 60) == ([], [], []):
                    continue
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    try:
                        _handle_payload(notify.payload)
                    except Exception:
                        logger.exception("Failed to handle database notify payload")
        except Exception:
            logger.exception("PostgreSQL listener error; reconnecting in 5s")
            time.sleep(5)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def start_pg_listener() -> None:
    """Start a background thread that pushes record changes from Postgres NOTIFY."""
    global _listener_started

    with _listener_lock:
        if _listener_started:
            return
        _listener_started = True

    thread = threading.Thread(
        target=_listen_loop,
        name="pyro-pg-record-listener",
        daemon=True,
    )
    thread.start()
