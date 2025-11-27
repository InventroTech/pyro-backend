"""
Object history app wiring.

Keeping imports lazy avoids circular dependencies during Django startup.
"""

default_app_config = "object_history.apps.ObjectHistoryConfig"


def register(*args, **kwargs):
    """
    Public helper so callers can do `from object_history import register`.
    """
    from .registry import register as registry_register  # local import

    return registry_register(*args, **kwargs)


__all__ = ["register"]

