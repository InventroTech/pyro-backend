from __future__ import annotations

from django.db.models.signals import post_save
from django.dispatch import receiver

from crm_records.models import Record

from .broadcast import broadcast_record_updated

REALTIME_ENTITY_TYPES = frozenset({"lead", "support_ticket"})


@receiver(post_save, sender=Record, dispatch_uid="realtime_broadcast_record_updated")
def notify_record_updated(sender, instance: Record, created: bool, **kwargs) -> None:
    if kwargs.get("raw", False):
        return
    if instance.entity_type not in REALTIME_ENTITY_TYPES:
        return
    broadcast_record_updated(instance, created=created)
