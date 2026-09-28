from __future__ import annotations

from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver

from crm_records.models import Record

from .service import notify_lead_called_back, should_notify_lead_called_back

_previous_data_by_pk: dict[int, dict | None] = {}


@receiver(pre_save, sender=Record, dispatch_uid="notifications_cache_previous_data")
def cache_previous_data(sender, instance: Record, **kwargs) -> None:
    # Only leads can trigger call-back notifications — skip other entity types
    # so this module-level cache cannot grow unbounded.
    if not instance.pk or instance.entity_type != "lead":
        return
    previous = Record.objects.filter(pk=instance.pk).values_list("data", flat=True).first()
    _previous_data_by_pk[instance.pk] = previous if isinstance(previous, dict) else {}


@receiver(post_save, sender=Record, dispatch_uid="notifications_on_lead_saved")
def on_lead_saved(sender, instance: Record, created: bool, **kwargs) -> None:
    # Always pop first so a cached entry cannot leak if we return early.
    old_data = _previous_data_by_pk.pop(instance.pk, {} if created else None)

    if kwargs.get("raw", False):
        return
    if instance.entity_type != "lead":
        return

    if old_data is None and not created:
        old_data = {}

    new_data = instance.data if isinstance(instance.data, dict) else {}
    if not should_notify_lead_called_back(old_data, new_data):
        return

    notify_lead_called_back(instance)
