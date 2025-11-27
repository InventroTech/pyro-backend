from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Type, Union

from django.apps import apps
from django.core.exceptions import ImproperlyConfigured
from django.db import models


SnapshotStrategy = str


@dataclass(frozen=True)
class HistoryConfig:
    model: Type[models.Model]
    track_fields: Iterable[str]
    redact_fields: Iterable[str] = field(default_factory=set)
    snapshot_strategy: SnapshotStrategy = "minimal"
    custom_serializer: Optional[Callable[[models.Model], Dict[str, Any]]] = None

    def __post_init__(self):
        allowed_strategies = {"minimal", "full", "custom"}
        if self.snapshot_strategy not in allowed_strategies:
            raise ImproperlyConfigured(
                f"snapshot_strategy must be one of {allowed_strategies}, "
                f"got {self.snapshot_strategy!r}"
            )

        if self.snapshot_strategy == "custom" and not callable(self.custom_serializer):
            raise ImproperlyConfigured(
                "custom snapshot_strategy requires a callable custom_serializer"
            )


class HistoryRegistry:
    """
    Central registry for tracked models.
    """

    def __init__(self):
        self._configs: Dict[str, HistoryConfig] = {}

    def _get_model_label(self, model: Union[Type[models.Model], str]) -> str:
        if isinstance(model, str):
            if "." not in model:
                raise ImproperlyConfigured(
                    "Model string must be 'app_label.ModelName'"
                )
            app_label, model_name = model.split(".", 1)
            model_cls = apps.get_model(app_label, model_name)
            if model_cls is None:
                raise ImproperlyConfigured(f"Model {model} could not be resolved")
            return model_cls._meta.label_lower
        return model._meta.label_lower

    def register(
        self,
        model: Union[Type[models.Model], str],
        *,
        track_fields: Iterable[str],
        redact_fields: Optional[Iterable[str]] = None,
        snapshot_strategy: SnapshotStrategy = "minimal",
        custom_serializer: Optional[Callable[[models.Model], Dict[str, Any]]] = None,
    ) -> HistoryConfig:
        model_label = self._get_model_label(model)
        model_cls = apps.get_model(model_label)

        config = HistoryConfig(
            model=model_cls,
            track_fields=list(track_fields),
            redact_fields=set(redact_fields or []),
            snapshot_strategy=snapshot_strategy,
            custom_serializer=custom_serializer,
        )
        self._configs[model_label] = config
        return config

    def get(self, model: Union[Type[models.Model], str]) -> Optional[HistoryConfig]:
        label = self._get_model_label(model)
        return self._configs.get(label)

    def is_tracked(self, model: Union[Type[models.Model], str]) -> bool:
        return self.get(model) is not None

    def tracked_models(self) -> List[str]:
        return sorted(self._configs.keys())


registry = HistoryRegistry()


def register(*args, **kwargs):
    return registry.register(*args, **kwargs)


def get_config(model: Union[Type[models.Model], str]) -> Optional[HistoryConfig]:
    return registry.get(model)


__all__ = ["HistoryConfig", "HistoryRegistry", "register", "get_config", "registry"]
