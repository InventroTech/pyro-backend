from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from django.core.cache import cache

from crm_records.models import UserBucketAssignment
from crm_records.lead_pipeline.user_resolver import ResolvedUser


@dataclass(frozen=True)
class BucketAssignmentView:
    pk: int
    bucket_slug: str
    priority: int
    pull_strategy: dict
    filter_conditions: dict


def _matches_entity_type(filter_conditions: dict, entity_type: Optional[str]) -> bool:
    """
    When ``entity_type`` is None, include all buckets.
    For ``lead``: include buckets with no entity_type (legacy sales) or entity_type=lead.
    For other types: require an exact match on filter_conditions.entity_type.
    """
    if entity_type is None:
        return True
    fc_type = (filter_conditions or {}).get("entity_type")
    if entity_type == "lead":
        return fc_type in (None, "", "lead")
    return fc_type == entity_type


class BucketResolver:
    """
    Loads bucket assignments ordered by priority.

    Prefers **tenant-wide** rows (``user`` is NULL). If none exist, falls back to
    per-``TenantMembership`` rows (legacy).

    Optional ``entity_type`` filters which buckets apply (sales vs support).
    """

    CACHE_TTL_SECONDS = 300

    def resolve(
        self,
        tenant,
        user: ResolvedUser,
        *,
        entity_type: Optional[str] = None,
    ) -> List[BucketAssignmentView]:
        membership = user.membership
        if not membership:
            return []

        entity_suffix = entity_type or "all"
        tenant_key = f"bucket_assignments_tenant:{tenant.id}:v5:{entity_suffix}"
        legacy_key = f"bucket_assignments:{tenant.id}:{membership.id}:v3:{entity_suffix}"

        cached = cache.get(tenant_key)
        if cached is not None:
            return cached

        tenant_qs = (
            UserBucketAssignment.objects.filter(
                tenant=tenant,
                user__isnull=True,
                is_active=True,
                bucket__is_active=True,
            )
            .select_related("bucket")
            .order_by("priority")
        )

        if tenant_qs.exists():
            result = self._to_views(tenant_qs, entity_type=entity_type)
            cache.set(tenant_key, result, self.CACHE_TTL_SECONDS)
            return result

        cached_legacy = cache.get(legacy_key)
        if cached_legacy is not None:
            return cached_legacy

        legacy_qs = (
            UserBucketAssignment.objects.filter(
                tenant=tenant,
                user=membership,
                is_active=True,
                bucket__is_active=True,
            )
            .select_related("bucket")
            .order_by("priority")
        )
        result = self._to_views(legacy_qs, entity_type=entity_type)
        cache.set(legacy_key, result, self.CACHE_TTL_SECONDS)
        return result

    @staticmethod
    def _to_views(assignments, *, entity_type: Optional[str] = None) -> List[BucketAssignmentView]:
        views: List[BucketAssignmentView] = []
        for a in assignments:
            fc = dict(a.bucket.filter_conditions or {})
            if not _matches_entity_type(fc, entity_type):
                continue
            views.append(
                BucketAssignmentView(
                    pk=a.pk,
                    bucket_slug=a.bucket.slug,
                    priority=a.priority,
                    pull_strategy=dict(a.pull_strategy or {}),
                    filter_conditions=fc,
                )
            )
        return views
