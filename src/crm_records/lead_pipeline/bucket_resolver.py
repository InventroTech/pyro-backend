from __future__ import annotations

from dataclasses import dataclass
from typing import List

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


class BucketResolver:
    """
    Loads bucket assignments ordered by priority.

    Prefers **tenant-wide** rows (``user`` is NULL). If none exist, falls back to
    per-``TenantMembership`` rows (legacy).
    """

    CACHE_TTL_SECONDS = 300

    def resolve(self, tenant, user: ResolvedUser) -> List[BucketAssignmentView]:
        membership = user.membership
        if not membership:
            return []

        tenant_key = f"bucket_assignments_tenant:{tenant.id}:v4"
        legacy_key = f"bucket_assignments:{tenant.id}:{membership.id}:v2"

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
            result = self._to_views(tenant_qs)
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
        result = self._to_views(legacy_qs)
        cache.set(legacy_key, result, self.CACHE_TTL_SECONDS)
        return result

    @staticmethod
    def _to_views(assignments) -> List[BucketAssignmentView]:
        return [
            BucketAssignmentView(
                pk=a.pk,
                bucket_slug=a.bucket.slug,
                priority=a.priority,
                pull_strategy=dict(a.pull_strategy or {}),
                filter_conditions=dict(a.bucket.filter_conditions or {}),
            )
            for a in assignments
        ]

