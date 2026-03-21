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
    Loads per-user bucket assignments ordered by priority.
    """

    CACHE_TTL_SECONDS = 300

    def resolve(self, tenant, user: ResolvedUser) -> List[BucketAssignmentView]:
        membership = user.membership
        if not membership:
            return []

        # Version suffix: cached views now include filter_conditions (invalidate old pickles).
        cache_key = f"bucket_assignments:{tenant.id}:{membership.id}:v2"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

        assignments = (
            UserBucketAssignment.objects.filter(
                tenant=tenant,
                user=membership,
                is_active=True,
                bucket__is_active=True,
            )
            .select_related("bucket")
            .order_by("priority")
        )

        result: List[BucketAssignmentView] = [
            BucketAssignmentView(
                pk=a.pk,
                bucket_slug=a.bucket.slug,
                priority=a.priority,
                pull_strategy=dict(a.pull_strategy or {}),
                filter_conditions=dict(a.bucket.filter_conditions or {}),
            )
            for a in assignments
        ]

        cache.set(cache_key, result, self.CACHE_TTL_SECONDS)
        return result

