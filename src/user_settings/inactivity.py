"""
Mark a tenant email inactive after a stretch of days with no sign-in.

The settings page reads that time on the user list and edits the day threshold
here. Supabase stores it on auth.users.last_sign_in_at. Django auth
(AUTH_PROVIDER=django) stores it on authentication.User.last_login.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from django.conf import settings
from django.db.models import Q
from django.db.models.functions import Lower
from django.utils import timezone

from accounts.models import SupabaseAuthUser
from authentication.models import User
from authz.models import TenantMembership
from authz.service import drop_permissions_cache
from core.models import TenantSettings

logger = logging.getLogger(__name__)

DEFAULT_INACTIVE_LOGIN_DAYS = 72
MIN_INACTIVE_LOGIN_DAYS = 1
MAX_INACTIVE_LOGIN_DAYS = 3650


def _uses_django_login() -> bool:
    return getattr(settings, "AUTH_PROVIDER", "supabase") == "django"


def inactive_email_policy(tenant) -> dict:
    """Current policy for the settings page. Missing rows use the 72-day default."""
    row = None
    if tenant is not None and getattr(tenant, "pk", None) is not None:
        row = TenantSettings.objects.filter(pk=tenant.pk).first()
    if row is None:
        return {
            "enabled": True,
            "inactive_after_days": DEFAULT_INACTIVE_LOGIN_DAYS,
        }
    days = row.inactive_login_days
    if not isinstance(days, int) or days < MIN_INACTIVE_LOGIN_DAYS:
        days = DEFAULT_INACTIVE_LOGIN_DAYS
    if days > MAX_INACTIVE_LOGIN_DAYS:
        days = MAX_INACTIVE_LOGIN_DAYS
    return {
        "enabled": bool(row.inactive_emails_enabled),
        "inactive_after_days": days,
    }


def save_inactive_email_policy(tenant, *, enabled=None, inactive_after_days=None) -> dict:
    row, _created = TenantSettings.objects.get_or_create(tenant=tenant)
    update_fields = []
    if enabled is not None:
        row.inactive_emails_enabled = bool(enabled)
        update_fields.append("inactive_emails_enabled")
    if inactive_after_days is not None:
        row.inactive_login_days = int(inactive_after_days)
        update_fields.append("inactive_login_days")
    if update_fields:
        row.save(update_fields=update_fields)
    return inactive_email_policy(tenant)


def _auth_users_for_memberships(memberships) -> dict[int, SupabaseAuthUser | None]:
    """Match each membership to auth.users by user id, then by email."""
    if not memberships:
        return {}
    uids = [m.user_id for m in memberships if m.user_id]
    emails = [(m.email or "").strip().lower() for m in memberships if (m.email or "").strip()]
    query = Q()
    if uids:
        query |= Q(id__in=uids)
    if emails:
        query |= Q(email_lower__in=emails)
    rows = []
    if query:
        rows = list(SupabaseAuthUser.objects.annotate(email_lower=Lower("email")).filter(query))
    by_id = {row.id: row for row in rows}
    by_email: dict[str, SupabaseAuthUser] = {}
    for row in rows:
        key = (row.email or "").strip().lower()
        if key and key not in by_email:
            by_email[key] = row
    matched: dict[int, SupabaseAuthUser | None] = {}
    for membership in memberships:
        row = by_id.get(membership.user_id) if membership.user_id else None
        if row is None and membership.email:
            row = by_email.get(membership.email.strip().lower())
        matched[membership.id] = row
    return matched


def _django_users_for_memberships(memberships) -> dict[int, User | None]:
    """Match each membership to authentication.User by supabase uid, then by email."""
    if not memberships:
        return {}
    uids = [str(m.user_id) for m in memberships if m.user_id]
    emails = [(m.email or "").strip().lower() for m in memberships if (m.email or "").strip()]
    query = Q()
    if uids:
        query |= Q(supabase_uid__in=uids)
    if emails:
        query |= Q(email_lower__in=emails)
    rows = []
    if query:
        rows = list(User.objects.annotate(email_lower=Lower("email")).filter(query))
    by_uid = {row.supabase_uid: row for row in rows}
    by_email: dict[str, User] = {}
    for row in rows:
        key = (row.email or "").strip().lower()
        if key and key not in by_email:
            by_email[key] = row
    matched: dict[int, User | None] = {}
    for membership in memberships:
        row = by_uid.get(str(membership.user_id)) if membership.user_id else None
        if row is None and membership.email:
            row = by_email.get(membership.email.strip().lower())
        matched[membership.id] = row
    return matched


def _login_times(memberships) -> dict[int, tuple[datetime | None, datetime | None]]:
    """
    membership id -> (last sign-in, created-at anchor when there is no sign-in).

    Django auth reads User.last_login. Supabase reads auth.users.last_sign_in_at
    and falls back to auth.users.created_at.
    """
    if _uses_django_login():
        matched = _django_users_for_memberships(memberships)
        return {
            membership.id: (
                user.last_login if user is not None else None,
                membership.created_at,
            )
            for membership, user in ((m, matched.get(m.id)) for m in memberships)
        }

    matched = _auth_users_for_memberships(memberships)
    times: dict[int, tuple[datetime | None, datetime | None]] = {}
    for membership in memberships:
        row = matched.get(membership.id)
        if row is None:
            times[membership.id] = (None, membership.created_at)
        else:
            times[membership.id] = (row.last_sign_in_at, row.created_at or membership.created_at)
    return times


def last_sign_in_at_by_membership_id(memberships) -> dict[int, datetime | None]:
    """Last sign-in for the settings user list. Missing users are null."""
    return {
        membership_id: signed_in_at
        for membership_id, (signed_in_at, _anchor) in _login_times(memberships).items()
    }


def deactivate_inactive_emails(*, tenant) -> int:
    """
    Mark active memberships inactive when the last sign-in is at least
    inactive_after_days ago.

    An email that has never signed in is measured from auth.users.created_at
    on Supabase, or from the membership created_at on Django auth and when
    there is no auth row.
    """
    policy = inactive_email_policy(tenant)
    if not policy["enabled"]:
        return 0

    cutoff = timezone.now() - timedelta(days=policy["inactive_after_days"])
    memberships = list(TenantMembership.objects.filter(tenant=tenant, is_active=True))
    times = _login_times(memberships)
    count = 0
    for membership in memberships:
        signed_in_at, anchor = times.get(membership.id, (None, membership.created_at))
        reference = signed_in_at if signed_in_at is not None else anchor
        if reference is None or reference > cutoff:
            continue
        membership.is_active = False
        membership.save(update_fields=["is_active"])
        if membership.user_id:
            drop_permissions_cache(str(membership.user_id), tenant)
        count += 1
    if count:
        logger.info(
            "Marked %s email(s) inactive for tenant %s after %s days without a sign-in",
            count,
            tenant.pk,
            policy["inactive_after_days"],
        )
    return count
