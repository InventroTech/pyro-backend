"""
Hour threshold and last sign-in time for the settings user list.

Nothing here turns a membership off. Supabase stores the time on
auth.users.last_sign_in_at. Django auth (AUTH_PROVIDER=django) stores it on
authentication.User.last_login.
"""
from __future__ import annotations

from datetime import datetime

from django.conf import settings
from django.db.models import Q
from django.db.models.functions import Lower

from accounts.models import SupabaseAuthUser
from authentication.models import User
from core.models import TenantSettings

DEFAULT_INACTIVE_LOGIN_HOURS = 72
MIN_INACTIVE_LOGIN_HOURS = 1
MAX_INACTIVE_LOGIN_HOURS = 8760


def _uses_django_login() -> bool:
    return getattr(settings, "AUTH_PROVIDER", "supabase") == "django"


def inactive_email_policy(tenant) -> dict:
    """Current policy for the settings page. Missing rows use the 72-hour default."""
    row = None
    if tenant is not None and getattr(tenant, "pk", None) is not None:
        row = TenantSettings.objects.filter(pk=tenant.pk).first()
    if row is None:
        return {
            "enabled": True,
            "inactive_after_hours": DEFAULT_INACTIVE_LOGIN_HOURS,
        }
    hours = row.inactive_login_hours
    if not isinstance(hours, int) or hours < MIN_INACTIVE_LOGIN_HOURS:
        hours = DEFAULT_INACTIVE_LOGIN_HOURS
    if hours > MAX_INACTIVE_LOGIN_HOURS:
        hours = MAX_INACTIVE_LOGIN_HOURS
    return {
        "enabled": bool(row.inactive_emails_enabled),
        "inactive_after_hours": hours,
    }


def save_inactive_email_policy(tenant, *, enabled=None, inactive_after_hours=None) -> dict:
    row, _created = TenantSettings.objects.get_or_create(tenant=tenant)
    update_fields = []
    if enabled is not None:
        row.inactive_emails_enabled = bool(enabled)
        update_fields.append("inactive_emails_enabled")
    if inactive_after_hours is not None:
        row.inactive_login_hours = int(inactive_after_hours)
        update_fields.append("inactive_login_hours")
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
