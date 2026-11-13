"""
Hour threshold and last sign-in time for the settings user list.

Nothing here turns a membership off. The sign-in time is
authentication.User.last_login, set from the token's sign-in time on each
authenticated request (config.supabase_auth).
"""
from __future__ import annotations

from datetime import datetime

from django.db.models import F, Q
from django.db.models.functions import Lower

from authentication.models import User
from core.models import TenantSettings

DEFAULT_INACTIVE_LOGIN_HOURS = 72
MIN_INACTIVE_LOGIN_HOURS = 1
MAX_INACTIVE_LOGIN_HOURS = 8760


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


def _django_users_for_memberships(memberships) -> dict[int, User | None]:
    """
    Match each membership to authentication.User by supabase uid, then by email.
    When several users share an email, the one that signed in most recently wins.
    """
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
        rows = list(
            User.objects.annotate(email_lower=Lower("email"))
            .filter(query)
            .order_by(F("last_login").desc(nulls_last=True), "-pk")
        )
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


def last_sign_in_at_by_membership_id(memberships) -> dict[int, datetime | None]:
    """Last sign-in (User.last_login) for the settings user list. Missing users are null."""
    matched = _django_users_for_memberships(memberships)
    return {
        membership.id: user.last_login if (user := matched.get(membership.id)) else None
        for membership in memberships
    }
