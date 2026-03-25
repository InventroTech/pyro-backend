"""
Resolve record.data['assigned_to'] (UUID, email, or Supabase UID string) to a human-readable label.

1) TenantMembership.user_id (UUID) or .email
2) authentication.User.supabase_uid -> TenantMembership by that user's email (when membership.user_id is still null)
3) authentication.User.supabase_uid -> User.email (no membership)
"""
from __future__ import annotations

import uuid as uuid_module
from typing import Dict, Iterable, List, Optional, Set

from django.db.models import Q

from authz.models import TenantMembership


def _is_empty_assigned(value) -> bool:
    if value is None:
        return True
    s = str(value).strip()
    if not s:
        return True
    return s.lower() in ("null", "none")


def _display_from_membership(m: TenantMembership, fallback: str) -> str:
    name = (m.name or "").strip()
    if name:
        return name
    return (m.email or "").strip() or fallback


def _collect_lookup_keys(identifiers: Set[str]) -> tuple[List[uuid_module.UUID], List[str]]:
    uuid_ids: List[uuid_module.UUID] = []
    emails: List[str] = []
    for s in identifiers:
        try:
            uuid_ids.append(uuid_module.UUID(str(s).strip()))
        except (ValueError, AttributeError):
            if "@" in s:
                emails.append(s.strip().lower())
    return uuid_ids, emails


def _membership_lookup_maps(memberships: List[TenantMembership]) -> tuple[Dict[str, TenantMembership], Dict[str, TenantMembership]]:
    by_user_id: Dict[str, TenantMembership] = {}
    by_email: Dict[str, TenantMembership] = {}
    for m in memberships:
        if m.user_id:
            by_user_id[str(m.user_id)] = m
        by_email[m.email.lower()] = m
    return by_user_id, by_email


def _resolve_from_maps(
    raw: str,
    by_user_id: Dict[str, TenantMembership],
    by_email: Dict[str, TenantMembership],
) -> Optional[TenantMembership]:
    try:
        u = uuid_module.UUID(raw)
        return by_user_id.get(str(u))
    except (ValueError, AttributeError):
        return by_email.get(raw.lower())


def build_assigned_to_display_map(tenant, identifiers: Iterable[str]) -> Dict[str, str]:
    """
    Map each non-empty assigned_to string to a display label for the given tenant.
    """
    ids: Set[str] = {str(x).strip() for x in identifiers if not _is_empty_assigned(x)}
    if not ids or tenant is None:
        return {}

    uuid_ids, emails = _collect_lookup_keys(ids)
    q_parts: List[Q] = []
    if uuid_ids:
        q_parts.append(Q(user_id__in=uuid_ids))
    if emails:
        q_parts.append(Q(email__in=emails))

    memberships: List[TenantMembership] = []
    if q_parts:
        combined = q_parts[0]
        for p in q_parts[1:]:
            combined |= p
        memberships = list(TenantMembership.objects.filter(tenant=tenant).filter(combined))

    by_user_id, by_email = _membership_lookup_maps(memberships)

    out: Dict[str, str] = {}
    unresolved: List[str] = []
    for raw in ids:
        m = _resolve_from_maps(raw, by_user_id, by_email)
        if m:
            out[raw] = _display_from_membership(m, raw)
        else:
            unresolved.append(raw)

    if not unresolved:
        return out

    # Leads store supabase_uid while TenantMembership may only have email (user_id still null).
    from authentication.models import User

    uid_like = [r for r in unresolved if "@" not in r]
    if uid_like:
        auth_users = list(User.objects.filter(supabase_uid__in=uid_like))
        by_supa = {u.supabase_uid: u for u in auth_users}

        extra_emails: List[str] = []
        for u in auth_users:
            if u.email:
                extra_emails.append(u.email.strip().lower())

        extra_by_email: Dict[str, TenantMembership] = {}
        if extra_emails:
            for m in TenantMembership.objects.filter(tenant=tenant, email__in=extra_emails):
                extra_by_email.setdefault(m.email.lower(), m)

        for raw in uid_like:
            if raw in out:
                continue
            u = by_supa.get(raw)
            if not u:
                out[raw] = raw
                continue
            email_norm = (u.email or "").strip().lower()
            m = extra_by_email.get(email_norm) if email_norm else None
            if m:
                out[raw] = _display_from_membership(m, raw)
            elif u.email:
                out[raw] = u.email.strip()
            else:
                out[raw] = raw

    for raw in unresolved:
        if raw not in out:
            out[raw] = raw

    return out


def build_assigned_to_search_q(tenant, search_term: str) -> Q:
    """
    For global search: match leads where data.assigned_to contains the term OR the assignee's
    TenantMembership name/email matches (then exact-match stored assigned_to identifiers).
    """
    term = (search_term or "").strip()
    if not term:
        return Q(pk__in=[])

    q = Q(data__assigned_to__icontains=term)
    if tenant is None:
        return q

    ms = TenantMembership.objects.filter(tenant=tenant).filter(
        Q(name__icontains=term) | Q(email__icontains=term)
    )
    identifiers: Set[str] = set()
    membership_emails: List[str] = []
    for m in ms:
        if m.user_id:
            identifiers.add(str(m.user_id))
        if m.email:
            e = m.email.strip()
            identifiers.add(e)
            identifiers.add(e.lower())
            membership_emails.append(m.email.lower())

    if membership_emails:
        from authentication.models import User

        for u in User.objects.filter(email__in=list(set(membership_emails))):
            if u.supabase_uid:
                identifiers.add(str(u.supabase_uid).strip())

    identifiers = {i for i in identifiers if i}
    if identifiers:
        q |= Q(data__assigned_to__in=list(identifiers))
    return q
