import logging
from typing import Optional, Tuple

from django.core.cache import cache
from django.db import transaction
from django.db.models import Q

from accounts.models import SupabaseAuthUser
from accounts.services.supabase_session import revoke_supabase_sessions_globally
from authz.models import TenantMembership, Role as AuthZRole
from authz.service import drop_permissions_cache

logger = logging.getLogger(__name__)

class DeleteReport(dict):
    """
    dict with stable keys:
    {
        'tenant_id': str,
        'resolved_uid': <uuid or None>,
        'matched_by': 'uid' | 'email_role' | 'none',
        'deleted': {
            'auth_users': int,
            'legacy_users': int,
            'tenant_memberships': int
        },
        'memberships_deactivated': int,
        'sessions_revoked': [ { user_id, revoked, ... }, ... ],
        'notes': [ ... ]
    }
    """
    pass


def _resolve_uid_from_email_role(tenant, email: str, role_id) -> Tuple[Optional[str], list]:
    """
    Find uid using (tenant, email, role) via TenantMembership and AuthZ Role only.
    """
    notes = []

    authz_role_id = None
    if role_id and AuthZRole.objects.filter(id=role_id, tenant=tenant).exists():
        authz_role_id = role_id

    tm_uid = (
        TenantMembership.objects
        .filter(tenant=tenant, email=email)
        .filter(role_id=authz_role_id) if authz_role_id else
        TenantMembership.objects.filter(tenant=tenant, email=email)
    ).values_list("user_id", flat=True).exclude(user_id__isnull=True).first()

    if tm_uid:
        notes.append(f"Resolved uid from TenantMembership: {tm_uid}")
        return str(tm_uid), notes

    notes.append("Could not resolve uid from (email, role_id).")
    return None, notes


def _build_membership_queryset(*, tenant, resolved_uid=None, email=None):
    tm_q = TenantMembership.objects.filter(tenant=tenant)
    if resolved_uid:
        tm_q = tm_q.filter(Q(user_id=resolved_uid) | Q(email=email) if email else Q(user_id=resolved_uid))
    elif email:
        tm_q = tm_q.filter(email=email)
    return tm_q


def _force_logout_and_clear_caches(*, tenant, resolved_uid=None, email=None) -> list[dict]:
    """
    Revoke Supabase sessions globally and clear permission/tenant caches for
  every uid tied to memberships about to be deleted.
    """
    tm_q = _build_membership_queryset(tenant=tenant, resolved_uid=resolved_uid, email=email)
    uids_to_revoke: set[str] = set()
    for user_id, membership_tenant_id in tm_q.values_list("user_id", "tenant_id"):
        if not user_id:
            continue
        uid_str = str(user_id)
        uids_to_revoke.add(uid_str)
        drop_permissions_cache(uid_str, membership_tenant_id or tenant)
        cache.delete(f"tenant:sub:{uid_str}")

    if resolved_uid:
        uids_to_revoke.add(str(resolved_uid))
        cache.delete(f"tenant:sub:{resolved_uid}")
        drop_permissions_cache(str(resolved_uid), tenant)

    results = []
    for uid in uids_to_revoke:
        results.append(revoke_supabase_sessions_globally(uid))
    return results


@transaction.atomic
def delete_user_everywhere(*, tenant, uid=None, email=None, role_id=None):
    """
    Deletes rows for a user across:
      - Supabase Auth sessions (global sign-out on all devices)
      - auth.users (Supabase)
      - public.authz_tenantmembership (TenantMembership)

    Idempotent: if nothing exists, returns '0' in counts.
    """
    report = DeleteReport(
        tenant_id=str(getattr(tenant, "id", None)),
        resolved_uid=None,
        matched_by="none",
        deleted={
            "auth_users": 0,
            "legacy_users": 0,
            "tenant_memberships": 0
        },
        memberships_deactivated=0,
        sessions_revoked=[],
        notes=[]
    )

    # 1) Resolve uid if needed
    resolved_uid = None
    if uid:
        resolved_uid = str(uid)
        report["matched_by"] = "uid"
    elif email and role_id:
        resolved_uid, notes = _resolve_uid_from_email_role(tenant, email, role_id)
        report["notes"].extend(notes)
        report["matched_by"] = "email_role"

    report["resolved_uid"] = resolved_uid

    # 2) Force logout everywhere + clear caches before DB deletes
    report["sessions_revoked"] = _force_logout_and_clear_caches(
        tenant=tenant,
        resolved_uid=resolved_uid,
        email=email,
    )

    # 3) Deactivate + unlink TenantMembership rows, then soft-delete.
    tm_q = _build_membership_queryset(tenant=tenant, resolved_uid=resolved_uid, email=email)
    report["memberships_deactivated"] = tm_q.update(is_active=False, user_id=None)
    tm_deleted, _ = tm_q.delete()
    report["deleted"]["tenant_memberships"] = tm_deleted
    logger.info(
        "TenantMembership deactivated and deleted",
        extra={
            "deactivated": report["memberships_deactivated"],
            "deleted": tm_deleted,
            "tenant_id": str(tenant.id),
            "email": email,
            "uid": resolved_uid,
        },
    )

    # 4) Delete from auth.users when we have a uid
    if resolved_uid:
        au_deleted, _ = SupabaseAuthUser.objects.filter(id=resolved_uid).delete()
        report["deleted"]["auth_users"] = au_deleted
        logger.info("auth.users deleted", extra={"count": au_deleted, "tenant_id": str(tenant.id), "uid": resolved_uid})

    if not resolved_uid and email and report["deleted"]["auth_users"] == 0:
        report["notes"].append("Skipped deleting auth.users by email-only for safety (cross-tenant risk).")

    return report
