import logging
from typing import Optional, Tuple

from django.db import transaction, connection
from django.db.models import Q

from accounts.models import LegacyUser, SupabaseAuthUser, LegacyRole
from authz.models import TenantMembership, Role as AuthZRole
from authz.service import get_authz_role_from_legacy_role  # you already have this helper

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
        'notes': [ ... ]
    }
    """
    pass


def _resolve_uid_from_email_role(tenant, email: str, role_id) -> Tuple[Optional[str], list]:
    """
    Attempt to find uid using (tenant, email, role), supporting either legacy or authz role ids.
    Strategy:
      1) Try LegacyUser rows (public.users) for (tenant,email[,legacy_role_id]) -> uid
      2) Try TenantMembership (authz) for (tenant,email[,authz_role_id]) -> user_id
      3) If still no uid, return None (it may never have been linked in auth.users)
    """
    notes = []

    # 1) Legacy path: role_id provided may be legacy role id (public.roles.id)
    legacy_uid = (
        LegacyUser.objects
        .filter(tenant=tenant, email__iexact=email)
        .filter(Q(role_id=role_id) | Q(role_id__isnull=True) | Q(role_id__isnull=False))  # accept any role if unsure
        .values_list("uid", flat=True)
        .exclude(uid__isnull=True)
        .first()
    )
    if legacy_uid:
        notes.append(f"Resolved uid from LegacyUser: {legacy_uid}")
        return str(legacy_uid), notes

    # 2) AuthZ path: role_id may be an AuthZ role id; if it’s actually legacy, map it.
    authz_role_id = None
    try:
        # Try direct assume-as-authz first
        if AuthZRole.objects.filter(id=role_id, tenant=tenant).exists():
            authz_role_id = role_id
        else:
            # Attempt to map legacy->authz
            mapped = get_authz_role_from_legacy_role(role_id, tenant)
            if isinstance(mapped, AuthZRole):
                authz_role_id = mapped.id
    except Exception as e:
        notes.append(f"Role resolution warning: {e!r}")

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


@transaction.atomic
def delete_user_everywhere(*, tenant, uid=None, email=None, role_id=None):
    """
    Deletes rows for a user across:
      - auth.users (Supabase)  -> will cascade delete public.users via FK (uid) if present
      - public.users (LegacyUser) -> explicit delete for rows not linked by FK
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

    # 2) Delete TenantMembership first (no FK). Scope strictly by tenant.
    tm_q = TenantMembership.objects.filter(tenant=tenant)
    if resolved_uid:
        tm_q = tm_q.filter(Q(user_id=resolved_uid) | Q(email=email) if email else Q(user_id=resolved_uid))
    elif email:
        tm_q = tm_q.filter(email=email)

    tm_deleted, _ = tm_q.delete()
    report["deleted"]["tenant_memberships"] = tm_deleted
    logger.info("TenantMembership deleted", extra={"count": tm_deleted, "tenant_id": str(tenant.id), "email": email, "uid": resolved_uid})

    # 3) Delete from auth.users when we have a uid
    # NOTE: Deleting here will cascade LegacyUser via the FK (public.users.uid -> auth.users.id ON DELETE CASCADE)
    if resolved_uid:
        au_deleted, _ = SupabaseAuthUser.objects.filter(id=resolved_uid).delete()
        report["deleted"]["auth_users"] = au_deleted
        logger.info("auth.users deleted", extra={"count": au_deleted, "tenant_id": str(tenant.id), "uid": resolved_uid})

    # 4) Clean up any leftover LegacyUser rows (e.g., if no uid or legacy rows without uid)
    lu_q = LegacyUser.objects.filter(tenant=tenant)
    if resolved_uid:
        lu_q = lu_q.filter(Q(uid=resolved_uid) | Q(email=email) if email else Q(uid=resolved_uid))
    elif email:
        # best effort: limit by role if provided (legacy role id)
        if role_id:
            lu_q = lu_q.filter(email__iexact=email, role_id=role_id)
        else:
            lu_q = lu_q.filter(email__iexact=email)

    lu_deleted, _ = lu_q.delete()
    report["deleted"]["legacy_users"] = lu_deleted
    logger.info("public.users deleted", extra={"count": lu_deleted, "tenant_id": str(tenant.id), "email": email, "uid": resolved_uid})

    # 5) If no uid and caller still wants to remove auth.users by email:
    #     This is DANGEROUS cross-tenant; we *do not* do it automatically.
    if not resolved_uid and email and report["deleted"]["auth_users"] == 0:
        report["notes"].append("Skipped deleting auth.users by email-only for safety (cross-tenant risk).")

    return report
