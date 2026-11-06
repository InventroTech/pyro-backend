"""
Deleted-users log (who removed a user, and when).

Wraps the existing delete flow instead of changing it:
DELETE /accounts/delete-user-with-log/ snapshots the user, calls the unchanged
delete_user_everywhere, then stores a DeletedUserLog row for each membership it
actually soft-deleted. GET /accounts/deleted-users/ lists those rows only.
"""

from __future__ import annotations

import logging

from django.db import DatabaseError, transaction
from django.db.models import Q
from django.utils import timezone
from rest_framework import serializers, status
from rest_framework.response import Response
from rest_framework.views import APIView

from accounts.models import DeletedUserLog
from accounts.serializers import DeleteUserEverywhereSerializer
from accounts.services.delete_user_everywhere import delete_user_everywhere
from authz.models import TenantMembership
from authz.permissions import IsTenantAuthenticated
from realtime.broadcast import broadcast_to_tenant

logger = logging.getLogger(__name__)

# Realtime clients already relay "record_updated"; existing listeners filter on
# lead / support_ticket, so this entity type only reaches the deleted-users list.
REALTIME_ENTITY_TYPE = "tenant_membership"


def _broadcast_user_deleted(tenant_id, membership_ids: list[int]) -> None:
    for membership_id in membership_ids:
        broadcast_to_tenant(
            tenant_id,
            {
                "event": "record_updated",
                "entity_type": REALTIME_ENTITY_TYPE,
                "record_id": f"{REALTIME_ENTITY_TYPE}:{membership_id}",
                "created": False,
                "data": {"deleted": True},
            },
        )


def _actor(request) -> str:
    user = getattr(request, "user", None)
    return str(getattr(user, "email", None) or getattr(user, "supabase_uid", None) or "")


def _candidate_memberships(*, tenant, uid, email):
    """The live memberships delete_user_everywhere can remove for this request."""
    match = Q()
    if email:
        match |= Q(email=email)
    if uid:
        match |= Q(user_id=uid)
    if not match:
        return []
    return list(
        TenantMembership.objects.select_related("role", "user_parent_id").filter(tenant=tenant).filter(match)
    )


def _snapshot(membership) -> dict:
    manager = membership.user_parent_id
    return {
        "membership_id": membership.id,
        "name": membership.name or "",
        "email": membership.email,
        "role_id": membership.role_id,
        "role_name": membership.role.name if membership.role_id else "",
        "manager_name": (manager.name or "") if manager else "",
        "manager_email": manager.email if manager else "",
        "account_created_at": membership.created_at,
    }


def _save_delete_logs(rows: list[DeletedUserLog]) -> list[DeletedUserLog]:
    """
    Save the log rows in their own savepoint. If the log table is missing or the
    write fails, the delete still goes through and only the log is skipped.
    """
    if not rows:
        return []
    try:
        with transaction.atomic():
            return DeletedUserLog.objects.bulk_create(rows)
    except DatabaseError:
        logger.exception(
            "[DeleteUserWithLog] Could not save deleted-user log for %s; the user was still deleted",
            [row.email for row in rows],
        )
        return []


def _logged_deletions(tenant) -> list[DeletedUserLog]:
    """Log rows for the tenant, newest first, or none if the log table can't be read yet."""
    try:
        with transaction.atomic():
            return list(DeletedUserLog.objects.filter(tenant=tenant).order_by("-deleted_at", "-id"))
    except DatabaseError:
        logger.exception("[DeletedUsersList] Could not read deleted-user log; returning an empty list")
        return []


class DeleteUserWithLogView(APIView):
    """
    DELETE /accounts/delete-user-with-log/
    Body: same as /accounts/delete-user/.
    """

    permission_classes = [IsTenantAuthenticated]

    def delete(self, request):
        try:
            ser = DeleteUserEverywhereSerializer(data=request.data, context={"request": request})
            ser.is_valid(raise_exception=True)

            tenant = ser.validated_data["_tenant"]
            uid = ser.validated_data.get("uid")
            email = ser.validated_data.get("email")
            role_id = ser.validated_data.get("role_id")
            deleted_by = _actor(request)

            with transaction.atomic():
                snapshots = {m.id: _snapshot(m) for m in _candidate_memberships(tenant=tenant, uid=uid, email=email)}
                report = delete_user_everywhere(tenant=tenant, uid=uid, email=email, role_id=role_id)
                removed = TenantMembership.all_objects.filter(id__in=list(snapshots), is_deleted=True)
                now = timezone.now()
                logs = _save_delete_logs(
                    [
                        DeletedUserLog(
                            tenant=tenant,
                            deleted_at=membership.deleted_at or now,
                            deleted_by=deleted_by,
                            **snapshots[membership.id],
                        )
                        for membership in removed
                    ]
                )
                if logs:
                    deleted_ids = [log.membership_id for log in logs]
                    transaction.on_commit(lambda: _broadcast_user_deleted(tenant.id, deleted_ids))

            return Response(
                {
                    "success": True,
                    "matched_by": report.get("matched_by"),
                    "tenant_id": report.get("tenant_id"),
                    "resolved_uid": report.get("resolved_uid"),
                    "deleted": report.get("deleted"),
                    "memberships_deactivated": report.get("memberships_deactivated", 0),
                    "reports_reassigned": report.get("reports_reassigned", 0),
                    "sessions_revoked": report.get("sessions_revoked", []),
                    "notes": report.get("notes", []),
                    "logged": len(logs),
                },
                status=status.HTTP_200_OK,
            )
        except serializers.ValidationError as exc:
            return Response({"success": False, "error": exc.detail}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as exc:
            logger.error("DeleteUserWithLogView error: %s", exc, exc_info=True)
            return Response(
                {"success": False, "error": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


def _log_row(log: DeletedUserLog) -> dict:
    return {
        "id": f"log-{log.id}",
        "name": log.name,
        "email": log.email,
        "role_id": str(log.role_id) if log.role_id else None,
        "role_name": log.role_name or None,
        "manager_name": log.manager_name or None,
        "manager_email": log.manager_email or None,
        "created_at": log.account_created_at,
        "deleted_at": log.deleted_at,
        "deleted_by": log.deleted_by or None,
    }


class DeletedUsersListView(APIView):
    """
    GET /accounts/deleted-users/ — users removed through delete-user-with-log, newest first.

    Memberships soft-deleted before the log existed are not listed: many tenants have
    repeat deletes and re-added users there, which made the list look like everyone.
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        rows = [_log_row(log) for log in _logged_deletions(request.tenant)]
        return Response({"count": len(rows), "results": rows}, status=status.HTTP_200_OK)
