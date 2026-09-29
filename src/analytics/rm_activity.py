"""
Writes rows into rm_activity_events — the single table the RM PRD analytics
dashboard reads from. Called once from RecordEventView.post() for every lead
event; it figures out on its own whether that event is one worth recording.

Deliberately fails silently (logs, never raises): this is a side-effect log
for a dashboard, not part of the actual lead-disposition flow. A bug here
must never stop an RM's call outcome from being saved.
"""

import logging

from django.utils import timezone

from analytics.models import RmActivityEvent
from authz.models import TenantMembership

logger = logging.getLogger(__name__)

# the four status-updating events the lead card carousel already sends today —
# see bob/src/components/page-builder/lead-card-carousel/useLeadCardCarousel.ts
LEAD_EVENT_TO_UPDATED_STATUS = {
    "lead.call_not_connected": "NOT_CONNECTED",
    "lead.call_back_later": "CALL_BACK",
    "lead.not_interested": "NOT_INTERESTED",
    "lead.trial_activated": "TRIAL_ACTIVATED",
}


def _resolve_lead_group_name(tenant, membership):
    """
    The RM's own assigned lead group at touch time — the same GROUP user
    setting shown in the Add/Edit User screen and used by the Lead Groups
    page (user_settings.Group), not a crm_records.lead_pipeline Bucket.

    This used to resolve a pipeline Bucket instead (testing each of the
    RM's priority-ordered bucket assignments' filter_conditions against the
    lead), but buckets are pull-queue slices, not the segmentation managers
    actually organize RMs by — event_data.lead_bucket came out empty on
    every real touch row. The RM's own GROUP assignment is simpler (one KV
    lookup, no per-lead filter matching) and is what's actually populated.

    Returns None if the RM has no GROUP set.
    """
    if not membership:
        return None
    try:
        from user_settings.models import Group
        from user_settings.services import USER_KV_GROUP_ID_KEY, kv_int_by_membership

        group_id = kv_int_by_membership(tenant, [membership.id], USER_KV_GROUP_ID_KEY).get(membership.id)
        if group_id is None:
            return None
        group = Group.objects.filter(tenant=tenant, id=group_id).first()
        return group.name if group else None
    except Exception:
        logger.exception(
            "[RmActivity] Failed to resolve lead group for membership_id=%s",
            getattr(membership, "id", None),
        )
        return None


def record_lead_touch_event(event_name, record, payload, tenant, request_user):
    """
    Writes one CALL_TOUCH row for a lead-status-update event. Does nothing if
    `event_name` isn't one of the four statuses above, or if there's no
    tenant/RM to attribute the row to.

    The RM is whoever is actually authenticated on the request
    (request.user.supabase_uid) — not a user id read out of the payload,
    which the caller controls and could spoof another RM's attribution.
    """
    updated_status = LEAD_EVENT_TO_UPDATED_STATUS.get(event_name)
    if not updated_status or tenant is None:
        return

    try:
        rm_user_id = getattr(request_user, "supabase_uid", None)
        if not rm_user_id:
            logger.warning("[RmActivity] No authenticated RM user id for event=%s record_id=%s", event_name, getattr(record, "id", None))
            return

        duration_seconds = payload.get("duration_seconds")
        try:
            duration_seconds = int(duration_seconds) if duration_seconds is not None else None
        except (TypeError, ValueError):
            duration_seconds = None

        ended_at = timezone.now()
        started_at = ended_at - timezone.timedelta(seconds=duration_seconds) if duration_seconds else ended_at

        # names are copied onto the row at write time so a dashboard query
        # never has to join back to the membership table (see RmActivityEvent docstring)
        membership = (
            TenantMembership.objects.filter(tenant=tenant, user_id=rm_user_id, is_active=True)
            .select_related("user_parent_id")
            .first()
        )
        rm_name = membership.name if membership and membership.name else ""
        manager_name = (
            membership.user_parent_id.name
            if membership and membership.user_parent_id and membership.user_parent_id.name
            else ""
        )

        # RM's own state (not the lead's) — same STATE user setting shown in
        # the Add/Edit User screen, copied here the same way rm_name/manager_name
        # are so the dashboard never has to join back to user_kv_settings
        rm_state = ""
        if membership:
            from user_settings.services import USER_KV_STATE_KEY, kv_int_by_membership

            state_value = kv_int_by_membership(tenant, [membership.id], USER_KV_STATE_KEY).get(membership.id)
            rm_state = str(state_value) if state_value is not None else ""

        record_data = (getattr(record, "data", None) or {}) if record else {}
        lead_group_name = _resolve_lead_group_name(tenant, membership)

        RmActivityEvent.objects.create(
            tenant=tenant,
            event_type="CALL_TOUCH",
            event_data={
                "rm_user_id": str(rm_user_id),
                "rm_name": rm_name,
                "manager_name": manager_name,
                # team isn't tracked on TenantMembership yet — left blank until
                # that data exists somewhere; safe to fill in later without
                # touching any of the rows written before that.
                "team": "",
                "state": rm_state,
                "lead_record_id": getattr(record, "id", None),
                # the human-facing id (shown on the lead in the app, e.g.
                # "1793876") — lead_record_id above is only the internal DB
                # row id, never shown to a manager reading this dashboard
                "praja_id": record_data.get("praja_id"),
                "updated_status": updated_status,
                "lead_group": lead_group_name,
                "party": record_data.get("affiliated_party"),
                # only ever set today when the RM picks a "Not Interested"
                # reason — the lead-card carousel's other 3 dispositions
                # don't send one, so this is null/absent for them
                "reason": payload.get("reason"),
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "duration_seconds": duration_seconds,
            },
        )
    except Exception:
        logger.exception(
            "[RmActivity] Failed to record touch event=%s record_id=%s",
            event_name,
            getattr(record, "id", None),
        )
