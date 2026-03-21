from __future__ import annotations

import logging
from typing import Any, Optional

from background_jobs.queue_service import get_queue_service
from background_jobs.models import JobType
from django.utils import timezone

from crm_records.models import EventLog, Record

logger = logging.getLogger(__name__)


class PostAssignmentActions:
    """
    Extracted from GetNextLeadView assignment code:
    - write EventLog
    - enqueue Mixpanel + RM-assigned events
    """

    def run(
        self,
        *,
        record: Record,
        tenant,
        user,
        tenant_membership,
        user_identifier: str,
        user_uuid,
        lead_data: dict,
    ) -> None:
        self._log_event(record=record, tenant=tenant, user_identifier=user_identifier, user_uuid=user_uuid)
        self._enqueue_mixpanel(
            record=record,
            tenant=tenant,
            user=user,
            tenant_membership=tenant_membership,
            user_identifier=user_identifier,
            lead_data=lead_data,
        )

    def _log_event(self, *, record: Record, tenant, user_identifier: str, user_uuid) -> None:
        try:
            EventLog.objects.create(
                record=record,
                tenant=tenant,
                event="lead.get_next_lead",
                payload={"user_id": str(user_uuid) if user_uuid else user_identifier, "lead_id": record.id, "record_id": record.id},
                timestamp=timezone.now(),
            )
        except Exception as e:
            logger.warning("[PostAssignment] EventLog failed: %s", e)

    def _enqueue_mixpanel(
        self,
        *,
        record: Record,
        tenant,
        user,
        tenant_membership,
        user_identifier: str,
        lead_data: dict,
    ) -> None:
        try:
            lead_name = lead_data.get("name", "") if isinstance(lead_data, dict) else ""
            rm_email = getattr(user, "email", None)
            if not rm_email and tenant_membership:
                rm_email = tenant_membership.email

            praja_id = lead_data.get("praja_id")
            queue_service = get_queue_service()
            tenant_id = str(tenant.id) if tenant else None

            if praja_id:
                try:
                    if isinstance(praja_id, int):
                        mixpanel_user_id = str(praja_id)
                    elif isinstance(praja_id, str):
                        cleaned = praja_id.upper().replace("PRAJA", "").replace("-", "").replace("_", "").strip()
                        mixpanel_user_id = cleaned if cleaned.isdigit() else praja_id
                    else:
                        mixpanel_user_id = str(praja_id)
                except (ValueError, TypeError, AttributeError):
                    mixpanel_user_id = str(praja_id) if praja_id else None

                if mixpanel_user_id:
                    mixpanel_properties = {
                        "lead_id": record.id,
                        "lead_name": lead_name,
                        "lead_status": lead_data.get("lead_stage", "ASSIGNED"),
                        "lead_score": lead_data.get("lead_score"),
                        "lead_type": lead_data.get("affiliated_party"),
                        "assigned_to": user_identifier,
                        "praja_id": praja_id,
                        "rm_email": rm_email,
                    }
                    mixpanel_properties.update(lead_data)

                    job = queue_service.enqueue_job(
                        job_type=JobType.SEND_MIXPANEL_EVENT,
                        payload={
                            "user_id": mixpanel_user_id,
                            "event_name": "pyro_crm_rm_assigned_backend",
                            "properties": mixpanel_properties,
                        },
                        tenant_id=tenant_id,
                    )
                    logger.info("[PostAssignment] Enqueued Mixpanel job_id=%s lead_id=%s", job.id, record.id)
                else:
                    logger.warning("[PostAssignment] Skipping Mixpanel job - no mixpanel_user_id from praja_id=%s", praja_id)
            else:
                logger.warning("[PostAssignment] Skipping Mixpanel job - no praja_id")

            # Enqueue RM assigned event (rm_assigned endpoint)
            if praja_id and rm_email:
                try:
                    praja_id_int = int(praja_id)
                    job2 = queue_service.enqueue_job(
                        job_type=JobType.SEND_RM_ASSIGNED_EVENT,
                        payload={"praja_id": praja_id_int, "rm_email": rm_email},
                        tenant_id=tenant_id,
                    )
                    logger.info("[PostAssignment] Enqueued rm_assigned job_id=%s lead_id=%s praja_id=%s", job2.id, record.id, praja_id_int)
                except (ValueError, TypeError) as e:
                    logger.error("[PostAssignment] Could not enqueue RM assigned job - praja_id=%s error=%s", praja_id, e)
        except Exception as e:
            logger.error("[PostAssignment] Failed enqueueing post-assignment actions: %s", e, exc_info=True)

