from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from django.db import transaction
from django.utils import timezone

from crm_records.lead_assignment_tracking import merge_first_assignment_today_anchor
from crm_records.lead_pipeline.candidate_selector import CandidateSelector
from crm_records.lead_pipeline.post_assignment import PostAssignmentActions
from crm_records.models import Record

logger = logging.getLogger(__name__)


@dataclass
class AssignmentResult:
    record: Record
    is_fresh_assignment: bool


class LeadAssigner:
    ASSIGNED_STATUS = "ASSIGNED"

    def __init__(self, *, candidate_selector: Optional[CandidateSelector] = None, post_actions: Optional[PostAssignmentActions] = None):
        self.candidate_selector = candidate_selector or CandidateSelector()
        self.post_actions = post_actions or PostAssignmentActions()

    def assign_main_queue(
        self,
        *,
        candidate_pk: int,
        tenant,
        user,
        tenant_membership,
        user_identifier: str,
        user_uuid,
        now,
    ) -> Optional[AssignmentResult]:
        """
        Lock + validate cooldown + mutate Record.data + emit post-assignment actions.
        This is extracted from GetNextLeadView Step 5 and keeps the same semantics.
        """
        with transaction.atomic():
            candidate_locked = Record.objects.select_for_update(skip_locked=True).filter(pk=candidate_pk).first()
            if not candidate_locked:
                return None

            if not self.candidate_selector.is_due_for_call(candidate_locked.data, timezone.now()):
                return None

            data = candidate_locked.data.copy() if candidate_locked.data else {}
            previous_assigned_to = data.get("assigned_to")
            is_fresh_assignment = previous_assigned_to in (None, "", "null", "None")

            data["assigned_to"] = user_identifier
            data["lead_stage"] = self.ASSIGNED_STATUS

            if "call_attempts" not in data or data.get("call_attempts") in (None, "", "null"):
                data["call_attempts"] = 0

            call_attempts = data.get("call_attempts", 0)
            try:
                call_attempts_int = int(call_attempts) if call_attempts is not None else 0
            except (TypeError, ValueError):
                call_attempts_int = 0

            last_call_outcome = (data.get("last_call_outcome", "") or "").lower()
            lead_stage = (data.get("lead_stage", "") or "").upper()

            is_not_connected_retry = (
                call_attempts_int > 0
                or last_call_outcome == "not_connected"
                or lead_stage == "NOT_CONNECTED"
            )

            if is_fresh_assignment and "first_assigned_at" not in data and not is_not_connected_retry:
                data["first_assigned_at"] = now.isoformat()
                data["first_assigned_to"] = user_identifier
                logger.info(
                    "[LeadAssigner] Set first_assigned_to=%s and first_assigned_at for lead_id=%d (fresh assignment)",
                    user_identifier,
                    candidate_locked.id,
                )
            elif is_fresh_assignment and is_not_connected_retry:
                logger.info(
                    "[LeadAssigner] Skipping first_assigned tracking for lead_id=%d (retry lead - call_attempts=%d last_call_outcome=%s lead_stage=%s)",
                    candidate_locked.id,
                    call_attempts_int,
                    last_call_outcome,
                    lead_stage,
                )

            if is_fresh_assignment:
                merge_first_assignment_today_anchor(data, now)

            candidate_locked.data = data
            candidate_locked.updated_at = timezone.now()
            candidate_locked.save(update_fields=["data", "updated_at"])

            self.post_actions.run(
                record=candidate_locked,
                tenant=tenant,
                user=user,
                tenant_membership=tenant_membership,
                user_identifier=user_identifier,
                user_uuid=user_uuid,
                lead_data=data,
            )

            return AssignmentResult(record=candidate_locked, is_fresh_assignment=is_fresh_assignment)

