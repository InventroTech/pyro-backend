from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as std_utc, timedelta
from typing import Dict, List, Optional, Tuple

from django.db.models import Q, QuerySet

from crm_records.models import CallAttemptMatrix, Record

try:
    from dateutil import parser as date_parser  # type: ignore
except ImportError:  # pragma: no cover
    date_parser = None


class CallAttemptMatrixFilter:
    """
    Extracted call-attempt matrix exclusion logic from GetNextLeadView.

    Notes:
    - Max attempts + SLA are applied via DB-level exclude where possible.
    - Minimum time between calls is enforced via per-record evaluation (first 1000).
    """

    def apply(
        self,
        *,
        qs: QuerySet,
        tenant,
        eligible_lead_types: List[str],
        now,
    ) -> QuerySet:
        if not eligible_lead_types:
            return qs

        matrices = self._load_matrices(tenant, eligible_lead_types)
        if not matrices:
            return qs

        qs = self._exclude_by_max_attempts_and_sla(qs, matrices, now)
        qs = self._exclude_by_min_time(qs, matrices, now)
        return qs

    def _load_matrices(self, tenant, lead_types: List[str]) -> Dict[str, CallAttemptMatrix]:
        return {m.lead_type: m for m in CallAttemptMatrix.objects.filter(tenant=tenant, lead_type__in=lead_types)}

    def _exclude_by_max_attempts_and_sla(self, qs: QuerySet, matrices: Dict[str, CallAttemptMatrix], now) -> QuerySet:
        exclusion_filters = Q()
        for lead_type, matrix in matrices.items():
            lead_type_filter = Q(data__affiliated_party=lead_type)
            exclusion_filters |= lead_type_filter & Q(data__call_attempts__gte=matrix.max_call_attempts)
            exclusion_filters |= lead_type_filter & Q(created_at__lt=now - timedelta(days=matrix.sla_days))

        return qs.exclude(exclusion_filters) if exclusion_filters else qs

    def _exclude_by_min_time(self, qs: QuerySet, matrices: Dict[str, CallAttemptMatrix], now) -> QuerySet:
        final_valid_ids: List[int] = []

        # Matches existing view behavior: min-time needs per record parsing.
        for lead in qs[:1000]:
            lead_data = lead.data or {}
            lead_type = (lead_data or {}).get("affiliated_party")
            matrix = matrices.get(lead_type)
            if not matrix:
                final_valid_ids.append(lead.id)
                continue

            should_exclude, _reason = self._should_exclude_lead_by_matrix(lead, lead_data, matrix, now)
            if should_exclude:
                continue
            final_valid_ids.append(lead.id)

        if not final_valid_ids:
            return qs.none()
        return qs.filter(id__in=final_valid_ids)

    def _should_exclude_lead_by_matrix(self, record: Record, lead_data: dict, matrix: CallAttemptMatrix, now) -> Tuple[bool, Optional[str]]:
        if not matrix:
            return False, None

        call_attempts_int = self._safe_int(lead_data.get("call_attempts"))
        if call_attempts_int >= matrix.max_call_attempts:
            return True, f"Max call attempts ({matrix.max_call_attempts}) reached"

        if record and record.created_at:
            days_since_creation = (now - record.created_at).days
            if days_since_creation > matrix.sla_days:
                return True, f"SLA ({matrix.sla_days} days) exceeded"

        next_call_at_str = lead_data.get("next_call_at")
        if next_call_at_str and call_attempts_int > 0:
            try:
                if date_parser:
                    next_call_at = date_parser.parse(next_call_at_str)
                else:
                    next_call_at = datetime.fromisoformat(str(next_call_at_str).replace("Z", "+00:00"))

                # Normalize timezone like GetNextLeadView.
                if now.tzinfo is None and next_call_at.tzinfo is not None:
                    next_call_at = next_call_at.replace(tzinfo=None)
                elif now.tzinfo is not None and next_call_at.tzinfo is None:
                    next_call_at = next_call_at.replace(tzinfo=std_utc.utc)

                hours_since_last_call = (now - next_call_at).total_seconds() / 3600
                if hours_since_last_call < matrix.min_time_between_calls_hours:
                    return True, f"Minimum time between calls ({matrix.min_time_between_calls_hours} hours) not met"
            except Exception:
                # If parsing fails, keep the lead (matches view: debug log only).
                return False, None

        return False, None

    def _safe_int(self, v) -> int:
        try:
            return int(v) if v is not None else 0
        except (TypeError, ValueError):
            return 0

