from django.db.models import Q
from rest_framework.exceptions import ValidationError
from rest_framework.filters import SearchFilter, OrderingFilter

# Utility to normalize multi-select params: supports repeated & CSV
def get_multi_values(query_params, key, alt_key):
    vals = query_params.getlist(key)
    if not vals and alt_key:
        csv_val = query_params.get(alt_key)
        if csv_val:
            vals = [v.strip() for v in csv_val.split(",") if v.strip()]
    flat = []
    for v in vals:
        if "," in v:  # tolerate accidental CSV in a single repeated param
            flat.extend([p.strip() for p in v.split(",") if p.strip()])
        else:
            flat.append(v.strip())
    # de-dup while preserving order
    seen = set()
    out = []
    for v in flat:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out

RESOLUTION_CHOICES = {"WIP","Resolved","Can't Resolve", "Closed", "Snoozed"}

def build_nullable_in_q(field_name, values, allowed):
    """Builds Q for field IN values plus NULL if 'null' present."""
    vals = [v for v in values if v.lower() != "null"]
    if allowed is not None:
        bad = [v for v in vals if v not in allowed]
        if bad:
            raise ValidationError({field_name: f"Invalid values: {bad}"})
    q = Q()
    if vals:
        q |= Q(**{f"{field_name}__in": vals})
    if any(v.lower() == "null" for v in values):
        q |= Q(**{f"{field_name}__isnull": True})
    return q

class SafeSearchFilter(SearchFilter):
    # restrict which fields are searched to avoid degenerate queries
    search_param = "search"

class SafeOrderingFilter(OrderingFilter):
    ordering_param = "ordering"
    def get_valid_fields(self, queryset, view, context=None):
        
        return [
            ("created_at", "created_at"),
            ("-created_at", "-created_at"),
            ("ticket_date", "ticket_date"),
            ("-ticket_date", "-ticket_date"),
            ("resolution_status", "resolution_status"),
            ("-resolution_status", "-resolution_status"),
        ]
