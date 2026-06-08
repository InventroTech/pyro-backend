from calendar import monthrange
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Q
from django.db.models.functions import Lower
from django.conf import settings
from django.utils import timezone
import jwt

from rest_framework.permissions import AllowAny, IsAuthenticated
from authz.permissions import IsTenantAuthenticated, HasPermissionKey
from authz.models import Role, TenantMembership
from .serializers import RoleListSerializer, CreateSyncedRoleSerializer, TenantMembershipUserSerializer
from .service import create_or_sync_role
from user_settings.models import Group, TenantMemberSetting
from user_settings.services import USER_KV_GROUP_ID_KEY


INTERNAL_BILLING_EMAIL_DOMAIN = "@thepyro.ai"
INTERNAL_BILLING_EMAIL_ADDRESSES = (
    "ritammajumder0@gmail.com",
    "ritammajumder2025@gmail.com",
    "ritam.majumder.21@aot.edu.in",
    "bibhab.mukhopadhyay.21@aot.edu.in",
    "ritamcoding@gmail.com",
    "ritamvlog@mail.com",
    "aquiveda@gmail.com",
    "beguntalajagaranisangha@gmail.com",
    "bibhabindia@gmail.com",
    "bibhab1208@gmail.com",
    "bibhabindia2@gmail.com",
    "ritam.pyro@thecircleapp.in",
    "bibhab.pyro@thecircleapp.in",
    "dinesh.pyro@thecircleapp.in",
    "ranjith1610@gmail.com",
    "ranji.nitt@gmail.com",
    "harisudhan.nandhu@gmail.com",
    "abhsr1987@gmail.com",
    "ritam.pyro@circleapp.in",
)
BILLING_ROLE_RATES = {
    "CSE": Decimal("1500"),
    "RM": Decimal("2000"),
}


def _parse_billing_month(value):
    if not value:
        today = _today()
        return date(today.year, today.month, 1)

    try:
        year_text, month_text = str(value).split("-", 1)
        return date(int(year_text), int(month_text), 1)
    except (TypeError, ValueError):
        raise ValueError("month must be in YYYY-MM format")


def _current_billing_month():
    today = _today()
    return date(today.year, today.month, 1)


def _today():
    return timezone.now().date()


def _calendar_days_for_month(billing_month):
    return monthrange(billing_month.year, billing_month.month)[1]


def _billing_period_end(billing_month):
    if billing_month == _current_billing_month():
        return _today()

    calendar_days = _calendar_days_for_month(billing_month)
    return date(billing_month.year, billing_month.month, calendar_days)


def _membership_billing_end(membership, period_end):
    deleted_at = getattr(membership, "deleted_at", None)
    if not deleted_at:
        return period_end

    deleted_date = _date_from_datetime(deleted_at)
    return min(period_end, deleted_date)


def _parse_cycle_days(value, billing_month):
    raw = _calendar_days_for_month(billing_month) if value in (None, "") else value
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        raise ValueError("cycle_days must be a valid integer")

    if parsed <= 0:
        raise ValueError("cycle_days must be greater than 0")
    return parsed


def _parse_non_negative_decimal(value, field_name):
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{field_name} must be a valid number")

    if parsed < 0:
        raise ValueError(f"{field_name} must be greater than or equal to 0")
    return parsed


def _default_rate_for_role(role):
    role_key = (getattr(role, "key", "") or "").strip().upper()
    if role_key in BILLING_ROLE_RATES:
        return BILLING_ROLE_RATES[role_key]

    role_name = (getattr(role, "name", "") or "").strip().upper()
    if role_name in BILLING_ROLE_RATES:
        return BILLING_ROLE_RATES[role_name]

    return Decimal("0")


def _tenant_billing_roles(tenant):
    return list(Role.objects.filter(tenant=tenant).order_by("name", "key", "id"))


def _billing_role_rates_from_request(query_params, tenant):
    roles = _tenant_billing_roles(tenant)
    rates_by_role_id = {
        str(role.id): _default_rate_for_role(role)
        for role in roles
    }

    raw_role_rates = query_params.get("role_rates")
    if raw_role_rates not in (None, ""):
        try:
            role_rate_overrides = json.loads(raw_role_rates)
        except (TypeError, ValueError):
            raise ValueError("role_rates must be a valid JSON object")

        if not isinstance(role_rate_overrides, dict):
            raise ValueError("role_rates must be a valid JSON object")

        for role_id, raw_rate in role_rate_overrides.items():
            role_id = str(role_id)
            if role_id in rates_by_role_id and raw_rate not in (None, ""):
                rates_by_role_id[role_id] = _parse_non_negative_decimal(raw_rate, f"role_rates.{role_id}")

    # Backward-compatible query params used by older BillingPage versions.
    for role in roles:
        role_key = (role.key or "").strip().upper()
        role_name = (role.name or "").strip().upper()
        for legacy_key in BILLING_ROLE_RATES:
            if role_key == legacy_key or role_name == legacy_key:
                raw_value = query_params.get(f"{legacy_key.lower()}_rate")
                if raw_value not in (None, ""):
                    rates_by_role_id[str(role.id)] = _parse_non_negative_decimal(
                        raw_value,
                        f"{legacy_key.lower()}_rate",
                    )

    return roles, rates_by_role_id


def _date_from_datetime(value):
    return timezone.localtime(value).date() if timezone.is_aware(value) else value.date()


def _internal_billing_email_q():
    query = Q(email__iendswith=INTERNAL_BILLING_EMAIL_DOMAIN)
    for email in INTERNAL_BILLING_EMAIL_ADDRESSES:
        query |= Q(email__iexact=email)
    return query


def _role_billing_key(role, role_rates=None):
    role_rates = role_rates or BILLING_ROLE_RATES
    if not role:
        return None

    role_key = (getattr(role, "key", "") or "").strip().upper()
    if role_key in role_rates:
        return role_key

    role_name = (getattr(role, "name", "") or "").strip().upper()
    if role_name in role_rates:
        return role_name

    return None


def get_membership_monthly_amount(membership, role_rates=None):
    role = getattr(membership, "role", None)
    role_rates = role_rates or BILLING_ROLE_RATES
    if role is not None:
        role_id = str(getattr(role, "id", "") or "")
        if role_id in role_rates:
            billing_key = (getattr(role, "key", "") or getattr(role, "name", "") or role_id).strip()
            return billing_key, role_rates.get(role_id, Decimal("0"))

    billing_key = _role_billing_key(role, role_rates)
    return billing_key, role_rates.get(billing_key, Decimal("0"))


def calculate_membership_billing(joined_at, billing_month, monthly_amount, cycle_days=None, period_end=None):
    """
    Prorate seat billing for a monthly cycle.

    Billable days are counted within the selected billing window. For the current
    month, the billing window ends today; for past months it ends on month-end.
    """
    cycle_days = cycle_days or _calendar_days_for_month(billing_month)
    period_end = period_end or date(
        billing_month.year,
        billing_month.month,
        min(cycle_days, _calendar_days_for_month(billing_month)),
    )
    joined_date = _date_from_datetime(joined_at)
    join_month_index = joined_date.year * 12 + joined_date.month
    billing_month_index = billing_month.year * 12 + billing_month.month

    if joined_date > period_end or join_month_index > billing_month_index:
        billable_days = 0
    elif join_month_index < billing_month_index or joined_date <= billing_month:
        billable_days = min(cycle_days, period_end.day)
    else:
        billable_days = max(period_end.day - joined_date.day + 1, 0)

    amount = ((monthly_amount * Decimal(billable_days)) / Decimal(cycle_days)).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )
    return billable_days, amount


class RolesView(APIView):
    """
    GET  /api/authz/roles      -> list roles from authz_role (tenant-scoped)
    POST /api/authz/roles      -> create role in BOTH authz_role & legacy roles (same UUID)
    """

    def get_permissions(self):
        if self.request.method == 'POST':
            return [IsTenantAuthenticated()]
        return [AllowAny()]

    def get(self, request, *args, **kwargs):
        """
        NEW: Added support for 'key' query parameter to filter by role key
        GET /api/authz/roles?key=public -> returns role with key='public'
        GET /api/authz/roles -> returns all roles for tenant
        """
        tenant = request.tenant
        role_key = request.query_params.get('key', '').strip()
        
        # NEW: Filter by key if provided (for public role lookup)
        if role_key:
            qs = Role.objects.filter(tenant=tenant, key__iexact=role_key)
        else:
            qs = (
                Role.objects
                .filter(tenant=tenant)
                .annotate(norm_name=Lower('name'))
                .order_by('norm_name', 'id')   # order_by must include distinct keys prefix
                .distinct('norm_name')         # collapse GM/gm/etc.
            )

        data = [{
            "id": str(r.id),
            "name": r.name,
            "description": r.description or "",
            "key": r.key or "",
        } for r in qs]

        return Response({"count": len(data), "results": data}, status=status.HTTP_200_OK)
    

    def post(self, request):
        # Enforce GM only for create, while GET stays open to tenant users.
        # Atomic guarantee: both tables written or none.
        serializer = CreateSyncedRoleSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        tenant = request.tenant
        result = create_or_sync_role(
            tenant=tenant,
            key=serializer.validated_data['key'],
            name=serializer.validated_data['name'],
            description=serializer.validated_data.get('description', '')
        )
        
        return Response({"success": True, "role": result['role']}, status=status.HTTP_201_CREATED)



class ListTenantUsersView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        """
        NEW: Uses TenantMembership.name directly (no LegacyUser fallback).
        The name field has been migrated from LegacyUser to TenantMembership.
        """
        qs = (TenantMembership.objects
              .select_related("role", "user_parent_id")
              .filter(tenant=request.tenant)
              .order_by("-is_active", "email"))
        
        # Serialize the data
        data = TenantMembershipUserSerializer(qs, many=True).data
        memberships = list(qs)
        setting_map = {
            s.tenant_membership_id: s.value
            for s in TenantMemberSetting.objects.filter(
                tenant=request.tenant,
                key=USER_KV_GROUP_ID_KEY,
                tenant_membership_id__in=[m.id for m in memberships],
            )
        }
        group_ids = {gid for gid in setting_map.values() if gid}
        groups_by_id = {}
        if group_ids:
            groups_by_id = {
                g.id: g.name
                for g in Group.objects.filter(tenant=request.tenant, id__in=group_ids).only("id", "name")
            }
        
        # Add name and company_name fields to each result (from TenantMembership)
        for i, item in enumerate(data):
            membership = memberships[i]
            # Use TenantMembership.name directly (migrated from LegacyUser)
            item['name'] = membership.name or ''
            # Include company_name if serializer doesn't already include it
            if 'company_name' not in item:
                item['company_name'] = membership.company_name or ''
            item["lead_group_name"] = groups_by_id.get(setting_map.get(membership.id))
        
        return Response({"count": len(data), "results": data}, status=status.HTTP_200_OK)


class TenantMembershipBillingView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        try:
            billing_month = _parse_billing_month(request.query_params.get("month"))
            if billing_month > _current_billing_month():
                raise ValueError("Cannot calculate billing for a future month")
            cycle_days = _parse_cycle_days(request.query_params.get("cycle_days"), billing_month)
            billing_roles, role_rates = _billing_role_rates_from_request(request.query_params, request.tenant)
        except ValueError as exc:
            return Response({"error": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

        period_end = _billing_period_end(billing_month)

        base_memberships = (
            TenantMembership.all_objects
            .select_related("role")
            .filter(tenant=request.tenant)
            .filter(created_at__date__lte=period_end)
            .filter(Q(deleted_at__isnull=True) | Q(deleted_at__date__gte=billing_month))
        )
        internal_email_query = _internal_billing_email_q()
        excluded_internal_member_count = base_memberships.filter(internal_email_query).count()
        memberships = list(
            base_memberships
            .exclude(internal_email_query)
            .order_by("created_at", "email")
        )

        rows = []
        total_amount = Decimal("0.00")
        total_billable_days = 0

        for membership in memberships:
            billing_role_key, monthly_amount = get_membership_monthly_amount(membership, role_rates)
            membership_period_end = _membership_billing_end(membership, period_end)
            daily_rate = (monthly_amount / Decimal(cycle_days)).quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP,
            )
            billable_days, amount = calculate_membership_billing(
                membership.created_at,
                billing_month,
                monthly_amount,
                cycle_days,
                membership_period_end,
            )
            total_billable_days += billable_days
            total_amount += amount
            joined_date = _date_from_datetime(membership.created_at)

            rows.append({
                "membership_id": membership.id,
                "name": membership.name or "",
                "email": membership.email,
                "role": {
                    "id": str(membership.role.id),
                    "key": membership.role.key,
                    "name": membership.role.name,
                } if membership.role_id else None,
                "is_active": membership.is_active,
                "is_deleted": membership.is_deleted,
                "joined_at": membership.created_at.isoformat(),
                "joined_date": joined_date.isoformat(),
                "billing_end_date": membership_period_end.isoformat(),
                "deleted_at": membership.deleted_at.isoformat() if membership.deleted_at else None,
                "billable_days": billable_days,
                "cycle_days": cycle_days,
                "billing_role_key": billing_role_key,
                "monthly_amount": str(monthly_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
                "daily_rate": str(daily_rate),
                "billing_amount": str(amount),
            })

        return Response({
            "month": billing_month.strftime("%Y-%m"),
            "period_start": billing_month.isoformat(),
            "period_end": period_end.isoformat(),
            "cycle_days": cycle_days,
            "excluded_email_domain": INTERNAL_BILLING_EMAIL_DOMAIN,
            "excluded_email_addresses_count": len(INTERNAL_BILLING_EMAIL_ADDRESSES),
            "billing_roles": [
                {
                    "id": str(role.id),
                    "key": role.key,
                    "name": role.name,
                    "rate": str(role_rates.get(str(role.id), Decimal("0")).quantize(
                        Decimal("0.01"),
                        rounding=ROUND_HALF_UP,
                    )),
                }
                for role in billing_roles
            ],
            "role_rates": {
                role.key: str(role_rates.get(str(role.id), Decimal("0")).quantize(
                    Decimal("0.01"),
                    rounding=ROUND_HALF_UP,
                ))
                for role in billing_roles
            },
            "summary": {
                "member_count": len(rows),
                "excluded_internal_member_count": excluded_internal_member_count,
                "total_billable_days": total_billable_days,
                "total_amount": str(total_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
            },
            "results": rows,
        }, status=status.HTTP_200_OK)


class SpoofTenantUserTokenView(APIView):
    """
    Generate a Supabase-style JWT for a specific tenant user (membership).

    POST /api/membership/users/<membership_id>/spoof-token/

    This is intended for internal admin "user spoofing" tools only.
    """

    # Only tenant users with the GM role can spoof other users.
    permission_classes = [IsTenantAuthenticated, HasPermissionKey("users:spoof")]

    def post(self, request, membership_id, *args, **kwargs):
        tenant = getattr(request, "tenant", None)
        if not tenant:
            return Response(
                {"error": "Tenant not resolved for request"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Resolve the acting membership (current authenticated user within this tenant)
        supabase_uid = getattr(request.user, "supabase_uid", None)
        acting_membership = None
        if supabase_uid:
            acting_membership = (
                TenantMembership.objects.filter(
                    tenant=tenant,
                    user_id=supabase_uid,
                    is_active=True,
                )
                .select_related("role")
                .first()
            )

        if not acting_membership:
            return Response(
                {"error": "No active membership found for requesting user"},
                status=status.HTTP_403_FORBIDDEN,
            )

        try:
            membership = (
                TenantMembership.objects.select_related("role")
                .filter(tenant=tenant, is_active=True)
                .get(id=membership_id)
            )
        except TenantMembership.DoesNotExist:
            return Response(
                {"error": "User membership not found for this tenant"},
                status=status.HTTP_404_NOT_FOUND,
            )

        # Do not allow users to spoof themselves.
        if membership.user_id == acting_membership.user_id:
            return Response(
                {"error": "Cannot spoof your own membership"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not membership.user_id or not membership.email:
            return Response(
                {"error": "Membership is missing user_id or email"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        jwt_secret = getattr(settings, "SUPABASE_JWT_SECRET", None)
        if not jwt_secret:
            return Response(
                {"error": "SUPABASE_JWT_SECRET is not configured"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        payload = {
            "sub": str(membership.user_id),
            "email": membership.email.lower(),
            "tenant_id": str(tenant.id),
            "tenant_membership_id": str(membership.id),
            "membership_id": str(membership.id),
            "role": "authenticated",
            "aud": "authenticated",
            "user_data": {
                "tenant_id": str(tenant.id),
                "tenant_membership_id": str(membership.id),
                "membership_id": str(membership.id),
                "role_id": str(membership.role.id),
                "role_key": membership.role.key,
                "user_id": str(membership.user_id),
            },
        }

        token = jwt.encode(payload, jwt_secret, algorithm="HS256")
        if isinstance(token, bytes):
            token = token.decode("utf-8")

        # Lightweight audit metadata in the response for the caller; full audit should go to logs.
        audit_meta = {
            "actor_membership_id": str(acting_membership.id),
            "actor_role_key": getattr(acting_membership.role, "key", None),
            "target_membership_id": str(membership.id),
            "target_role_key": getattr(membership.role, "key", None),
        }

        return Response(
            {
                "token": token,
                "membership_id": membership_id,
                "tenant_membership_id": membership.id,
                "email": membership.email,
                "name": membership.name or "",
                "tenant_id": str(tenant.id),
                "audit": audit_meta,
            },
            status=status.HTTP_200_OK,
        )


class CurrentUserRoleView(APIView):
    """
    Get the current authenticated user's role from TenantMembership (backend source of truth).
    This ensures frontend uses the same role that backend permissions check against.

    Uses IsAuthenticated (not IsTenantAuthenticated) because the frontend calls
    this endpoint as a fallback when the JWT lacks user_data claims. Requiring an
    active membership here would create a chicken-and-egg problem: the endpoint
    that checks membership would itself require membership to access.
    """
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = request.user
        tenant = request.tenant
        
        if not tenant:
            return Response({
                'error': 'Tenant not found'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        supabase_uid = getattr(user, 'supabase_uid', None)
        if not supabase_uid:
            return Response({
                'error': 'User supabase_uid not found'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Get the membership from TenantMembership (same source backend uses)
        membership = TenantMembership.objects.filter(
            tenant=tenant,
            user_id=supabase_uid,
            is_active=True
        ).select_related('role').first()
        
        if not membership:
            return Response({
                'role_key': None,
                'role_name': None,
                'role_id': None,
                'tenant_id': None,
                'department': None,
                'error': 'No active tenant membership found'
            }, status=status.HTTP_200_OK)

        # Expose tenant_membership_id and user_parent_id for team_lead: use user_parent_id_id if set, else own membership id
        user_parent_id_value = membership.user_parent_id_id  # FK to parent TenantMembership (integer)
        return Response({
            'role_key': membership.role.key,
            'role_name': membership.role.name,
            'role_id': str(membership.role.id),
            'tenant_id': str(tenant.id),
            'tenant_slug': tenant.slug,
            'is_active': membership.is_active,
            'department': membership.department,
            'tenant_membership_id': membership.id,
            'user_parent_id': user_parent_id_value,
        }, status=status.HTTP_200_OK)


class UpdateUserHierarchyView(APIView):
    """
    PATCH /api/membership/users/hierarchy
    Body: { "assignments": [ { "membership_id": int, "parent_membership_id": int|null }, ... ] }
    Restricts to GM/ASM. Validates tenant and prevents cycles.
    """
    permission_classes = [IsTenantAuthenticated]

    def _collect_subtree_ids(self, parent_to_children, root_membership_id, exclude_membership_id=None):
        """Return set of all membership ids in the subtree under root_membership_id (excluding exclude_membership_id).
        Uses pre-built parent_to_children map for in-memory traversal (no DB queries).
        
        Args:
            parent_to_children: Dict mapping parent_id -> [child_ids] (built from bulk query)
            root_membership_id: Root membership ID to start traversal from
            exclude_membership_id: Optional membership ID to exclude from results
        """
        # Traverse subtree in memory (no DB queries)
        seen = set()
        stack = [root_membership_id]
        while stack:
            mid = stack.pop()
            if mid == exclude_membership_id:
                continue
            if mid in seen:
                continue
            seen.add(mid)
            # Get children from in-memory map
            children = parent_to_children.get(mid, [])
            stack.extend(children)
        return seen

    def patch(self, request):
        tenant = request.tenant
        assignments = request.data.get('assignments')
        if not isinstance(assignments, list):
            return Response(
                {'error': 'assignments must be a list'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Resolve all membership ids that belong to this tenant
        all_ids = set()
        for a in assignments:
            if not isinstance(a, dict):
                continue
            mid = a.get('membership_id')
            pid = a.get('parent_membership_id')
            if mid is not None:
                all_ids.add(mid)
            if pid is not None:
                all_ids.add(pid)

        if not all_ids:
            return Response({'count': 0}, status=status.HTTP_200_OK)

        valid_ids = set(
            TenantMembership.objects.filter(
                tenant=tenant, id__in=all_ids
            ).values_list('id', flat=True)
        )
        invalid = all_ids - valid_ids
        if invalid:
            return Response(
                {'error': f'membership_id or parent_membership_id not in tenant: {invalid}'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Fetch all memberships once for cycle detection (avoid N+1 in _collect_subtree_ids)
        all_memberships = list(
            TenantMembership.objects.filter(
                tenant=tenant
            ).values('id', 'user_parent_id_id')
        )
        
        # Build parent_id -> [child_ids] map for in-memory traversal
        parent_to_children = {}
        for m in all_memberships:
            parent_id = m.get('user_parent_id_id')
            if parent_id is not None:
                parent_to_children.setdefault(parent_id, []).append(m['id'])

        # Prevent cycle: for each assignment, new parent must not be in the member's subtree
        for a in assignments:
            if not isinstance(a, dict):
                continue
            mid = a.get('membership_id')
            pid = a.get('parent_membership_id')
            if mid is None or pid is None:
                continue
            subtree = self._collect_subtree_ids(parent_to_children, mid, exclude_membership_id=mid)
            if pid in subtree:
                return Response(
                    {'error': f'Cycle: parent_membership_id {pid} is in subtree of membership_id {mid}'},
                    status=status.HTTP_400_BAD_REQUEST
                )

        # Apply updates - use bulk_update to avoid N+1 queries
        # Collect all membership_ids that need updating
        membership_ids_to_update = []
        parent_id_map = {}  # membership_id -> parent_membership_id
        
        for a in assignments:
            if not isinstance(a, dict):
                continue
            mid = a.get('membership_id')
            pid = a.get('parent_membership_id')
            if mid is None:
                continue
            membership_ids_to_update.append(mid)
            parent_id_map[mid] = pid
        
        if not membership_ids_to_update:
            return Response({'count': 0}, status=status.HTTP_200_OK)
        
        # Fetch all memberships in one query
        memberships_to_update = list(
            TenantMembership.objects.filter(
                tenant=tenant,
                id__in=membership_ids_to_update
            )
        )
        
        # Update user_parent_id_id in memory
        for membership in memberships_to_update:
            membership.user_parent_id_id = parent_id_map[membership.id]
        
        # Bulk update all at once (single query)
        TenantMembership.objects.bulk_update(
            memberships_to_update,
            ['user_parent_id_id']
        )

        return Response({'count': len(membership_ids_to_update)}, status=status.HTTP_200_OK)
