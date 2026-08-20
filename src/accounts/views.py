import re
import uuid
from typing import Optional
from django.db import transaction, connection
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, serializers
from django.conf import settings
from accounts.serializers import TenantMembershipCreateSerializer, TenantMembershipUpdateSerializer
from authz.permissions import IsTenantAuthenticated, HasTenantRole
from authz.service import get_authz_role_from_legacy_role  # DEPRECATED: Will be removed
from authz.models import TenantMembership, Role
from user_settings.models import Group
from user_settings.services import upsert_user_kv_settings
from rest_framework.permissions import IsAuthenticated, AllowAny
from core.models import Tenant

from django.db.models import Subquery
from .serializers import LinkUserUidSerializer, DeleteUserEverywhereSerializer
from authz.service import link_user_uid_and_activate, drop_permissions_cache
from accounts.services.delete_user_everywhere import delete_user_everywhere
import logging

logger = logging.getLogger(__name__)


def _apply_group_and_assignment(
    tenant,
    membership: TenantMembership,
    lead_group_name: Optional[str],
    daily_target,
    daily_limit,
):
    group = None
    normalized_group_name = (lead_group_name or "").strip()
    if normalized_group_name:
        group = Group.objects.filter(tenant=tenant, name__iexact=normalized_group_name).first()
        if not group:
            raise serializers.ValidationError({"lead_group_name": "Lead group not found for this tenant."})

    upsert_user_kv_settings(
        tenant=tenant,
        tenant_membership=membership,
        group_id=group.id if group else None,
        daily_target=daily_target,
        daily_limit=daily_limit,
    )

    return group

class TenantMembershipCreateView(APIView):
    permission_classes = [IsTenantAuthenticated]
    def post(self, request):
        ser = TenantMembershipCreateSerializer(data = request.data, context={'request':request})
        ser.is_valid(raise_exception=True)
        tenant = request.tenant
        name = ser.validated_data["name"].strip()
        email = ser.validated_data["email"]
        company_name = ser.validated_data.get("company_name")
        department = (ser.validated_data.get("department") or "").strip() or None
        role_id = ser.validated_data.get("role_id")
        uid = ser.validated_data.get("uid")
        lead_group_name = ser.validated_data.get("lead_group_name")
        daily_target = ser.validated_data.get("daily_target")
        daily_limit = ser.validated_data.get("daily_limit")

        if not role_id:
            return Response({
                'error': 'role_id is required to create TenantMembership'
            }, status=status.HTTP_400_BAD_REQUEST)

        with transaction.atomic():
            try:
                try:
                    authz_role = Role.objects.get(id=role_id, tenant=tenant)
                except Role.DoesNotExist:
                    try:
                        authz_role = get_authz_role_from_legacy_role(role_id, tenant)
                    except Exception as e:
                        logger.error(f"Failed to find role {role_id} for tenant {tenant.id}: {e}")
                        return Response({
                            'error': f'Role with ID {role_id} not found for this tenant'
                        }, status=status.HTTP_400_BAD_REQUEST)
                
                membership, created = TenantMembership.objects.get_or_create(
                    tenant=tenant,
                    email=email,
                    role=authz_role,
                    defaults={
                        'name': name,
                        'company_name': company_name,
                        'department': department,
                        'user_id': uid,
                        'is_active': bool(uid)
                    }
                )
                
                if not created:
                    membership.name = name
                    if company_name is not None:
                        membership.company_name = company_name
                    if department is not None:
                        membership.department = department
                    membership.role = authz_role
                    if uid:
                        membership.user_id = uid
                        membership.is_active = True
                    membership.save()

                group = _apply_group_and_assignment(
                    tenant=tenant,
                    membership=membership,
                    lead_group_name=lead_group_name,
                    daily_target=daily_target,
                    daily_limit=daily_limit,
                )
                
                if membership.user_id:
                    drop_permissions_cache(str(membership.user_id), membership.tenant)
                
                return Response({
                    'id': str(membership.id),
                    'name': membership.name,
                    'email': membership.email,
                    'tenant_id': str(tenant.id),
                    'company_name': membership.company_name,
                    'department': membership.department,
                    'role_id': str(membership.role.id),
                    'uid': str(membership.user_id) if membership.user_id else None,
                    'is_active': membership.is_active,
                    'lead_group_id': group.id if group else None,
                    'lead_group_name': group.name if group else None,
                    'created': created
                }, status=status.HTTP_201_CREATED)
                        
            except Exception as e:
                logger.error(f"Failed to create TenantMembership for user {email}: {str(e)}", exc_info=True)
                return Response({
                    'error': f'Failed to create user: {str(e)}'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class TenantMembershipUpdateView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        ser = TenantMembershipUpdateSerializer(data=request.data, context={"request": request})
        ser.is_valid(raise_exception=True)

        tenant = request.tenant
        membership = ser.validated_data["_membership"]
        name = ser.validated_data["name"].strip()
        email = ser.validated_data["email"]
        department = (ser.validated_data.get("department") or "").strip() or None
        role_id = ser.validated_data["role_id"]
        lead_group_name = ser.validated_data.get("lead_group_name")
        daily_target = ser.validated_data.get("daily_target")
        daily_limit = ser.validated_data.get("daily_limit")

        with transaction.atomic():
            try:
                try:
                    authz_role = Role.objects.get(id=role_id, tenant=tenant)
                except Role.DoesNotExist:
                    try:
                        authz_role = get_authz_role_from_legacy_role(role_id, tenant)
                    except Exception as e:
                        logger.error(f"Failed to find role {role_id} for tenant {tenant.id}: {e}")
                        return Response({
                            "error": f"Role with ID {role_id} not found for this tenant"
                        }, status=status.HTTP_400_BAD_REQUEST)

                membership.name = name
                membership.email = email
                membership.department = department
                membership.role = authz_role

                # Preserve or update manager details safely if provided in request
                if "manager_email" in ser.validated_data:
                    membership.manager_email = ser.validated_data["manager_email"]
                if "manager_name" in ser.validated_data:
                    membership.manager_name = ser.validated_data["manager_name"]

                membership.save()

                group = _apply_group_and_assignment(
                    tenant=tenant,
                    membership=membership,
                    lead_group_name=lead_group_name,
                    daily_target=daily_target,
                    daily_limit=daily_limit,
                )

                if membership.user_id:
                    drop_permissions_cache(str(membership.user_id), membership.tenant)

                return Response({
                    "id": str(membership.id),
                    "name": membership.name,
                    "email": membership.email,
                    "tenant_id": str(tenant.id),
                    "department": membership.department,
                    "role_id": str(membership.role.id),
                    "uid": str(membership.user_id) if membership.user_id else None,
                    "is_active": membership.is_active,
                    "lead_group_id": group.id if group else None,
                    "lead_group_name": group.name if group else None,
                    "updated": True
                }, status=status.HTTP_200_OK)
            except Exception as e:
                logger.error(f"Failed to update TenantMembership {membership.id}: {str(e)}", exc_info=True)
                return Response({
                    "error": f"Failed to update user: {str(e)}"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class AssigneesByRoleView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        role_key = (request.query_params.get('role') or '').strip().upper()
        tenant = request.tenant
        
        if not tenant:
            return Response({'error': "Unable to determine tenant for current user."}, status=403)
        
        try:
            if role_key:
                role = Role.objects.filter(tenant=tenant, key__iexact=role_key).first()
                if not role:
                    role = Role.objects.filter(tenant=tenant, name__iexact=role_key).first()
                
                if role:
                    qs = TenantMembership.objects.filter(
                        tenant=tenant,
                        role=role,
                        is_active=True
                    ).select_related('role').order_by('email', 'id')
                else:
                    qs = TenantMembership.objects.none()
            else:
                qs = TenantMembership.objects.filter(
                    tenant=tenant,
                    is_active=True
                ).select_related('role').order_by('email', 'id')
            
            data = []
            for membership in qs:
                name = membership.name or membership.email.split('@')[0] if membership.email else 'Unknown'
                
                data.append({
                    'id': str(membership.id),
                    'name': name,
                    'email': membership.email,
                    'company_name': membership.company_name,
                    'department': membership.department,
                    'uid': str(membership.user_id) if membership.user_id else None,
                    'role': {
                        'id': str(membership.role.id),
                        'key': membership.role.key,
                        'name': membership.role.name
                    }
                })
            
            return Response({'count': len(data), 'results': data})
            
        except Exception as e:
            logger.error(f"Error in AssigneesByRoleView: {e}", exc_info=True)
            return Response({'count': 0, 'results': []}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


PYRO_ADMIN_ROLE_KEY = "pyro_admin"
PYRO_ADMIN_ROLE_NAME = "PYRO_ADMIN"


def _slugify_tenant_slug(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        return ""
    s = re.sub(r"[^a-z0-9\-]", "", raw.lower().strip())
    return re.sub(r"-+", "-", s).strip("-") or ""


class SetupNewTenantView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        tenant_slug = (request.data.get("tenant_slug") or "").strip()
        tenant_name = (request.data.get("tenant_name") or "").strip()
        if not tenant_slug:
            return Response({"error": "tenant_slug is required"}, status=status.HTTP_400_BAD_REQUEST)
        slug = _slugify_tenant_slug(tenant_slug)
        if not slug:
            return Response(
                {"error": "tenant_slug must contain at least one letter or number"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        email = (getattr(request.user, "email", None) or "").lower().strip()
        if not email:
            return Response({"error": "User email not found"}, status=status.HTTP_400_BAD_REQUEST)
        supabase_uid = getattr(request.user, "supabase_uid", None)
        if not supabase_uid:
            return Response({"error": "User supabase_uid not found"}, status=status.HTTP_400_BAD_REQUEST)

        existing = (
            TenantMembership.objects.filter(user_id=supabase_uid, is_active=True)
            .select_related("tenant", "role")
            .first()
        )
        if existing:
            return Response(
                {
                    "success": True,
                    "tenant_id": str(existing.tenant.id),
                    "tenant_slug": existing.tenant.slug,
                    "role_id": str(existing.role.id),
                    "role_key": existing.role.key,
                    "message": "Already set up",
                },
                status=status.HTTP_200_OK,
            )

        name = tenant_name or slug.replace("-", " ").title()
        with transaction.atomic():
            tenant, tenant_created = Tenant.objects.get_or_create(
                slug=slug,
                defaults={"id": uuid.uuid4(), "name": name, "created_at": timezone.now()},
            )
            if not tenant_created:
                return Response(
                    {"error": f"Organization slug '{slug}' is already taken"},
                    status=status.HTTP_409_CONFLICT,
                )
            role, _ = Role.objects.get_or_create(
                tenant=tenant,
                key=PYRO_ADMIN_ROLE_KEY,
                defaults={"name": PYRO_ADMIN_ROLE_NAME, "description": "Default admin role"},
            )
            membership, _ = TenantMembership.objects.get_or_create(
                tenant=tenant,
                email=email,
                role=role,
                defaults={
                    "user_id": supabase_uid,
                    "is_active": True,
                    "name": name or email.split("@")[0],
                },
            )
            if membership.user_id != supabase_uid:
                membership.user_id = supabase_uid
                membership.is_active = True
                membership.save(update_fields=["user_id", "is_active"])

        return Response(
            {
                "success": True,
                "tenant_id": str(tenant.id),
                "tenant_slug": tenant.slug,
                "role_id": str(role.id),
                "role_key": role.key,
                "message": "Tenant, PYRO_ADMIN role, and membership created",
            },
            status=status.HTTP_201_CREATED,
        )


class LinkUserUidView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        try:
            serializer = LinkUserUidSerializer(data=request.data)
            serializer.is_valid(raise_exception=True)

            email = serializer.validated_data["email"]
            uid = serializer.validated_data["uid"]

            result = link_user_uid_and_activate(email, uid)

            if result.get("success") or result.get("code") == "UID_CONFLICT":
                return Response(result, status=status.HTTP_200_OK)
            return Response(result, status=status.HTTP_400_BAD_REQUEST)

        except serializers.ValidationError as ve:
            return Response({"error": ve.detail}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Error in LinkUserUidView.post: {e}", exc_info=True)
            return Response(
                {"error": "Internal server error", "message": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class DeleteUserEverywhereView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def delete(self, request):
        try:
            ser = DeleteUserEverywhereSerializer(data=request.data, context={"request": request})
            ser.is_valid(raise_exception=True)

            tenant = ser.validated_data["_tenant"]
            uid = ser.validated_data.get("uid")
            email = ser.validated_data.get("email")
            role_id = ser.validated_data.get("role_id")

            report = delete_user_everywhere(tenant=tenant, uid=uid, email=email, role_id=role_id)

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
                },
                status=status.HTTP_200_OK,
            )

        except Exception as e:
            logger.error("DeleteUserEverywhereView error: %s", str(e), exc_info=True)
            return Response(
                {"success": False, "error": "Internal server error"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )