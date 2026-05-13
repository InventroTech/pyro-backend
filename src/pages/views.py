from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, generics
from uuid import UUID

from authz.permissions import IsTenantAuthenticated
from authz.models import TenantMembership
from .models import Page, CustomIcon
from .serializers import PageSerializer, PageCreateUpdateSerializer, CustomIconSerializer


def _current_user_id(request):
    """Supabase auth user UUID from request."""
    uid = getattr(request.user, 'supabase_uid', None)
    if not uid:
        return None
    try:
        return UUID(uid) if isinstance(uid, str) else uid
    except (ValueError, TypeError):
        return None


def _resolve_role_id(request):
    """
    Resolve the role_id to filter pages by.

    Priority:
      1. ?role_id= query param (frontend passes target role directly for spoof)
      2. ?user_id= query param -> look up that user's TenantMembership -> role_id
      3. Authenticated user's own TenantMembership -> role_id
    """
    tenant = request.tenant
    if not tenant:
        return None

    # 1) Direct role_id param (spoof mode: frontend passes the target role)
    role_id_param = request.query_params.get('role_id')
    if role_id_param:
        return role_id_param

    # 2) user_id param: look up that user's membership to get their role
    spoof_uid = request.query_params.get('user_id')
    if spoof_uid:
        membership = TenantMembership.objects.filter(
            tenant=tenant, user_id=spoof_uid, is_active=True
        ).select_related('role').first()
        if membership and membership.role_id:
            return str(membership.role_id)
        return None

    # 3) Normal: use authenticated user's own role
    user_id = _current_user_id(request)
    if not user_id:
        return None

    membership = TenantMembership.objects.filter(
        tenant=tenant, user_id=str(user_id), is_active=True
    ).select_related('role').first()
    if membership and membership.role_id:
        return str(membership.role_id)
    return None


class PageListCreateView(APIView):
    """List pages for a role. Supports ?user_id= for spoof/preview."""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant = request.tenant
        role_id = _resolve_role_id(request)

        if role_id:
            pages = Page.objects.filter(tenant=tenant, role_id=role_id).order_by('-updated_at')
        else:
            pages = Page.objects.none()

        serializer = PageSerializer(pages, many=True)
        return Response(serializer.data)

    def post(self, request):
        tenant = request.tenant
        user_id = _current_user_id(request)
        if user_id is None:
            return Response(
                {'detail': 'User id not found.'},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        serializer = PageCreateUpdateSerializer(
            data=request.data,
            context={'tenant': tenant, 'user_id': user_id},
        )
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        page = serializer.save()
        return Response(PageSerializer(page).data, status=status.HTTP_201_CREATED)


class PageDetailView(APIView):
    """Retrieve, update, or delete a page (role-scoped)."""
    permission_classes = [IsTenantAuthenticated]

    def _get_page(self, request, pk):
        tenant = request.tenant
        role_id = _resolve_role_id(request)

        if not role_id:
            return None, Response(
                {'detail': 'Not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )

        page = Page.objects.filter(pk=pk, tenant=tenant, role_id=role_id).first()
        if page is None:
            return None, Response(
                {'detail': 'Not found.'},
                status=status.HTTP_404_NOT_FOUND,
            )
        return page, None

    def get(self, request, pk):
        page, err = self._get_page(request, pk)
        if err is not None:
            return err
        return Response(PageSerializer(page).data)

    def put(self, request, pk):
        page, err = self._get_page(request, pk)
        if err is not None:
            return err
        serializer = PageCreateUpdateSerializer(
            page,
            data=request.data,
            partial=True,
            context={'tenant': request.tenant, 'user_id': page.user_id},
        )
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        serializer.save()
        page.refresh_from_db()
        return Response(PageSerializer(page).data)

    def patch(self, request, pk):
        return self.put(request, pk)

    def delete(self, request, pk):
        page, err = self._get_page(request, pk)
        if err is not None:
            return err
        page.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class CustomIconListCreateView(generics.ListCreateAPIView):
    """List all custom icons for the tenant and upload a new one."""
    permission_classes = [IsTenantAuthenticated]
    serializer_class = CustomIconSerializer

    def get_queryset(self):
        return CustomIcon.objects.filter(tenant=self.request.tenant)

    def perform_create(self, serializer):
        serializer.save(tenant=self.request.tenant)


class CustomIconDetailView(generics.DestroyAPIView):
    """Delete a custom icon."""
    permission_classes = [IsTenantAuthenticated]
    serializer_class = CustomIconSerializer

    def get_queryset(self):
        return CustomIcon.objects.filter(tenant=self.request.tenant)
