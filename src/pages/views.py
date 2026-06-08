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
    Resolve role_id for spoof / preview requests only.

    Callers must pass ?role_id= and/or ?user_id= when in preview mode.

    Priority:
      1. ?role_id= query param (target role directly)
      2. ?user_id= -> TenantMembership.role_id for that user in this tenant
      3. Authenticated user's membership role (only if preview params incomplete)
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


def _page_filter_is_role_preview(request):
    """
    True when the list/detail should filter by Page.role (spoof / preview as another role).

    The client often sends ?role_id= on every request equal to the logged-in user's
    role; that must NOT trigger role mode — otherwise "My pages" only shows rows for
    that role instead of every page this user created (Page.user_id).
    """
    tenant = request.tenant
    if not tenant:
        return False

    preview_q = (request.query_params.get('role_preview') or '').strip().lower()
    if preview_q in {'1', 'true', 'yes'}:
        return True

    role_q = (request.query_params.get('role_id') or '').strip()
    user_q = (request.query_params.get('user_id') or '').strip()

    if not role_q and not user_q:
        return False

    current_uid = _current_user_id(request)
    current_uid_str = str(current_uid) if current_uid else ''

    if user_q and user_q != current_uid_str:
        return True

    if role_q:
        if not current_uid:
            return True
        own_role_id = (
            TenantMembership.objects.filter(
                tenant=tenant, user_id=current_uid_str, is_active=True
            ).values_list('role_id', flat=True).first()
        )
        if own_role_id is not None and str(own_role_id) == role_q:
            return False
        return True

    return False


class PageListCreateView(APIView):
    """List pages: My pages by owner (default), or by role when spoof/preview params apply."""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant = request.tenant

        if _page_filter_is_role_preview(request):
            role_id = _resolve_role_id(request)
            if role_id:
                pages = Page.objects.filter(tenant=tenant, role_id=role_id).order_by('-updated_at')
            else:
                pages = Page.objects.none()
        else:
            user_id = _current_user_id(request)
            if user_id is None:
                return Response(
                    {'detail': 'User id not found.'},
                    status=status.HTTP_401_UNAUTHORIZED,
                )
            pages = Page.objects.filter(tenant=tenant, user_id=user_id).order_by('-updated_at')

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
    """Retrieve, update, or delete a page. Role-scoped when spoof preview applies."""

    permission_classes = [IsTenantAuthenticated]

    def _get_page(self, request, pk):
        tenant = request.tenant

        if _page_filter_is_role_preview(request):
            role_id = _resolve_role_id(request)
            if not role_id:
                return None, Response(
                    {'detail': 'Not found.'},
                    status=status.HTTP_404_NOT_FOUND,
                )
            page = Page.objects.filter(pk=pk, tenant=tenant, role_id=role_id).first()
        else:
            user_id = _current_user_id(request)
            if user_id is None:
                return None, Response(
                    {'detail': 'User id not found.'},
                    status=status.HTTP_401_UNAUTHORIZED,
                )
            page = Page.objects.filter(pk=pk, tenant=tenant, user_id=user_id).first()

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
