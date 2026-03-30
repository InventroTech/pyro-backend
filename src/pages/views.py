from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, generics
from django.shortcuts import get_object_or_404
from uuid import UUID
from yaml import serializer

from authz.permissions import IsTenantAuthenticated
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


class PageListCreateView(APIView):
    """List my pages (by user_id) and create a new page."""
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant = request.tenant
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
    """Retrieve, update, or delete a page (only if owned by current user)."""
    permission_classes = [IsTenantAuthenticated]

    def _get_page(self, request, pk):
        tenant = request.tenant
        user_id = _current_user_id(request)
        if user_id is None:
            return None, Response(
                {'detail': 'User id not found.'},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        page = get_object_or_404(Page, pk=pk, tenant=tenant, user_id=user_id)
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
