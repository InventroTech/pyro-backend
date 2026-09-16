from django.utils import timezone
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from authz.permissions import IsTenantAuthenticated

from .models import InAppNotification
from .serializers import InAppNotificationSerializer


def _current_user_id(request) -> str | None:
    user = getattr(request, "user", None)
    if user is None or getattr(user, "is_anonymous", True):
        return None
    uid = getattr(user, "supabase_uid", None)
    return str(uid).strip() if uid else None


class InAppNotificationListView(APIView):
    """
    GET /notifications/
    Returns unread in-app notifications for the current user (default).
    Pass ?include_read=true to include read ones.
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        user_id = _current_user_id(request)
        if not user_id:
            return Response({"error": "Unauthorized"}, status=status.HTTP_401_UNAUTHORIZED)

        qs = InAppNotification.objects.filter(user_id=user_id)
        tenant = getattr(request, "tenant", None)
        if tenant is not None:
            qs = qs.filter(tenant_id=tenant.id)

        include_read = str(request.query_params.get("include_read", "")).lower() in {
            "1",
            "true",
            "yes",
        }
        if not include_read:
            qs = qs.filter(read_at__isnull=True)

        rows = list(qs.order_by("-created_at")[:50])
        return Response(
            {
                "count": len(rows),
                "results": InAppNotificationSerializer(rows, many=True).data,
            },
            status=status.HTTP_200_OK,
        )


class InAppNotificationMarkReadView(APIView):
    """
    POST /notifications/<id>/read/
    Marks a single notification as read so it no longer appears in the inbox.
    """

    permission_classes = [IsTenantAuthenticated]

    def post(self, request, pk: int):
        user_id = _current_user_id(request)
        if not user_id:
            return Response({"error": "Unauthorized"}, status=status.HTTP_401_UNAUTHORIZED)

        qs = InAppNotification.objects.filter(pk=pk, user_id=user_id)
        tenant = getattr(request, "tenant", None)
        if tenant is not None:
            qs = qs.filter(tenant_id=tenant.id)

        notification = qs.first()
        if not notification:
            return Response({"error": "Notification not found"}, status=status.HTTP_404_NOT_FOUND)

        if notification.read_at is None:
            notification.read_at = timezone.now()
            notification.save(update_fields=["read_at", "updated_at"])

        return Response(
            InAppNotificationSerializer(notification).data,
            status=status.HTTP_200_OK,
        )
