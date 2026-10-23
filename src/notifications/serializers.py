from rest_framework import serializers

from .models import InAppNotification


class InAppNotificationSerializer(serializers.ModelSerializer):
    is_read = serializers.BooleanField(read_only=True)

    class Meta:
        model = InAppNotification
        fields = (
            "id",
            "notification_type",
            "title",
            "message",
            "record_id",
            "tenant_id",
            "created_at",
            "updated_at",
            "read_at",
            "is_read",
        )
        read_only_fields = fields
