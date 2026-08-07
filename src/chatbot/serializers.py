from rest_framework import serializers

from chatbot.models import Conversation, Message


class MessageSerializer(serializers.ModelSerializer):
    class Meta:
        model = Message
        fields = (
            "id",
            "role",
            "content",
            "mode",
            "sources",
            "tool_calls",
            "page_context",
            "meta",
            "created_at",
        )
        read_only_fields = fields


class ConversationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Conversation
        fields = ("id", "title", "created_at", "updated_at")
        read_only_fields = fields


class ConversationDetailSerializer(serializers.ModelSerializer):
    messages = MessageSerializer(many=True, read_only=True)

    class Meta:
        model = Conversation
        fields = ("id", "title", "created_at", "updated_at", "messages")
        read_only_fields = fields


class SendMessageSerializer(serializers.Serializer):
    message = serializers.CharField(max_length=8000)
    page_context = serializers.DictField(required=False, default=dict)
    conversation_id = serializers.UUIDField(required=False, allow_null=True)


class CreateConversationSerializer(serializers.Serializer):
    title = serializers.CharField(max_length=255, required=False, allow_blank=True, default="")
