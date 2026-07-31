from uuid import UUID

from django.utils import timezone
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from authz.permissions import IsTenantAuthenticated
from chatbot.models import Conversation, Message
from chatbot.serializers import (
    ConversationDetailSerializer,
    ConversationSerializer,
    CreateConversationSerializer,
    MessageSerializer,
    SendMessageSerializer,
)
from chatbot.services.orchestrator import handle_message


def _current_user_id(request):
    uid = getattr(request.user, "supabase_uid", None)
    if not uid:
        return None
    try:
        return UUID(uid) if isinstance(uid, str) else uid
    except (ValueError, TypeError):
        return None


def _require_tenant_user(request):
    if not getattr(request, "tenant", None):
        return None, None, Response(
            {"error": "Tenant required"},
            status=status.HTTP_400_BAD_REQUEST,
        )
    user_id = _current_user_id(request)
    if not user_id:
        return None, None, Response(
            {"error": "Authenticated user required"},
            status=status.HTTP_401_UNAUTHORIZED,
        )
    return request.tenant, user_id, None


def _append_exchange(tenant, conv, text, page_context, user_id=None):
    """Persist user message, run orchestrator, persist assistant reply."""
    user_msg = Message.objects.create(
        tenant=tenant,
        conversation=conv,
        role=Message.ROLE_USER,
        content=text,
        page_context=page_context or {},
    )
    history = [
        {"role": m.role, "content": m.content}
        for m in conv.messages.exclude(id=user_msg.id).order_by("created_at")
        if m.role in (Message.ROLE_USER, Message.ROLE_ASSISTANT)
    ]
    result = handle_message(
        text,
        tenant=tenant,
        user_id=user_id,
        history=history,
        page_context=page_context or {},
    )
    assistant_msg = Message.objects.create(
        tenant=tenant,
        conversation=conv,
        role=Message.ROLE_ASSISTANT,
        content=result.get("answer") or "",
        mode=result.get("mode") or "",
        sources=result.get("sources") or [],
        tool_calls=result.get("tool_calls") or [],
        meta={"intent": result.get("intent")},
    )
    if not conv.title or conv.title == "New chat":
        conv.title = text[:80]
    conv.updated_at = timezone.now()
    conv.save(update_fields=["title", "updated_at"])
    return user_msg, assistant_msg, result


class ConversationListCreateView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def get(self, request):
        tenant, user_id, err = _require_tenant_user(request)
        if err:
            return err
        qs = Conversation.objects.filter(tenant=tenant, user_id=user_id).order_by(
            "-updated_at"
        )[:50]
        return Response(ConversationSerializer(qs, many=True).data)

    def post(self, request):
        tenant, user_id, err = _require_tenant_user(request)
        if err:
            return err
        ser = CreateConversationSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        conv = Conversation.objects.create(
            tenant=tenant,
            user_id=user_id,
            title=(ser.validated_data.get("title") or "New chat")[:255],
        )
        return Response(ConversationSerializer(conv).data, status=status.HTTP_201_CREATED)


class ConversationDetailView(APIView):
    permission_classes = [IsTenantAuthenticated]

    def get(self, request, conversation_id):
        tenant, user_id, err = _require_tenant_user(request)
        if err:
            return err
        conv = (
            Conversation.objects.filter(
                id=conversation_id, tenant=tenant, user_id=user_id
            )
            .prefetch_related("messages")
            .first()
        )
        if not conv:
            return Response({"error": "Not found"}, status=status.HTTP_404_NOT_FOUND)
        return Response(ConversationDetailSerializer(conv).data)


class ConversationMessagesView(APIView):
    """
    GET/POST /chat/conversations/<id>/messages/
    Send a user message and get the hybrid assistant reply (RAG + CRM/ERP tools).
    """

    permission_classes = [IsTenantAuthenticated]

    def get(self, request, conversation_id):
        tenant, user_id, err = _require_tenant_user(request)
        if err:
            return err
        conv = Conversation.objects.filter(
            id=conversation_id, tenant=tenant, user_id=user_id
        ).first()
        if not conv:
            return Response({"error": "Not found"}, status=status.HTTP_404_NOT_FOUND)
        messages = conv.messages.order_by("created_at")
        return Response(MessageSerializer(messages, many=True).data)

    def post(self, request, conversation_id):
        tenant, user_id, err = _require_tenant_user(request)
        if err:
            return err
        conv = Conversation.objects.filter(
            id=conversation_id, tenant=tenant, user_id=user_id
        ).first()
        if not conv:
            return Response({"error": "Not found"}, status=status.HTTP_404_NOT_FOUND)

        ser = SendMessageSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        try:
            user_msg, assistant_msg, result = _append_exchange(
                tenant,
                conv,
                ser.validated_data["message"],
                ser.validated_data.get("page_context") or {},
                user_id=user_id,
            )
        except Exception as exc:
            return Response(
                {"error": f"Chat failed: {exc}"},
                status=status.HTTP_502_BAD_GATEWAY,
            )

        return Response(
            {
                "conversation_id": str(conv.id),
                "user_message": MessageSerializer(user_msg).data,
                "assistant_message": MessageSerializer(assistant_msg).data,
                "answer": assistant_msg.content,
                "mode": assistant_msg.mode,
                "sources": assistant_msg.sources,
                "intent": result.get("intent"),
            },
            status=status.HTTP_201_CREATED,
        )


class ChatQuickView(APIView):
    """
    POST /chat/ask/
    One-shot ask: creates a conversation if needed, then answers.
    Body: { message, conversation_id?, page_context? }
    """

    permission_classes = [IsTenantAuthenticated]

    def post(self, request):
        tenant, user_id, err = _require_tenant_user(request)
        if err:
            return err

        ser = SendMessageSerializer(data=request.data)
        ser.is_valid(raise_exception=True)

        conversation_id = ser.validated_data.get("conversation_id")
        if conversation_id:
            conv = Conversation.objects.filter(
                id=conversation_id, tenant=tenant, user_id=user_id
            ).first()
            if not conv:
                return Response(
                    {"error": "Conversation not found"},
                    status=status.HTTP_404_NOT_FOUND,
                )
        else:
            conv = Conversation.objects.create(
                tenant=tenant,
                user_id=user_id,
                title="New chat",
            )

        try:
            user_msg, assistant_msg, result = _append_exchange(
                tenant,
                conv,
                ser.validated_data["message"],
                ser.validated_data.get("page_context") or {},
                user_id=user_id,
            )
        except Exception as exc:
            return Response(
                {"error": f"Chat failed: {exc}"},
                status=status.HTTP_502_BAD_GATEWAY,
            )

        return Response(
            {
                "conversation_id": str(conv.id),
                "answer": assistant_msg.content,
                "mode": assistant_msg.mode,
                "sources": assistant_msg.sources,
                "intent": result.get("intent"),
                "user_message": MessageSerializer(user_msg).data,
                "assistant_message": MessageSerializer(assistant_msg).data,
            },
            status=status.HTTP_201_CREATED,
        )
