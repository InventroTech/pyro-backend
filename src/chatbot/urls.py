from django.urls import path

from chatbot.views import (
    ChatQuickView,
    ConversationDetailView,
    ConversationListCreateView,
    ConversationMessagesView,
)

app_name = "chatbot"

urlpatterns = [
    path("ask/", ChatQuickView.as_view(), name="ask"),
    path("conversations/", ConversationListCreateView.as_view(), name="conversations"),
    path(
        "conversations/<uuid:conversation_id>/",
        ConversationDetailView.as_view(),
        name="conversation-detail",
    ),
    path(
        "conversations/<uuid:conversation_id>/messages/",
        ConversationMessagesView.as_view(),
        name="conversation-messages",
    ),
]
