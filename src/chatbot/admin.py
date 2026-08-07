from django.contrib import admin

from chatbot.models import Conversation, KnowledgeChunk, KnowledgeDocument, Message


@admin.register(Conversation)
class ConversationAdmin(admin.ModelAdmin):
    list_display = ("id", "tenant", "user_id", "title", "updated_at")
    list_filter = ("tenant",)
    search_fields = ("title", "user_id")


@admin.register(Message)
class MessageAdmin(admin.ModelAdmin):
    list_display = ("id", "conversation", "role", "mode", "created_at")
    list_filter = ("role", "mode")


@admin.register(KnowledgeDocument)
class KnowledgeDocumentAdmin(admin.ModelAdmin):
    list_display = ("slug", "title", "domain", "tenant", "updated_at")
    list_filter = ("domain",)
    search_fields = ("slug", "title")


@admin.register(KnowledgeChunk)
class KnowledgeChunkAdmin(admin.ModelAdmin):
    list_display = ("id", "document", "chunk_index", "token_count")
    list_filter = ("document__domain",)
