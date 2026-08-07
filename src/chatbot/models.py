import uuid

from django.db import models
from django.db.models import Q

from core.models import BaseModel
from core.soft_delete import alive_q
from object_history.models import HistoryTrackedModel


class Conversation(HistoryTrackedModel, BaseModel):
    """Tenant-scoped chat thread for one authenticated user."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user_id = models.UUIDField(
        db_index=True,
        help_text="Supabase auth user id who owns this conversation.",
    )
    title = models.CharField(max_length=255, blank=True, default="")

    class Meta(BaseModel.Meta):
        db_table = "chatbot_conversations"
        ordering = ["-updated_at"]
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["tenant", "user_id", "-updated_at"]),
        ]

    def __str__(self) -> str:
        return f"Conversation({self.id}, tenant={self.tenant_id}, user={self.user_id})"


class Message(HistoryTrackedModel, BaseModel):
    """One turn in a conversation (user / assistant / system / tool)."""

    ROLE_USER = "user"
    ROLE_ASSISTANT = "assistant"
    ROLE_SYSTEM = "system"
    ROLE_TOOL = "tool"
    ROLE_CHOICES = (
        (ROLE_USER, "User"),
        (ROLE_ASSISTANT, "Assistant"),
        (ROLE_SYSTEM, "System"),
        (ROLE_TOOL, "Tool"),
    )

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    conversation = models.ForeignKey(
        Conversation,
        on_delete=models.CASCADE,
        related_name="messages",
        db_column="conversation_id",
    )
    role = models.CharField(max_length=16, choices=ROLE_CHOICES, db_index=True)
    content = models.TextField(blank=True, default="")
    mode = models.CharField(
        max_length=32,
        blank=True,
        default="",
        help_text="rag | tools | hybrid | clarify",
    )
    sources = models.JSONField(default=list, blank=True)
    tool_calls = models.JSONField(default=list, blank=True)
    page_context = models.JSONField(default=dict, blank=True)
    meta = models.JSONField(default=dict, blank=True)

    class Meta(BaseModel.Meta):
        db_table = "chatbot_messages"
        ordering = ["created_at"]
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["conversation", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"Message({self.role}, {self.id})"


class KnowledgeDocument(HistoryTrackedModel, BaseModel):
    """
    Source document for RAG (product / CRM / ERP help).

    tenant=NULL means global (shared across tenants).
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    slug = models.SlugField(max_length=128)
    title = models.CharField(max_length=255)
    domain = models.CharField(
        max_length=32,
        default="general",
        db_index=True,
        help_text="crm | erp | general",
    )
    source_path = models.CharField(max_length=512, blank=True, default="")
    content = models.TextField()
    metadata = models.JSONField(default=dict, blank=True)

    class Meta(BaseModel.Meta):
        db_table = "chatbot_knowledge_documents"
        constraints = [
            models.UniqueConstraint(
                fields=["slug"],
                condition=alive_q() & Q(tenant__isnull=True),
                name="chatbot_knowledgedoc_global_slug_uniq_alive",
            ),
            models.UniqueConstraint(
                fields=["tenant", "slug"],
                condition=alive_q() & Q(tenant__isnull=False),
                name="chatbot_knowledgedoc_tenant_slug_uniq_alive",
            ),
        ]
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["domain", "slug"]),
        ]

    def __str__(self) -> str:
        return f"{self.title} ({self.domain})"


class KnowledgeChunk(HistoryTrackedModel, BaseModel):
    """Embedded text chunk for retrieval. Embedding stored as float list (JSON)."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    document = models.ForeignKey(
        KnowledgeDocument,
        on_delete=models.CASCADE,
        related_name="chunks",
        db_column="document_id",
    )
    chunk_index = models.PositiveIntegerField(default=0)
    content = models.TextField()
    embedding = models.JSONField(
        default=list,
        blank=True,
        help_text="Vector embedding as a list of floats.",
    )
    token_count = models.PositiveIntegerField(default=0)
    metadata = models.JSONField(default=dict, blank=True)

    class Meta(BaseModel.Meta):
        db_table = "chatbot_knowledge_chunks"
        ordering = ["document", "chunk_index"]
        indexes = [
            *BaseModel.Meta.indexes,
            models.Index(fields=["document", "chunk_index"]),
        ]

    def __str__(self) -> str:
        return f"Chunk({self.document_id}, #{self.chunk_index})"
