"""RAG retrieve + answer over KnowledgeChunk embeddings."""

from __future__ import annotations

from typing import Any, Optional

from django.db.models import Q

from chatbot.constants import RAG_TOP_K
from chatbot.models import KnowledgeChunk
from chatbot.services.embeddings import embed_query, top_k_by_similarity
from chatbot.services.llm import chat_completion, message_text


SYSTEM_PROMPT = (
    "You are Sparky, Pyro's friendly product assistant for CRM and ERP workflows. "
    "Answer ONLY using the provided context snippets. "
    "If the context is insufficient, say you don't know and suggest what the user can ask. "
    "Be concise and practical — short paragraphs or a few bullets. "
    "Use light Markdown (bold, short headings, lists). "
    "Do not dump long 'Cannot perform' / limitation essays unless the user asks what you cannot do. "
    "Never invent tool capabilities you do not have. "
    "When introducing yourself, call yourself Sparky."
)


def _chunk_queryset(tenant) -> Any:
    """Global docs (tenant NULL) plus tenant-specific docs."""
    qs = KnowledgeChunk.objects.select_related("document").filter(
        embedding__isnull=False,
    ).exclude(embedding=[])
    if tenant is not None:
        return qs.filter(Q(tenant=tenant) | Q(tenant__isnull=True) | Q(document__tenant__isnull=True))
    return qs.filter(Q(tenant__isnull=True) | Q(document__tenant__isnull=True))


def _chunk_to_result(chunk: KnowledgeChunk, score: float) -> dict[str, Any]:
    doc = chunk.document
    return {
        "chunk_id": str(chunk.id),
        "document_id": str(doc.id),
        "title": doc.title,
        "domain": doc.domain,
        "slug": doc.slug,
        "content": chunk.content,
        "score": round(float(score), 4),
    }


def _keyword_retrieve(
    question: str,
    *,
    tenant=None,
    top_k: int = RAG_TOP_K,
    domain: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Fallback when embeddings are missing / unavailable."""
    qs = KnowledgeChunk.objects.select_related("document")
    if tenant is not None:
        qs = qs.filter(Q(tenant=tenant) | Q(tenant__isnull=True) | Q(document__tenant__isnull=True))
    else:
        qs = qs.filter(Q(tenant__isnull=True) | Q(document__tenant__isnull=True))
    if domain:
        qs = qs.filter(document__domain=domain)

    tokens = [t for t in (question or "").lower().split() if len(t) > 2][:8]
    if tokens:
        q_filter = Q()
        for t in tokens:
            q_filter |= Q(content__icontains=t) | Q(document__title__icontains=t)
        qs = qs.filter(q_filter)

    results = []
    for chunk in qs.order_by("-updated_at")[:top_k]:
        results.append(_chunk_to_result(chunk, 0.0))
    if results:
        return results
    # Last resort: return newest chunks so help mode is never empty after ingest
    fallback_qs = KnowledgeChunk.objects.select_related("document").order_by("-updated_at")
    if domain:
        fallback_qs = fallback_qs.filter(document__domain=domain)
    return [_chunk_to_result(c, 0.0) for c in fallback_qs[:top_k]]


def retrieve_chunks(
    question: str,
    *,
    tenant=None,
    top_k: int = RAG_TOP_K,
    domain: Optional[str] = None,
) -> list[dict[str, Any]]:
    qs = _chunk_queryset(tenant)
    if domain:
        qs = qs.filter(document__domain=domain)

    candidates = []
    for chunk in qs.iterator(chunk_size=200):
        emb = chunk.embedding or []
        if isinstance(emb, list) and emb:
            candidates.append((chunk, emb))

    if not candidates:
        return _keyword_retrieve(question, tenant=tenant, top_k=top_k, domain=domain)

    try:
        query_vec = embed_query(question)
    except Exception:
        return _keyword_retrieve(question, tenant=tenant, top_k=top_k, domain=domain)

    if not query_vec:
        return _keyword_retrieve(question, tenant=tenant, top_k=top_k, domain=domain)

    ranked = top_k_by_similarity(query_vec, candidates, k=top_k)
    return [_chunk_to_result(chunk, score) for chunk, score in ranked]


def answer_from_rag(
    question: str,
    *,
    tenant=None,
    history: Optional[list[dict[str, str]]] = None,
    page_context: Optional[dict] = None,
) -> dict[str, Any]:
    chunks = retrieve_chunks(question, tenant=tenant)
    if not chunks:
        return {
            "answer": (
                "I don't have enough product documentation ingested yet to answer that. "
                "Try asking about leads, tickets, inventory, or run the knowledge ingest command."
            ),
            "sources": [],
            "mode": "rag",
        }

    context_blocks = []
    for i, c in enumerate(chunks, start=1):
        context_blocks.append(
            f"[{i}] ({c['domain']}) {c['title']}\n{c['content']}"
        )
    context = "\n\n".join(context_blocks)

    page_note = ""
    if page_context:
        page_note = f"\nCurrent UI context: {page_context}\n"

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
    ]
    if history:
        messages.extend(history[-6:])
    messages.append(
        {
            "role": "user",
            "content": (
                f"{page_note}"
                f"Context:\n{context}\n\n"
                f"Question: {question}\n\n"
                "Answer using the context. Cite sources as [n] where helpful."
            ),
        }
    )

    try:
        response = chat_completion(messages, temperature=0.2)
        answer = message_text(response) or "I couldn't generate an answer."
    except Exception as exc:
        # Don't dump raw docs as the main UX — surface the real LLM failure.
        err = str(exc)
        if len(err) > 280:
            err = err[:280] + "…"
        answer = (
            "I found relevant docs, but the LLM call failed so I couldn't rewrite them into a short answer.\n\n"
            f"Error: {err}\n\n"
            "Check CHATBOT_PROVIDER / API key / model name, then restart the backend.\n\n"
            "Relevant docs:\n"
            + "\n\n".join(f"### {c['title']}\n{c['content'][:600]}" for c in chunks[:2])
        )

    return {
        "answer": answer,
        "sources": [
            {
                "title": c["title"],
                "domain": c["domain"],
                "slug": c["slug"],
                "score": c["score"],
                "type": "doc",
            }
            for c in chunks
        ],
        "mode": "rag",
    }
