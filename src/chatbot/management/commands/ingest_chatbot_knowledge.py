"""
Ingest markdown knowledge files into KnowledgeDocument + embedded KnowledgeChunk rows.

Usage:
  python manage.py ingest_chatbot_knowledge
  python manage.py ingest_chatbot_knowledge --domain erp
"""

from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand

from chatbot.models import KnowledgeChunk, KnowledgeDocument
from chatbot.services.embeddings import embed_texts

KNOWLEDGE_DIR = Path(__file__).resolve().parents[2] / "knowledge"

DOMAIN_BY_PREFIX = {
    "crm_": "crm",
    "erp_": "erp",
    "general_": "general",
}

CHUNK_SIZE = 900
CHUNK_OVERLAP = 120


def _domain_for(filename: str) -> str:
    for prefix, domain in DOMAIN_BY_PREFIX.items():
        if filename.startswith(prefix):
            return domain
    return "general"


def _chunk_text(text: str) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    chunks = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + CHUNK_SIZE, n)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= n:
            break
        start = max(0, end - CHUNK_OVERLAP)
    return chunks


class Command(BaseCommand):
    help = "Ingest chatbot/knowledge/*.md into embedded knowledge chunks (global, tenant=NULL)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--domain",
            type=str,
            default="",
            help="Optional filter: crm | erp | general",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Parse and chunk only; do not write or embed.",
        )
        parser.add_argument(
            "--skip-embeddings",
            action="store_true",
            help="Store chunks without calling OpenAI embeddings (keyword RAG fallback).",
        )

    def handle(self, *args, **options):
        domain_filter = (options.get("domain") or "").strip().lower()
        dry_run = bool(options.get("dry_run"))
        skip_embeddings = bool(options.get("skip_embeddings"))

        if not KNOWLEDGE_DIR.exists():
            self.stderr.write(f"Knowledge dir missing: {KNOWLEDGE_DIR}")
            return

        files = sorted(KNOWLEDGE_DIR.glob("*.md"))
        if not files:
            self.stdout.write("No markdown files found.")
            return

        total_chunks = 0
        for path in files:
            domain = _domain_for(path.name)
            if domain_filter and domain != domain_filter:
                continue
            slug = path.stem
            content = path.read_text(encoding="utf-8")
            title = slug.replace("_", " ").title()
            for line in content.splitlines():
                if line.startswith("# "):
                    title = line[2:].strip()
                    break

            pieces = _chunk_text(content)
            self.stdout.write(f"{path.name}: domain={domain}, chunks={len(pieces)}")
            if dry_run:
                total_chunks += len(pieces)
                continue

            doc = (
                KnowledgeDocument.objects.filter(tenant__isnull=True, slug=slug)
                .order_by("-updated_at")
                .first()
            )
            if doc is None:
                doc = KnowledgeDocument(
                    tenant=None,
                    slug=slug,
                    title=title,
                    domain=domain,
                    source_path=str(path.relative_to(path.parents[2])),
                    content=content,
                )
            else:
                doc.title = title
                doc.domain = domain
                doc.source_path = str(path.relative_to(path.parents[2]))
                doc.content = content
            doc.save()

            # Replace chunks
            KnowledgeChunk.objects.filter(document=doc).delete()
            embeddings: list[list[float]] = []
            if pieces and not skip_embeddings:
                try:
                    embeddings = embed_texts(pieces)
                except Exception as exc:
                    self.stderr.write(
                        self.style.WARNING(
                            f"Embedding failed for {path.name} ({exc}); "
                            "storing chunks without vectors (keyword RAG fallback)."
                        )
                    )
                    embeddings = [[] for _ in pieces]
            elif pieces:
                embeddings = [[] for _ in pieces]
            to_create = []
            for idx, piece in enumerate(pieces):
                emb = embeddings[idx] if idx < len(embeddings) else []
                to_create.append(
                    KnowledgeChunk(
                        tenant=None,
                        document=doc,
                        chunk_index=idx,
                        content=piece,
                        embedding=emb,
                        token_count=len(piece.split()),
                        metadata={"source": path.name},
                    )
                )
            KnowledgeChunk.objects.bulk_create(to_create)
            total_chunks += len(to_create)

        self.stdout.write(self.style.SUCCESS(f"Done. chunks={total_chunks} dry_run={dry_run}"))
