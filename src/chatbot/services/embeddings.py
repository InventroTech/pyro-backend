import math
import os
from typing import Iterable, Sequence

from chatbot.constants import EMBEDDING_MODEL_DEFAULT, EMBEDDING_MODEL_ENV
from chatbot.services.llm import get_client


def embedding_model() -> str:
    return os.getenv(EMBEDDING_MODEL_ENV, EMBEDDING_MODEL_DEFAULT)


def embed_texts(texts: Sequence[str]) -> list[list[float]]:
    """Embed one or more texts with OpenAI embeddings API."""
    if not texts:
        return []
    client = get_client()
    response = client.embeddings.create(
        model=embedding_model(),
        input=list(texts),
    )
    # API returns data sorted by index
    sorted_data = sorted(response.data, key=lambda d: d.index)
    return [list(item.embedding) for item in sorted_data]


def embed_query(text: str) -> list[float]:
    vectors = embed_texts([text])
    return vectors[0] if vectors else []


def cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for x, y in zip(a, b):
        dot += x * y
        norm_a += x * x
        norm_b += y * y
    if norm_a <= 0 or norm_b <= 0:
        return 0.0
    return dot / (math.sqrt(norm_a) * math.sqrt(norm_b))


def top_k_by_similarity(
    query_vec: Sequence[float],
    candidates: Iterable[tuple[object, Sequence[float]]],
    k: int = 5,
) -> list[tuple[object, float]]:
    scored = [
        (item, cosine_similarity(query_vec, vec))
        for item, vec in candidates
        if vec
    ]
    scored.sort(key=lambda pair: pair[1], reverse=True)
    return scored[:k]
