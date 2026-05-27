"""
Semantic cache service — three-tier lookup:
  1. Exact text_hash match (O(1), no vector compute)
  2. pgvector cosine similarity top-1 (approximate nearest neighbour)
  3. Miss → caller invokes LLM

Follows ISP: ICacheService exposes only lookup + store.

SHOULD FIX #3 applied here:
  embed() is CPU-bound (NumPy or real model inference).  Calling it on the
  event loop thread blocks ALL other coroutines for the duration.  At 4 concurrent
  workers, each blocking ~10 µs (mock) to ~100 ms (real model), the event loop
  becomes unavailable for DB I/O, Redis, and FastAPI request handling.
  asyncio.to_thread() runs embed() in the default ThreadPoolExecutor so the event
  loop remains free during embedding computation.
"""

import asyncio
import hashlib
from dataclasses import dataclass
from typing import Optional

from src.db.repositories.semantic_cache import SemanticCacheRepository
from src.models.orm import SemanticCacheORM
from src.services.embeddings import IEmbeddingProvider


@dataclass
class CacheHit:
    entry: SemanticCacheORM
    similarity: float
    match_type: str  # "exact" | "semantic"


class SemanticCacheService:
    def __init__(
        self,
        repo: SemanticCacheRepository,
        embedder: IEmbeddingProvider,
        threshold: float = 0.9,
    ):
        self._repo = repo
        self._embedder = embedder
        self._threshold = threshold

    @staticmethod
    def hash_text(text: str) -> bytes:
        normalized = " ".join(text.lower().split())
        return hashlib.sha256(normalized.encode()).digest()

    async def lookup(self, text: str) -> tuple[Optional[CacheHit], list[float]]:
        """
        Returns (CacheHit | None, embedding).
        Embedding is always computed so the caller can store it on a miss.

        Tier 1 (exact hash) is checked first — if it hits, embed() is NOT called.
        embed() is only needed for tier-2 vector search, so the fast path avoids
        any CPU work entirely.
        """
        text_hash = self.hash_text(text)

        # Tier 1 — exact hash match (fastest path, no embedding needed)
        exact = await self._repo.find_exact(text_hash)
        if exact is not None:
            await self._repo.increment_hit_count(exact.id)
            return CacheHit(entry=exact, similarity=1.0, match_type="exact"), list(exact.embedding)

        # SHOULD FIX #3: asyncio.to_thread() moves CPU-bound embed() off the event loop.
        #
        # WHY THIS MATTERS:
        #   IEmbeddingProvider.embed() is synchronous.  For MockEmbeddingProvider it
        #   runs NumPy (~10 µs); for SentenceTransformerProvider it runs full model
        #   inference (~50-200 ms on CPU).  Both block the event loop thread.
        #
        #   With 4 worker slots in one process, sequential event-loop blocking means:
        #     mock:  4 × 10 µs = 40 µs blocked per second — acceptable
        #     real:  4 × 100 ms = 400 ms blocked per second — catastrophic
        #
        #   asyncio.to_thread() submits embed() to the OS thread pool.  The event
        #   loop continues handling DB I/O, Redis, and other coroutines while the
        #   embedding runs in a background thread.  NumPy releases the GIL for its
        #   core computation, so threads run in true parallel on multi-core hardware.
        #
        # TRADEOFF:
        #   Thread dispatch overhead is ~10 µs.  For the mock provider this adds ~100%
        #   overhead on the embed call itself, but the absolute time is negligible
        #   compared to DB round-trips.  For real models it is completely hidden.
        embedding = await asyncio.to_thread(self._embedder.embed, text)

        # Tier 2 — vector similarity
        nearest = await self._repo.find_nearest(embedding, self._threshold)
        if nearest is not None:
            entry, similarity = nearest
            await self._repo.increment_hit_count(entry.id)
            return CacheHit(entry=entry, similarity=similarity, match_type="semantic"), embedding

        return None, embedding

    async def store(
        self,
        prompt_id: str,
        text: str,
        embedding: list[float],
        response: str,
    ) -> SemanticCacheORM:
        text_hash = self.hash_text(text)
        entry = SemanticCacheORM(
            prompt_text=text,
            text_hash=text_hash,
            embedding=embedding,
            response=response,
            source_prompt_id=prompt_id,
            hit_count=0,
        )
        return await self._repo.upsert(entry)
