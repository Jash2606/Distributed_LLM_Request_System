from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.repositories.base import BaseRepository
from src.models.orm import SemanticCacheORM


def _parse_vector(value) -> list[float]:
    """
    asyncpg returns VECTOR columns as raw strings ('[0.1, 0.2, ...]') when
    queried via text() SQL.  SQLAlchemy ORM queries go through pgvector's
    type adapter and return a list directly.  This normalises both.
    """
    if isinstance(value, (list, tuple)):
        return [float(x) for x in value]
    if hasattr(value, "tolist"):          # numpy array
        return value.tolist()
    if isinstance(value, str):
        return [float(x) for x in value.strip("[]{}").split(",")]
    return list(value)


def _vec_to_pg(embedding: list[float]) -> str:
    """Format a Python float list as a PostgreSQL vector literal."""
    return "[" + ",".join(repr(float(x)) for x in embedding) + "]"


class SemanticCacheRepository(BaseRepository):
    """Repository for semantic_cache table (M in MVC)."""

    async def find_exact(self, text_hash: bytes) -> Optional[SemanticCacheORM]:
        """Fast O(1) exact hash lookup — runs before any vector search."""
        async with self._session() as session:
            result = await session.execute(
                select(SemanticCacheORM).where(SemanticCacheORM.text_hash == text_hash)
            )
            row = result.scalar_one_or_none()
            if row is None:
                return None
            # Normalise embedding type (ORM path returns list via pgvector adapter)
            if row.embedding is not None:
                row.embedding = _parse_vector(row.embedding)
            return row

    async def find_nearest(
        self,
        embedding: list[float],
        threshold: float,
    ) -> Optional[tuple[SemanticCacheORM, float]]:
        """
        pgvector cosine similarity top-1.
        Returns (entry, similarity) if similarity > threshold, else None.
        """
        vec_str = _vec_to_pg(embedding)
        async with self._session() as session:
            result = await session.execute(
                text("""
                    SELECT id, prompt_text, text_hash, response,
                           source_prompt_id, hit_count, created_at, last_hit_at,
                           embedding::text AS embedding_str,
                           1 - (embedding <=> :emb ::vector) AS similarity
                    FROM semantic_cache
                    ORDER BY embedding <=> :emb ::vector
                    LIMIT 1
                """),
                {"emb": vec_str},
            )
            row = result.mappings().first()
            if row is None:
                return None
            similarity = float(row["similarity"])
            if similarity <= threshold:
                return None

            entry = SemanticCacheORM(
                id=row["id"],
                prompt_text=row["prompt_text"],
                text_hash=bytes(row["text_hash"]),
                embedding=_parse_vector(row["embedding_str"]),
                response=row["response"],
                source_prompt_id=row["source_prompt_id"],
                hit_count=row["hit_count"],
                created_at=row["created_at"],
                last_hit_at=row["last_hit_at"],
            )
            return entry, similarity

    async def upsert(self, entry: SemanticCacheORM) -> SemanticCacheORM:
        """
        Insert on text_hash conflict — idempotent upsert.
        If the same hash already exists (worker crash + retry), skip quietly.
        """
        async with self._session() as session:
            async with session.begin():
                stmt = (
                    pg_insert(SemanticCacheORM)
                    .values(
                        prompt_text=entry.prompt_text,
                        text_hash=entry.text_hash,
                        embedding=entry.embedding,
                        response=entry.response,
                        source_prompt_id=entry.source_prompt_id,
                        hit_count=0,
                        created_at=datetime.now(timezone.utc),
                    )
                    .on_conflict_do_nothing(index_elements=["text_hash"])
                )
                await session.execute(stmt)
            return entry

    async def increment_hit_count(self, entry_id: int) -> None:
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    update(SemanticCacheORM)
                    .where(SemanticCacheORM.id == entry_id)
                    .values(
                        hit_count=SemanticCacheORM.hit_count + 1,
                        last_hit_at=datetime.now(timezone.utc),
                    )
                )
