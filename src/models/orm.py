from datetime import datetime
from typing import Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger, Boolean, DateTime, Integer, LargeBinary,
    SmallInteger, String, Text, func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class PromptRequestORM(Base):
    """M (Model) — source-of-truth request record."""
    __tablename__ = "prompt_requests"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(255), nullable=False)
    prompt_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    text_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False, index=True)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=2)
    embedding: Mapped[Optional[list]] = mapped_column(Vector(384), nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="received")
    cached: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    response: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=func.now(), onupdate=func.now(),
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    def processing_time_ms(self) -> Optional[int]:
        if self.completed_at and self.created_at:
            delta = self.completed_at - self.created_at
            return int(delta.total_seconds() * 1000)
        return None


class ProcessingJobORM(Base):
    """M (Model) — durable job queue record."""
    __tablename__ = "processing_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    prompt_request_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="queued", index=True)
    worker_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=2)
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )
    locked_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def lease_expired(self, now: datetime) -> bool:
        return self.locked_until is not None and self.locked_until < now

    def can_retry(self) -> bool:
        return self.attempt_count < self.max_attempts


class SemanticCacheORM(Base):
    """M (Model) — vector semantic cache."""
    __tablename__ = "semantic_cache"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    prompt_text: Mapped[str] = mapped_column(Text, nullable=False)
    text_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False, unique=True, index=True)
    embedding: Mapped[list] = mapped_column(Vector(384), nullable=False)
    response: Mapped[str] = mapped_column(Text, nullable=False)
    source_prompt_id: Mapped[str] = mapped_column(String(255), nullable=False)
    hit_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_hit_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

