"""initial schema

Revision ID: 001
Revises:
Create Date: 2024-01-01 00:00:00.000000
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from pgvector.sqlalchemy import Vector

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Enable pgvector extension
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # prompt_requests — source-of-truth request record
    op.create_table(
        "prompt_requests",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.String(255), nullable=False),
        sa.Column("prompt_id", sa.String(255), nullable=False),
        sa.Column("text", sa.Text, nullable=False),
        sa.Column("text_hash", sa.LargeBinary(32), nullable=False),
        sa.Column("priority", sa.SmallInteger, nullable=False, server_default="2"),
        sa.Column("embedding", Vector(384), nullable=True),
        sa.Column("status", sa.String(50), nullable=False, server_default="received"),
        sa.Column("cached", sa.Boolean, nullable=True),
        sa.Column("response", sa.Text, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("retry_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_prompt_requests_prompt_id", "prompt_requests", ["prompt_id"], unique=True)
    op.create_index("ix_prompt_requests_text_hash", "prompt_requests", ["text_hash"])
    op.create_index("ix_prompt_requests_status_created", "prompt_requests", ["status", "created_at"])
    op.create_index("ix_prompt_requests_user_id", "prompt_requests", ["user_id", "created_at"])

    # processing_jobs — durable queue
    op.create_table(
        "processing_jobs",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("prompt_request_id", sa.BigInteger, nullable=False),
        sa.Column("status", sa.String(50), nullable=False, server_default="queued"),
        sa.Column("worker_id", sa.String(255), nullable=True),
        sa.Column("attempt_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer, nullable=False, server_default="5"),
        sa.Column("priority", sa.SmallInteger, nullable=False, server_default="2"),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["prompt_request_id"], ["prompt_requests.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_processing_jobs_request_id", "processing_jobs", ["prompt_request_id"], unique=True)
    op.create_index("ix_processing_jobs_claim", "processing_jobs", ["status", "priority", "scheduled_at"])
    op.create_index("ix_processing_jobs_reaper", "processing_jobs", ["status", "locked_until"])

    # semantic_cache — vector cache
    op.create_table(
        "semantic_cache",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("prompt_text", sa.Text, nullable=False),
        sa.Column("text_hash", sa.LargeBinary(32), nullable=False),
        sa.Column("embedding", Vector(384), nullable=False),
        sa.Column("response", sa.Text, nullable=False),
        sa.Column("source_prompt_id", sa.String(255), nullable=False),
        sa.Column("hit_count", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("last_hit_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_semantic_cache_text_hash", "semantic_cache", ["text_hash"], unique=True)
    # HNSW index for fast approximate nearest-neighbor search
    op.execute(
        "CREATE INDEX ix_semantic_cache_embedding_hnsw "
        "ON semantic_cache USING hnsw (embedding vector_cosine_ops)"
    )


def downgrade() -> None:
    op.drop_table("semantic_cache")
    op.drop_table("processing_jobs")
    op.drop_table("prompt_requests")
    op.execute("DROP EXTENSION IF EXISTS vector")
