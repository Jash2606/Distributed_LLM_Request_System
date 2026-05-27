"""Add partial index for hot queue claim query

Revision ID: 002
Revises: 001
Create Date: 2024-01-01 00:00:01.000000

SHOULD FIX #2 — Partial index for the processing_jobs claim query.

PROBLEM:
  The claim query filters WHERE status='queued' AND scheduled_at<=now().
  The existing index ix_processing_jobs_claim covers (status, priority, scheduled_at)
  but includes ALL rows — completed, failed, dead, and processing rows are in the
  index even though they are never accessed by the claim query.

  As the table grows (millions of completed rows), the B-tree index bloats:
    - Index pages grow → more I/O per scan
    - Index maintenance on every INSERT/UPDATE slows writes
    - Buffer pool fills with cold index pages, evicting hot data

SOLUTION:
  A partial index that only indexes rows WHERE status='queued'.
  At any given time, the number of queued jobs is small (< worker count × 10)
  while completed/failed rows accumulate without bound.  The partial index
  stays tiny regardless of table growth.

  Before:  Planner scans ix_processing_jobs_claim over ALL rows to find the
           few 'queued' ones.
  After:   Planner uses ix_processing_jobs_queued_priority — contains ONLY
           'queued' rows, sorted by priority then scheduled_at.  The claim
           query becomes an index scan on an always-small structure.

WRITE TRADEOFF:
  Partial indexes have lower write overhead than full indexes because fewer
  rows qualify (only status='queued').  INSERT into processing_jobs adds one
  entry to this index.  When a job is claimed (status → 'processing'), the
  entry is removed from the index.  Net index size stays bounded by queue depth.
"""
from typing import Sequence, Union
from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE INDEX ix_processing_jobs_queued_priority
        ON processing_jobs (priority ASC, scheduled_at ASC)
        WHERE status = 'queued'
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_processing_jobs_queued_priority")
