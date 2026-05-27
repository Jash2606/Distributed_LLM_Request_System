# Distributed LLM Request Processing System

> **A production-grade async prompt pipeline** — built from scratch without Celery, RabbitMQ, or Kafka. Just FastAPI, PostgreSQL, Redis, and a deep understanding of distributed systems.

---

## The Problem I Was Asked to Solve

Imagine you're building the backend for an LLM-powered product. Users send prompts. Your LLM provider is expensive, slow, and rate-limited. Requests come in concurrently. Workers can crash mid-job. The same prompt can arrive multiple times. You have a strict budget of **300 LLM calls per minute** shared across all workers.

**The requirements:**

| # | Requirement | Why It's Hard |
|---|-------------|---------------|
| 1 | Accept and queue prompts from multiple users | Concurrency — workers can't step on each other |
| 2 | Return results synchronously when possible | Can't block the HTTP thread — need async coordination |
| 3 | Never call the LLM twice for the same prompt | Idempotency — duplicate requests are a real problem |
| 4 | Return semantically similar cached responses | Can't just hash-match — "Explain AI" ≈ "What is artificial intelligence?" |
| 5 | Enforce a global 300 calls/min rate limit | Shared across all worker replicas — can't be per-process |
| 6 | Survive worker crashes without losing jobs | Crash recovery — a worker can die at any point mid-job |
| 7 | Support priority queuing (high/normal/low) | Low-priority jobs shouldn't block urgent ones |
| 8 | Retry failed jobs with backoff | Transient LLM failures should not permanently fail a job |
| 9 | Expose health checks and metrics | Observable in production — not a black box |

The assignment gave me a mock LLM with 200–500ms latency, 5% random failure rate, and a hard 300/min cap. Everything else was mine to design.

---

## My First Instinct — And Why I Abandoned It

When you hear "background job queue," your brain immediately goes to **Celery + Redis**. That's the industry default. I considered it seriously.

Here's what the first design looked like:

```
Client → FastAPI → Celery → Redis (broker) → Celery Workers → MockLLM
                         ↓
                   Redis (result backend)
```

**It seemed reasonable.** Celery handles retries, worker concurrency, routing. Redis is fast. The ecosystem is mature.

Then I asked: *what happens when a worker crashes mid-job?*

Celery's answer: it depends on whether you configured acknowledgement correctly, whether the broker is configured for durability, and whether your task function is idempotent. Three things you have to get right, all invisible in the code.

I needed to **show my work** — explicit lease management, explicit crash recovery, explicit retry logic. Celery hides all of that. If you put `@celery.task(max_retries=5)` and call it done, you've answered the easy question.

> **The real test wasn't "can you use a job queue."**
> **It was "do you understand what a job queue has to do to be correct."**

So I threw out Celery and built the queue myself.

---

## What Changed (And What I Discovered)

Replacing Celery forced me to think from first principles. Three realizations followed:

**Realization 1: PostgreSQL can be the queue.**
`SELECT ... FOR UPDATE SKIP LOCKED` is PostgreSQL's native answer to "give me the next job that nobody else is processing." It's ACID, durable, and trivially observable with plain SQL. No separate broker.

**Realization 2: PostgreSQL can be the vector database too.**
The `pgvector` extension adds cosine similarity search with HNSW indexing. Instead of standing up Pinecone or Qdrant as a third service, semantic caching lives in the same database as the source of truth. One fewer service to fail.

**Realization 3: Redis has exactly one job.**
Rate limiting. Atomic Lua scripts in Redis are perfect for the token bucket algorithm — one round-trip, no race condition. Nothing else belongs there.

The final architecture dropped from:
- `FastAPI + Celery + Redis (broker) + Redis (result backend) + Postgres + Pinecone`

...to:
- `FastAPI + PostgreSQL (queue + cache + source of truth) + Redis (rate limiter only)`

---

## What This System Can Do

### Core Capabilities

**Submit and forget — or submit and wait.**
`POST /process` is synchronous from the client's perspective: it waits up to 10 seconds for the worker to complete, then returns the final result inline. On a cache hit, that's under 50ms. If the 10-second window is exceeded (heavy load, slow LLM), the API returns `status: processing` and the client polls `GET /result/{id}`.

**Semantic cache with three lookup tiers.**
Before touching the LLM, every prompt goes through:
1. Exact SHA-256 hash match — sub-millisecond, no embedding needed
2. pgvector cosine ANN search — finds semantically similar cached responses
3. If both miss — rate check → LLM call → cache the response

The result: the LLM only gets called for genuinely new content.

**Idempotency that actually works.**
The same `prompt_id` + text always returns the same result, no matter how many times you call it or how many workers race to process it. A 5-case decision table handles every scenario — new, completed, in-flight, failed, or conflict. Enforced at the database layer with `UNIQUE(prompt_id)` so application-layer races can't win.

**Crash recovery with no manual intervention.**
Workers hold time-limited leases (15 seconds). A background heartbeat extends the lease every 5 seconds. If the worker dies — OOM, SIGKILL, network partition — the lease expires and the Reaper reclaims the job within 15 seconds. No human involvement. No stuck jobs.

**Priority queue that works under concurrency.**
High-priority jobs preempt normal and low. `ORDER BY priority, scheduled_at` in the claim query, backed by a partial index on `status = 'queued'` rows only (so the index stays tiny as historical rows accumulate).

**Global rate limiting shared across all workers.**
A Lua script in Redis implements a token bucket: one atomic operation per LLM call, no race conditions, shared across all 12 concurrent worker slots (3 replicas × 4 coroutines).

**Real-time completion via LISTEN/NOTIFY.**
No client-side polling required on the happy path. PostgreSQL's built-in pub/sub delivers the "job done" signal to the waiting API coroutine in milliseconds — not via polling, not via a message broker.

**Structured observability out of the box.**
Every log line is JSON. Every interesting metric (cache hit rate, LLM failure rate, queue depth, per-stage latency) is a Prometheus counter, gauge, or histogram at `/metrics`. The `/health` endpoint probes DB, Redis, and worker liveness.

### Numbers (with mock LLM)

| Scenario | Latency |
|----------|---------|
| Exact cache hit (Tier 1) | < 5ms |
| Semantic cache hit (Tier 2) | 5–20ms |
| LLM call (mock) | 200–500ms |
| POST /process timeout fallback | 10s |
| Worker crash → job reclaimed | ≤ 15s |
| Rate-limit deferral backoff | 200ms → exponential |
| Max concurrency | 12 jobs (3 replicas × 4 slots, configurable) |

---

## End Result

All 9 requirements from the problem statement were met. Here's how each one was delivered:

| # | Requirement | How It Was Solved |
|---|-------------|-------------------|
| 1 | Queue prompts from multiple users | `FOR UPDATE SKIP LOCKED` — workers claim jobs without stepping on each other |
| 2 | Return results synchronously when possible | `LISTEN/NOTIFY` — API waits up to 10s and returns inline; `GET /result` as fallback |
| 3 | Never call LLM twice for same prompt | 5-case idempotency table + `UNIQUE(prompt_id)` at DB level |
| 4 | Semantic similarity cache | 3-tier lookup: SHA-256 exact → pgvector cosine ANN → LLM |
| 5 | Global 300 calls/min rate limit | Redis Lua token bucket — atomic, shared across all 12 worker slots |
| 6 | Survive worker crashes | Lease + heartbeat + Reaper — crashed jobs reclaimed within 15 seconds |
| 7 | Priority queuing | `ORDER BY priority, scheduled_at` with a partial index on queued rows |
| 8 | Retry with backoff | Exponential backoff: 1s → 2s → 4s → 8s → 16s (capped at 30s), up to 5 attempts |
| 9 | Health checks and metrics | `/health` probes DB + Redis + worker liveness; `/metrics` serves Prometheus format |

---

### What This System Can Handle

**Concurrency**
- 12 simultaneous jobs out of the box (3 Docker replicas × 4 async coroutines each)
- Adding replicas scales linearly — `deploy.replicas: 10` gives 40 concurrent slots with zero code changes
- Each worker slot is async so DB and Redis I/O never block other slots within the same process

**Throughput**
- Cache hits (Tier 1 exact + Tier 2 semantic): effectively unlimited — no LLM token consumed, sub-20ms latency
- LLM calls: capped at 300/min by the Redis token bucket regardless of how many workers are running
- Queue: accepts up to 1000 in-flight jobs before returning 429 backpressure (configurable via `MAX_QUEUE_DEPTH`)

**Duplicate and concurrent traffic**
- Thousands of duplicate requests for the same `prompt_id` are handled correctly — only one LLM call ever, rest return from the idempotency cache
- Concurrent submits of the same `prompt_id` race to INSERT; the DB `UNIQUE` constraint settles it atomically, no 500 errors
- Clients that don't supply a `prompt_id` get deterministic auto-IDs — repeated calls are idempotent by default

**Failure resilience**
- A worker can crash at any point (after claim, during LLM call, after LLM but before write) — the job is reclaimed within `LEASE_SECONDS` (default 15s) and reprocessed by another worker
- Redis going down: rate limiter fails closed — no LLM calls go through, all jobs re-queue with backoff. System resumes automatically when Redis recovers
- PostgreSQL going down: API returns 500 immediately (no silent hanging). Workers retry with backoff. Reaper resumes on reconnect
- SIGTERM (graceful shutdown): in-flight jobs finish within their current pipeline step; the queue lease system ensures nothing is lost on restart

**Semantic deduplication**
- Any two prompts with cosine similarity ≥ 0.9 share a cached response — the LLM is never called twice for "Explain quantum computing" vs "Can you explain quantum computing simply?"
- Works from zero cached entries (HNSW index grows incrementally — no training step needed)

---

### Current Bottlenecks

These are the known limits of the current design — real issues that would need addressing before going to production at scale:

| Bottleneck | What breaks | Fix |
|------------|-------------|-----|
| **Single PostgreSQL node** | Writes saturate around ~5k–10k/s. Under heavy load the job queue table becomes a write hotspot | Add PgBouncer for connection pooling; read replica for `GET /result` polling; partition `processing_jobs` by date at high volume |
| **Global rate limit bucket** | One bucket shared by all users — a single heavy user can starve everyone else. No per-user quota | Add per-user Redis keys (`llm:ratelimit:{user_id}`) alongside the global bucket |
| **Mock embeddings ≠ real semantics** | SHA-256-seeded random vectors don't produce true semantic similarity — paraphrase detection doesn't work without a real model | Set `EMBED_PROVIDER=sentence_transformer` in production. The Strategy pattern means no code changes, just the env var |
| **One asyncpg connection per API waiter** | Each `POST /process` that waits opens its own PostgreSQL connection. At 100+ concurrent waiting requests, PostgreSQL connection count hits its limit | Add PgBouncer in transaction-pooling mode; or batch listeners onto fewer shared connections |
| **No authentication or per-user isolation** | Any caller can submit unlimited prompts and read any result by `prompt_id` | Add JWT/API key middleware at the FastAPI layer; scope `GET /result` to the requesting user |
| **No circuit breaker on the LLM** | If the LLM is fully down, every job retries 5 times with exponential backoff before dying — slow and wasteful | Add a circuit breaker: after N consecutive failures, fail fast immediately instead of burning all retries |
| **HNSW index recall at scale** | Beyond ~1M cached vectors, ANN recall degrades and index memory footprint grows large | Partition the cache by domain or date; or migrate semantic search to a dedicated vector DB (Qdrant, Pinecone) |
| **Single Uvicorn worker process** | The API runs as `--workers 1`. Multiple Uvicorn workers would improve CPU utilisation on the API side | Switch to `--workers 4` (one per core); all state is in DB/Redis so multi-process is safe |

---

## Architecture at a Glance

```
Client
  │
  POST /process
  │
  ▼
FastAPI API  ──── PromptProcessor Facade ─────────────────────┐
                   │                                          │
                   ├─ 1. Idempotency check (5-case table)    │
                   ├─ 2. Queue depth check → 429 if full     │
                   ├─ 3. Atomic INSERT: request + job         │
                   ├─ 4. LISTEN "prompt_done_N"  ◄─ FIRST    │
                   └─ 5. NOTIFY "job_queued"     ◄─ SECOND   │
                                                              │
  PostgreSQL ←──────────────────────────────────────────────┘
  (source of truth + job queue + vector cache)
       │
       │  FOR UPDATE SKIP LOCKED
       ▼
  Worker Pool  (3 replicas × 4 async coroutines = 12 concurrent slots)
       │
       ├─ JobClaimer ──→ claim job with lease
       ├─ HeartbeatManager ──→ renew lease every 5s
       ├─ Reaper ──→ reclaim expired leases every 5s
       │
       └─ PromptPipeline (Chain of Responsibility):
            ├─ Tier 1: exact SHA-256 hash   →  hit: return in <5ms
            ├─ Tier 2: pgvector cosine ANN  →  hit: return in <20ms
            ├─ Redis Token Bucket           →  denied: re-queue with backoff
            ├─ MockLLM (200–500ms)          →  call LLM
            └─ Cache store → NOTIFY "prompt_done_N" → API wakes → 200 OK

Redis
(rate limiter only — Lua token bucket, atomic, shared)
```

**MVC in the API layer:**
- `src/models/` + `src/db/repositories/` = **Model**
- `src/api/schemas.py` (Pydantic DTOs) = **View** (ORM never leaks to clients)
- `src/api/routes/` = **Controller**

For the full component breakdown, failure mode table, database design, concurrency model, and scalability analysis — see **[ARCHITECTURE.md](ARCHITECTURE.md)**.

---

## Quick Start

```bash
# 1. Copy env file (edit values if needed — this drives all Docker settings)
cp .env.example .env

# 2. Start everything — one command
docker-compose up --build
```

Wait ~20 seconds for Postgres, Redis, API migrations, and 3 worker replicas to stabilize.

> **How configuration works:** `docker-compose.yml` reads all settings from `.env` via `env_file: .env`. Only `DATABASE_URL` and `REDIS_URL` are overridden inside the compose file to use Docker service hostnames (`postgres`, `redis`) instead of `localhost`. Every other setting — timeouts, concurrency, LLM config, rate limits — is controlled entirely from `.env`.

```bash
# Submit a high-priority prompt
curl -X POST http://localhost:8000/process \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "alice",
    "prompt_id": "p-001",
    "text": "Explain quantum computing simply",
    "priority": "high"
  }'
```

```bash
# Submit without a prompt_id — auto-derived from sha256(user_id:text)
# Same user + same text always produces the same ID → idempotent by default
curl -X POST http://localhost:8000/process \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice", "text": "What is machine learning?"}'

# Poll result if POST returned status: processing
curl http://localhost:8000/result/p-001

# System health (DB + Redis + worker liveness)
curl http://localhost:8000/health

# Prometheus metrics
curl http://localhost:8000/metrics
```

---

## API Reference

### `POST /process` — Submit a Prompt

```json
{
  "user_id": "alice",
  "prompt_id": "p-001",
  "text": "Explain quantum computing simply",
  "priority": "high"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `user_id` | Yes | User identifier |
| `prompt_id` | No | Idempotency key. Omit to auto-derive from `sha256(user_id:normalize(text))` |
| `text` | Yes | Prompt text |
| `priority` | No | `high` \| `normal` \| `low` — default `normal` |

**Response (200 OK — completed):**
```json
{
  "user_id": "alice",
  "prompt_id": "p-001",
  "status": "completed",
  "cached": false,
  "response": "Mock response for prompt [a3f2c1b8]: ...",
  "error": null,
  "retry_count": 0,
  "processing_time_ms": 342
}
```

| `status` | Meaning |
|----------|---------|
| `completed` | Done — `response` is populated |
| `processing` | Timeout exceeded — poll `/result/{id}` |
| `failed` | All retries exhausted — `error` explains why |

| Error | HTTP | When |
|-------|------|------|
| Same `prompt_id`, different text | 409 | Idempotency conflict |
| Invalid `priority` / missing field | 422 | Pydantic validation |
| Queue at capacity | 429 | `Retry-After: 5` header included |

---

### `GET /result/{prompt_id}` — Poll Result

Use when `POST /process` returned `status: processing`. Returns same schema as above.

Returns `404` if the `prompt_id` has never been submitted.

---

### `GET /health` — System Health

```json
{
  "status": "healthy",
  "timestamp": "2025-05-28T10:30:00+00:00",
  "components": {
    "database": "connected",
    "worker": "running",
    "cache": "available"
  }
}
```

`worker` is `running` if any worker heartbeat was seen in the last 30 seconds, `idle` if not.

---

### `GET /metrics` — Prometheus Metrics

```
prompt_requests_total{status="completed", cached="false"} 24
prompt_cache_hits_total{match_type="exact"} 6
prompt_cache_hits_total{match_type="semantic"} 2
llm_calls_total{outcome="success"} 22
rate_limit_denials_total 3
job_queue_depth 0
processing_latency_seconds{stage="total", le="0.5"} 20
worker_active_jobs{worker_id="worker-a1b2-slot0"} 1
```

---

## Technology Stack

| Component | Choice | Why this, not that |
|-----------|--------|-------------------|
| API | FastAPI + Uvicorn | Async-native; Pydantic validation; OpenAPI docs free. Flask/Django are synchronous by default. |
| Database | PostgreSQL 16 + pgvector | `FOR UPDATE SKIP LOCKED` for queue; `LISTEN/NOTIFY` for push; pgvector for semantic cache. One system does everything. |
| Queue | Custom DB-backed (no Celery) | Full explicit control over lease, heartbeat, retry, crash recovery. This is what the system is testing. |
| Rate limiter | Redis 7 + Lua token bucket | Single atomic round-trip; shared across all workers; no TOCTOU race. DB locks would be a hot-spot at this exact bottleneck. |
| Embeddings | Mock (SHA-256-seeded) / sentence-transformers | Deterministic, reproducible, no 400MB image. Swap to real model with one env var. |
| Migrations | Alembic (auto-run on startup) | `docker-compose up` needs zero manual steps. |
| Logging | structlog → JSON | Every field is machine-parseable. Grep, filter, ship to any aggregator without a parser. |
| Metrics | prometheus_client | Industry standard. `/metrics` is ready for Prometheus + Grafana. |
| Containers | Docker Compose | One-command local deployment. Multi-replica workers out of the box. |

---

## Configuration

Copy `.env.example` to `.env`. This file is the **single source of truth** for all settings — both for local development and for Docker. `docker-compose.yml` loads it automatically via `env_file: .env` and only overrides the two URL values that need Docker service hostnames (`DATABASE_URL`, `REDIS_URL`).

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://...` | Async PostgreSQL connection string |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `WORKER_CONCURRENCY` | `4` | Async coroutines per worker process |
| `LEASE_SECONDS` | `15` | How long a worker holds a job before the reaper reclaims it |
| `HEARTBEAT_INTERVAL` | `5` | Seconds between lease renewals |
| `REAPER_INTERVAL` | `5` | Seconds between reaper scans for expired leases |
| `MAX_JOB_ATTEMPTS` | `5` | Retries before a job is permanently marked dead |
| `API_TIMEOUT_SECONDS` | `10` | How long `POST /process` waits before returning `status: processing` |
| `SIMILARITY_THRESHOLD` | `0.9` | Cosine similarity cutoff for semantic cache hit |
| `EMBED_PROVIDER` | `mock` | `mock` \| `sentence_transformer` |
| `LLM_PROVIDER` | `mock` | `mock` \| `real` |
| `LLM_FAILURE_RATE` | `0.05` | Mock LLM 5% random failure rate |
| `RATE_LIMIT_CAPACITY` | `300` | Token bucket burst capacity |
| `RATE_LIMIT_REFILL_PER_SEC` | `5.0` | = 300 calls/minute smooth refill |
| `MAX_QUEUE_DEPTH` | `1000` | Queue depth cap; returns 429 above this |

---

## Running Tests

```bash
# Unit tests — no Docker needed
pip install -r requirements-dev.txt
pytest tests/unit/ -v

# Integration tests — requires docker-compose running
pytest tests/integration/ -v --run-integration

# Resilience test — kills a worker mid-job, verifies crash recovery
bash scripts/test_resilience.sh

# Load test — 100 requests at 20 concurrent
bash scripts/load_test.sh 100 20
```

| Test | What it proves |
|------|---------------|
| `test_idempotency.py` | All 5 idempotency cases — new, completed, in-flight, failed, conflict |
| `test_ratelimit.py` | Token bucket math — burst, refill, denial |
| `test_embeddings.py` | Deterministic mock vectors — same text → same vector |
| `test_llm_mock.py` | Simulated latency and failure rate |
| `test_status_transitions.py` | No illegal state machine transitions |
| `test_end_to_end.py` | Full pipeline — submit → worker processes → result returned |
| `test_resilience.sh` | Worker crash → lease expires → reaper reclaims → job completes |

---

## Project Structure

```
src/
├── api/
│   ├── main.py           # Composition root — ALL dependencies wired here in lifespan()
│   ├── health_checker.py
│   ├── schemas.py        # Pydantic DTOs — ORM models never reach the client
│   └── routes/
│       ├── process.py    # POST /process
│       ├── result.py     # GET /result/{id}
│       ├── health.py     # GET /health
│       └── metrics.py    # GET /metrics
├── models/
│   ├── orm.py            # SQLAlchemy entities (PromptRequest, ProcessingJob, SemanticCache)
│   └── enums.py          # RequestStatus, JobStatus, Priority
├── db/
│   ├── session.py        # asyncpg engine + session factory
│   ├── listen_notify.py  # PostgreSQL LISTEN/NOTIFY — 3 connection strategies
│   └── repositories/     # All SQL hidden here — services never write queries
│       ├── prompt_request.py
│       ├── processing_job.py
│       └── semantic_cache.py
├── services/
│   ├── prompt_processor.py  # API-side Facade: idempotency → enqueue → wait
│   ├── prompt_pipeline.py   # Worker-side Facade: cache → ratelimit → llm → store
│   ├── cache.py             # 3-tier semantic cache lookup
│   ├── idempotency.py       # 5-case decision table
│   ├── ratelimit.py         # Redis token bucket + in-memory for tests
│   ├── embeddings.py        # IEmbeddingProvider strategy
│   └── llm_mock.py          # MockLLMProvider strategy
├── worker/
│   ├── __main__.py      # Worker composition root
│   ├── pool.py          # asyncio.TaskGroup of N slots
│   ├── claimer.py       # FOR UPDATE SKIP LOCKED + LISTEN wakeup
│   ├── heartbeat.py     # Lease renewal + pipeline cancel on loss
│   ├── reaper.py        # Reclaim expired leases
│   └── shutdown.py      # SIGTERM → graceful drain
└── observability/
    ├── logging.py       # structlog JSON
    └── metrics.py       # Prometheus counters/gauges/histograms

alembic/versions/
├── 001_initial_schema.py          # Tables + pgvector extension
└── 002_add_partial_queue_index.py # Partial index on status='queued' only
```
