# Architecture — Distributed LLM Request Processing System

## Overview

This system processes LLM prompts through a distributed, fault-tolerant pipeline. An HTTP client submits a prompt, the API queues it durably in PostgreSQL, a worker pool picks it up, runs it through a semantic cache + rate limiter + LLM provider chain, and writes the result back. The API waits for completion using PostgreSQL LISTEN/NOTIFY and returns the result inline (no polling required for most requests).

---

## High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CLIENT                                      │
│   POST /process          GET /result/{id}          GET /health      │
└───────────┬──────────────────┬──────────────────────────┬───────────┘
            │                  │                          │
            ▼                  ▼                          ▼
┌───────────────────────────────────────────────────────────────────┐
│                     FastAPI Application (API Process)             │
│                                                                   │
│  ┌─────────────────┐   ┌──────────────────┐   ┌───────────────┐  │
│  │ PromptProcessor  │   │ PromptRequestRepo│   │HealthChecker  │  │
│  │ (Facade)         │   │ (Result polling) │   │               │  │
│  │                  │   └──────────────────┘   └───────────────┘  │
│  │  IdempotencyCheck│                                              │
│  │  QueueDepthCheck │                                              │
│  │  AtomicInsert    │                                              │
│  │  LISTEN/wait()   │                                              │
│  └────────┬─────────┘                                              │
│           │ NOTIFY "job_queued"                                    │
└───────────┼───────────────────────────────────────────────────────┘
            │
            ▼
┌───────────────────────────────────────────────────────────────────┐
│               PostgreSQL 16 + pgvector (Source of Truth)          │
│                                                                   │
│  ┌─────────────────────┐  ┌──────────────────┐  ┌─────────────┐  │
│  │  prompt_requests    │  │ processing_jobs   │  │semantic_cache│ │
│  │  ─────────────────  │  │ ────────────────  │  │────────────  │ │
│  │  prompt_id (UNIQUE) │  │ status            │  │ text_hash   │ │
│  │  status             │  │ worker_id         │  │ embedding   │ │
│  │  text_hash          │  │ locked_until      │  │ (Vector384) │ │
│  │  embedding (vec384) │  │ attempt_count     │  │ hit_count   │ │
│  │  response           │  │ last_heartbeat_at │  └─────────────┘ │
│  └─────────────────────┘  └──────────────────┘                   │
│                                                                   │
│  LISTEN/NOTIFY channels:                                          │
│    "job_queued"         → wakes idle workers                      │
│    "prompt_done_{req.id}" → wakes waiting API request             │
└───────────┬───────────────────────────────────────────────────────┘
            │ FOR UPDATE SKIP LOCKED (claim_next)
            ▼
┌───────────────────────────────────────────────────────────────────┐
│          Worker Processes (3 replicas × 4 coroutines = 12 slots)  │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │  WorkerPool (asyncio.TaskGroup)                             │  │
│  │                                                             │  │
│  │  ┌──────────────────────────────────────────────────────┐   │  │
│  │  │ _worker_loop (slot 0..3)                             │   │  │
│  │  │   JobClaimer ──→ FOR UPDATE SKIP LOCKED              │   │  │
│  │  │   HeartbeatManager ──→ extend_lease every 5s         │   │  │
│  │  │   PromptPipeline (sub-Task):                         │   │  │
│  │  │     Stage 1: SemanticCacheService.lookup()           │   │  │
│  │  │       ├── Tier 1: Exact hash match (O(1))            │   │  │
│  │  │       └── Tier 2: pgvector cosine ANN               │   │  │
│  │  │     Stage 2: RedisTokenBucketLimiter.try_acquire()   │   │  │
│  │  │     Stage 3: MockLLMProvider.complete()              │   │  │
│  │  │     Stage 4: SemanticCacheService.store()            │   │  │
│  │  └──────────────────────────────────────────────────────┘   │  │
│  │                                                             │  │
│  │  Reaper (background coroutine): reclaim expired leases      │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                   │
│                           ┌─────────────────────┐                 │
│                           │  Redis 7             │                 │
│                           │  Token Bucket (Lua)  │                 │
│                           │  Key: llm:ratelimit  │                 │
│                           └─────────────────────┘                 │
└───────────────────────────────────────────────────────────────────┘
```

---

## Request Lifecycle (Happy Path)

### Step-by-step for `POST /process`

```
Client → POST /process {user_id, prompt_id, text, priority}
  │
  ├─ 1. PromptSubmitRequest validated by Pydantic
  │      • prompt_id absent? → sha256(user_id:normalize(text)) auto-generated
  │
  ├─ 2. IdempotencyService.check(prompt_id, text_hash)
  │      • New ID → proceed
  │      • Existing + same text + completed → return cached (< 1ms)
  │      • Existing + same text + in-flight → await existing job
  │      • Existing + same text + failed → re-enqueue
  │      • Existing + different text → 409 Conflict
  │
  ├─ 3. QueueDepthCheck: queued_depth() >= max_queue_depth → 429
  │
  ├─ 4. Atomic INSERT: PromptRequestORM + ProcessingJobORM in ONE transaction
  │      • SQLAlchemy: INSERT request, flush() → get PK, INSERT job
  │      • IntegrityError (concurrent same ID) → idempotent fetch + continue
  │
  ├─ 5. LISTEN on channel "prompt_done_{req.id}" (BEFORE sending NOTIFY)
  │
  ├─ 6. NOTIFY "job_queued" → wakes idle workers
  │
  ├─ 7. Poll loop (max 10s):
  │      await notification (timeout 1s per iteration)
  │      re-read DB each iteration
  │      break when status ∈ {completed, failed}
  │
  └─ 8. Return PromptResponse DTO (ORM never leaks out)
```

### Worker Processing Steps

```
JobClaimer.next_job():
  │
  ├─ LISTEN "job_queued" (persistent connection)
  │  Fallback: poll every 2s if notification missed
  │
  ├─ claim_next(): SELECT ... FOR UPDATE SKIP LOCKED
  │   • Sets worker_id, locked_until = now + 15s, status = processing
  │   • SKIP LOCKED guarantees: two workers never claim the same row
  │
WorkerPool creates pipeline_task + heartbeat:
  │
  ├─ HeartbeatManager: every 5s → extend_lease(job_id, worker_id, 15s)
  │   • Returns False if reaper reclaimed → cancel pipeline_task immediately
  │
  ├─ PromptPipeline.execute():
  │   ├─ Stage 1: cache lookup
  │   │   ├─ Tier 1: SELECT WHERE text_hash = sha256(normalize(text))  ← O(1) exact
  │   │   └─ Tier 2: embedding via asyncio.to_thread() → pgvector ANN
  │   │              SELECT embedding <=> $1 ORDER BY ... LIMIT 1 WHERE sim >= 0.9
  │   │
  │   ├─ CACHE HIT → mark_completed(cached=True) → NOTIFY "prompt_done_N"
  │   │
  │   ├─ Stage 2: Redis rate limit → Lua script → atomic token bucket check
  │   │   └─ DENIED → release_lease(retry_after) → job re-queued with wait
  │   │
  │   ├─ Stage 3: LLMProvider.complete(text) → 200-500ms mock latency
  │   │
  │   ├─ Stage 4: SemanticCacheService.store(embedding, response)
  │   │   └─ UPSERT into semantic_cache
  │   │
  │   └─ mark_completed(cached=False) → NOTIFY "prompt_done_N"
  │
  └─ WorkerPool NOTIFY → wakes waiting API coroutine → returns to client
```

---

## Component Details

### API Layer (`src/api/`)

**`main.py` — Composition Root**
All dependencies are wired exactly once in the `lifespan()` context manager. Routers never construct their own dependencies — they access `request.app.state`. This is dependency injection without a DI framework.

**`routes/process.py` — Controller**
Thin: receives request, delegates to `PromptProcessor`, maps ORM → DTO, handles exceptions → HTTP codes. No business logic here.

**`schemas.py` — View (Pydantic DTOs)**
ORM models are never returned to clients. `PromptResponse` is a separate Pydantic model. The DTO contract is stable even if the ORM changes.

**`health_checker.py`**
Probes DB, Redis, and worker liveness (via `processing_jobs.last_heartbeat_at`). Used by Docker Compose healthchecks.

---

### PromptProcessor (`src/services/prompt_processor.py`)

**Facade** over the API-side workflow. Hides complexity of:
- 5-case idempotency decision table
- Atomic DB insert
- Race condition handling (IntegrityError)
- LISTEN/NOTIFY sequencing (listen BEFORE notify)
- Timeout fallback to polling

Key invariant: `wait_for_notification()` is opened BEFORE `notify()` so the notification is never missed even on instant cache hits.

---

### PromptPipeline (`src/services/prompt_pipeline.py`)

**Chain of Responsibility** over the worker-side pipeline:

```
embed → cache_lookup → [HIT: short-circuit]
                      → rate_limit → llm → cache_store → persist
```

Key correctness rules:
- `asyncio.CancelledError` is explicitly re-raised (not caught by broad `except Exception`) so graceful shutdown propagates
- `release_lease()` passes `worker_id` for ownership validation to prevent stale workers from resetting live jobs
- Exponential backoff: `min(30, 2^(attempt_count - 1))` seconds

---

### WorkerPool (`src/worker/pool.py`)

**`asyncio.TaskGroup`** replaces `asyncio.gather(return_exceptions=True)`:
- `gather(return_exceptions=True)` swallows exceptions silently → pool runs with N-1 workers, no alert
- `TaskGroup`: any unhandled exception cancels all siblings and raises `ExceptionGroup` → process exits, Docker restarts it

Each worker slot creates the pipeline as a **sub-Task** (not `await pipeline` directly):
- HeartbeatManager can cancel just the pipeline task on lease loss
- The worker slot `while` loop continues to the next job

---

### JobClaimer (`src/worker/claimer.py`)

Uses PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` to claim jobs. Workers that can't claim a row skip it instantly instead of blocking. Combined with LISTEN/NOTIFY for sub-millisecond wakeup and 2-second poll fallback for missed notifications.

---

### HeartbeatManager (`src/worker/heartbeat.py`)

Runs as a separate asyncio Task. Every `heartbeat_interval` seconds:
1. Calls `extend_lease(job_id, worker_id, lease_seconds)`
2. The UPDATE validates `worker_id` ownership — prevents stale workers extending someone else's lease
3. If `UPDATE` returns 0 rows (reaper reclaimed the job): cancels `pipeline_task` immediately
4. On transient DB error: logs warning, keeps running — reaper will sort out genuine expiry

---

### Reaper (`src/worker/reaper.py`)

Runs every 5 seconds. Finds jobs where `locked_until < now` and `status = processing` → resets them to `queued`. Notifies workers via `NOTIFY "job_queued"` so they wake up and claim the reclaimed jobs.

This handles the crash scenario: if a worker dies without heartbeating, the reaper reclaims its jobs within `lease_seconds` (15s by default).

---

### SemanticCacheService (`src/services/cache.py`)

Three-tier lookup optimized for latency:

| Tier | Mechanism | Cost | When used |
|------|-----------|------|-----------|
| 1 | `sha256(normalize(text))` hash equality | O(1), no embedding | Exact same text |
| 2 | pgvector cosine similarity, HNSW index | O(log n) ANN | Similar text (≥ 0.9 similarity) |
| 3 | LLM call | 200–500ms + cost | New content |

`embed()` runs in `asyncio.to_thread()` — CPU-bound numpy/model inference never blocks the event loop.

---

### RedisTokenBucketLimiter (`src/services/ratelimit.py`)

Atomic Lua script runs on Redis:
1. Load bucket state (`tokens`, `last_refill`) from Redis hash
2. Compute tokens added since last refill: `elapsed × refill_rate`
3. If tokens ≥ 1: decrement, return 1 (allowed)
4. Else: return 0 (denied)

All in one round-trip — no TOCTOU race between check and decrement.

Shared across all worker replicas via the same Redis key (`llm:ratelimit:global`).

---

### ListenNotify (`src/db/listen_notify.py`)

Three distinct connection strategies to avoid asyncpg's single-operation constraint:

| Use case | Connection strategy |
|----------|-------------------|
| `notify()` — fire-and-forget | Fresh connection per call; closed after execute |
| Workers `listen()` — channel subscription | Single persistent connection per worker slot |
| API `wait_for_notification()` — per-request wait | Private connection per waiter (concurrent API requests don't race) |
| `prepared_listener()` — open before notify | Private connection, LISTEN established first, yielded as context manager |

---

## Database Design

### Tables

#### `prompt_requests`
- `prompt_id` — UNIQUE, indexed. Application-level idempotency key.
- `text_hash` — `sha256(normalize(text))` as BYTEA. Used for exact cache lookup and idempotency check.
- `embedding` — `Vector(384)`. Stored on completion for analytical queries.
- `status` — FSM: `received → queued → processing → completed | failed`

#### `processing_jobs`
- `prompt_request_id` — UNIQUE (one job per request)
- `worker_id` — set on claim, used for ownership validation in `extend_lease` and `release_lease`
- `locked_until` — reaper reclaims when `locked_until < now AND status = processing`
- `last_heartbeat_at` — used by HealthChecker to report worker liveness
- `attempt_count` / `max_attempts` — retry counter; job becomes `dead` when `attempt_count >= max_attempts`

#### `semantic_cache`
- `text_hash` — UNIQUE. Tier-1 exact lookup.
- `embedding` — `Vector(384)` with HNSW index.
- `hit_count` — incremented on each cache hit.

### Indexes

```sql
-- Partial index: only queue entries, sorted by priority then scheduled_at
-- Dramatically reduces index size and scan cost at high volume
CREATE INDEX idx_pj_queue ON processing_jobs (priority, scheduled_at)
WHERE status = 'queued' AND scheduled_at <= NOW();

-- HNSW index for semantic cache ANN search
-- m=16, ef_construction=64 are standard starting values
-- No training data required (unlike IVFFlat)
CREATE INDEX idx_sc_embedding ON semantic_cache
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);
```

**Why HNSW over IVFFlat?**
- IVFFlat requires a training step with existing vectors — the cache starts empty, so training is impossible
- HNSW builds as vectors are inserted; works correctly from zero entries
- Recall is typically higher at the same speed

---

## Concurrency Model

```
Process: API (1 Uvicorn worker)
  asyncio event loop
    ├── FastAPI request handlers (concurrent coroutines)
    ├── PromptProcessor.submit_and_wait() per request
    └── ListenNotify connections (one per waiting request)

Process: Worker (3 replicas)
  asyncio event loop per replica
    ├── asyncio.TaskGroup (4 slots)
    │    ├── _worker_loop slot-0
    │    │    ├── pipeline-{job_id}  ← sub-Task
    │    │    └── heartbeat-{job_id} ← sub-Task
    │    ├── _worker_loop slot-1
    │    ├── _worker_loop slot-2
    │    └── _worker_loop slot-3
    └── Reaper (background coroutine)
```

**Thread usage:**
- `asyncio.to_thread()` for `embed()` — runs in the OS thread pool
- All DB and Redis I/O are truly async (asyncpg, redis.asyncio) — no threads needed

**Total concurrency:** 3 replicas × 4 slots = 12 concurrent jobs maximum.

---

## Failure Modes and Recovery

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Worker process crash | `locked_until` expires | Reaper reclaims within 15s, re-queues job |
| LLM transient error | `LLMRandomFailureError` (retryable) | Exponential backoff, retry up to 5 times |
| LLM permanent error | `LLMTimeoutError` after max attempts | Job marked `dead`, request marked `failed` |
| Redis unavailable | `RedisUnavailableError` from Lua script | Fail-closed: deny the call, trigger retry |
| DB unavailable | SQLAlchemy exception | `DatabaseUnavailableError` (retryable) |
| Two workers claim same job | `FOR UPDATE SKIP LOCKED` | Second worker skips the row atomically |
| Two API requests insert same prompt_id simultaneously | `UNIQUE(prompt_id)` constraint | IntegrityError caught, loser fetches winner's row |
| Notification missed (NOTIFY before LISTEN) | `prepared_listener()` context manager | Eliminated: LISTEN is opened before NOTIFY |
| Stale worker extends someone else's lease | `WHERE worker_id = $1` in UPDATE | Returns 0 rows; caller detects no-op |
| Queue overflow | `queued_depth()` check | 429 with `Retry-After: 5` header |

---

## Design Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| **Facade** | `PromptProcessor`, `PromptPipeline` | Hide multi-step workflow behind a single method |
| **Strategy** | `ILLMProvider`, `IEmbeddingProvider`, `IRateLimiter` | Swap implementations (mock ↔ real) without changing callers |
| **Chain of Responsibility** | `PromptPipeline._run()` stages | Each stage can short-circuit (cache hit) or pass through |
| **Repository** | `PromptRequestRepository`, `ProcessingJobRepository`, `SemanticCacheRepository` | Encapsulate all DB access; controllers/services never write SQL |
| **MVC** | Entire API layer | Routes = Controller; Schemas = View; ORM + repos = Model |
| **Composition Root** | `main.py lifespan()`, `worker/__main__.py` | Wire all dependencies once at startup; inject via `app.state` |
| **Template Method** | `BaseRepository` | Shared session management; subclasses implement queries |

---

## Observability

### Logging (structlog → JSON)

Every log line is a structured JSON object with consistent fields:
```json
{"event": "pipeline_complete", "prompt_id": "p-001", "job_id": 42, "level": "info", "timestamp": "..."}
```

Key log events:
- `request_enqueued` — job entered the queue
- `cache_hit` / `cache_miss` — tier 1 or 2 result
- `rate_limit_deferred` — job deferred; includes `wait_s`
- `pipeline_complete` — job finished successfully
- `pipeline_dead` — all retries exhausted
- `lease_lost` — heartbeat detected reaper reclaimed the job

### Metrics (Prometheus — `/metrics`)

| Metric | Type | Labels |
|--------|------|--------|
| `prompt_requests_total` | Counter | `status`, `cached` |
| `prompt_cache_hits_total` | Counter | `match_type` (exact/semantic) |
| `prompt_cache_misses_total` | Counter | — |
| `llm_calls_total` | Counter | `outcome` (success/failure) |
| `rate_limit_denials_total` | Counter | — |
| `job_queue_depth` | Gauge | — |
| `worker_active_jobs` | Gauge | `worker_id` |
| `processing_latency_seconds` | Histogram | `stage` (cache/llm/total) |
| `job_attempts_total` | Histogram | — |

---
**Result:**

| Setting type | Where it lives |
|---|---|
| All application settings (timeout, concurrency, LLM config, rate limits, etc.) | `.env` only |
| Docker service hostnames for DB and Redis | `docker-compose.yml` `environment:` only |

### Key Configuration Values

| Variable | Default | Notes |
|---|---|---|
| `API_TIMEOUT_SECONDS` | `10` | `POST /process` waits this long before returning `status: processing`. Reduced from 30s — 10s is the practical upper bound for an interactive user |
| `LEASE_SECONDS` | `15` | Must be > `HEARTBEAT_INTERVAL × 3` to tolerate transient DB slowness |
| `HEARTBEAT_INTERVAL` | `5` | Every 5s the worker proves it's alive. 3 missed beats = lease expiry |
| `WORKER_CONCURRENCY` | `4` | Coroutines per process. 3 replicas × 4 = 12 total concurrent jobs |
| `SIMILARITY_THRESHOLD` | `0.9` | 90% cosine similarity required for semantic cache hit. Lower = more hits but risk of wrong answers |
| `MAX_QUEUE_DEPTH` | `1000` | Requests above this get 429. Set based on `WORKER_CONCURRENCY × replicas × acceptable_wait_seconds / avg_job_latency` |
| `RATE_LIMIT_CAPACITY` | `300` | Token bucket burst size. Combined with `RATE_LIMIT_REFILL_PER_SEC=5.0` gives 300 LLM calls/min |

---

## Scalability Considerations

### Horizontal Scaling (what's easy)

- **Worker replicas**: add more `worker` containers in docker-compose or Kubernetes. `FOR UPDATE SKIP LOCKED` handles contention automatically.
- **API replicas**: stateless; add more Uvicorn instances behind a load balancer. Each maintains its own Redis connection pool.
- **Redis**: single instance sufficient for token bucket; shard keys if needed.

### Bottlenecks (what's hard)

| Bottleneck | Limit | Solution |
|------------|-------|----------|
| PostgreSQL write throughput | ~10k writes/s single node | Read replica for result polling, PgBouncer for connection pooling |
| HNSW index recall at scale | Degrades slightly beyond ~1M vectors | Partition by domain, or migrate to dedicated vector DB |
| asyncio event loop blocking | CPU-bound work blocks all coroutines | `asyncio.to_thread()` for embed; `ProcessPoolExecutor` for heavier work |
| Rate limiter granularity | Global bucket — one bad actor blocks all | Per-user buckets with separate Redis keys |

### What would change for production

1. Add per-user rate limiting in addition to global
2. Replace mock LLM with real provider (OpenAI, Anthropic) — just swap the `ILLMProvider` implementation
3. Replace mock embeddings with `sentence-transformers` or an API embedder — just swap `IEmbeddingProvider`
4. Add distributed tracing (OpenTelemetry) — structlog fields are already trace-friendly
5. Replace `docker-compose` with Kubernetes + HPA for auto-scaling workers on queue depth
