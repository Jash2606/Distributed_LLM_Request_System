from prometheus_client import Counter, Gauge, Histogram, REGISTRY, generate_latest, CONTENT_TYPE_LATEST

# Request counters
prompt_requests_total = Counter(
    "prompt_requests_total",
    "Total prompt requests",
    ["status", "cached"],
)
cache_hits_total = Counter(
    "prompt_cache_hits_total",
    "Total cache hits",
    ["match_type"],  # exact | semantic
)
cache_misses_total = Counter(
    "prompt_cache_misses_total",
    "Total cache misses",
)
llm_calls_total = Counter(
    "llm_calls_total",
    "Total LLM provider calls",
    ["outcome"],  # success | failure | timeout
)
rate_limit_denials_total = Counter(
    "rate_limit_denials_total",
    "Total rate-limit denials",
)

# Gauges
job_queue_depth = Gauge(
    "job_queue_depth",
    "Number of jobs currently queued",
)
worker_active_jobs = Gauge(
    "worker_active_jobs",
    "Number of jobs currently being processed",
    ["worker_id"],
)

# Histograms
processing_latency = Histogram(
    "processing_latency_seconds",
    "Processing latency by stage",
    ["stage"],  # embedding | cache | llm | total
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
)
job_attempts_histogram = Histogram(
    "job_attempts_total",
    "Number of attempts per completed job",
    buckets=(1, 2, 3, 4, 5),
)


def get_metrics_output() -> tuple[bytes, str]:
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
