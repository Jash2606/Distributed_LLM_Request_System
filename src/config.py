from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/llm_processor"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Worker
    worker_concurrency: int = 4
    lease_seconds: int = 15
    heartbeat_interval: int = 5
    reaper_interval: int = 5
    max_job_attempts: int = 5

    # API
    api_timeout_seconds: int = 10

    # Semantic cache
    similarity_threshold: float = 0.9
    embed_dim: int = 384
    embed_provider: str = "mock"  # mock | sentence_transformer

    # LLM provider
    llm_provider: str = "mock"  # mock | real
    llm_failure_rate: float = 0.05
    llm_latency_min: float = 0.2
    llm_latency_max: float = 0.5

    # Rate limiter
    rate_limit_capacity: int = 300
    rate_limit_refill_per_sec: float = 5.0

    # Queue backpressure — reject INSERT_NEW above this in-flight job count
    max_queue_depth: int = 1000

    # Observability
    log_level: str = "INFO"
    app_name: str = "llm-processor"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
