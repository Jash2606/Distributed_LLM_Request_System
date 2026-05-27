class DomainError(Exception):
    pass


class IdempotencyConflictError(DomainError):
    """Same prompt_id submitted with a different text."""
    def __init__(self, prompt_id: str):
        self.prompt_id = prompt_id
        super().__init__(f"prompt_id '{prompt_id}' already exists with different text")


class PromptValidationError(DomainError):
    pass


class QueueFullError(DomainError):
    """Job queue has reached max depth. Client should back off and retry."""
    def __init__(self, depth: int, max_depth: int):
        self.depth = depth
        self.max_depth = max_depth
        super().__init__(
            f"Queue at capacity ({depth}/{max_depth}). "
            "Back off and retry after a few seconds."
        )


class InfrastructureError(Exception):
    pass


class DatabaseUnavailableError(InfrastructureError):
    pass


class RedisUnavailableError(InfrastructureError):
    pass


class ProviderError(Exception):
    pass


class LLMProviderError(ProviderError):
    pass


class LLMTimeoutError(LLMProviderError):
    pass


class LLMRandomFailureError(LLMProviderError):
    pass


class RateLimitExceededError(Exception):
    def __init__(self, wait_seconds: float = 0.0):
        self.wait_seconds = wait_seconds
        super().__init__(f"Rate limit exceeded; retry after {wait_seconds:.2f}s")


class JobError(Exception):
    pass


class JobLeaseExpiredError(JobError):
    pass


class JobMaxAttemptsExceededError(JobError):
    pass


# Classify which exceptions are retryable
RETRYABLE_ERRORS = (
    LLMTimeoutError,
    LLMRandomFailureError,
    DatabaseUnavailableError,
    RedisUnavailableError,
    JobLeaseExpiredError,
    RateLimitExceededError,
)

NON_RETRYABLE_ERRORS = (
    PromptValidationError,
    IdempotencyConflictError,
    JobMaxAttemptsExceededError,
)


def is_retryable(exc: Exception) -> bool:
    return isinstance(exc, RETRYABLE_ERRORS)
