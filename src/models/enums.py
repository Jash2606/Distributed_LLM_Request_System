from enum import Enum


class RequestStatus(str, Enum):
    RECEIVED = "received"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

    def is_terminal(self) -> bool:
        return self in (RequestStatus.COMPLETED, RequestStatus.FAILED)


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


class Priority(int, Enum):
    HIGH = 1
    NORMAL = 2
    LOW = 3

    @classmethod
    def from_str(cls, value: str) -> "Priority":
        mapping = {"high": cls.HIGH, "normal": cls.NORMAL, "low": cls.LOW}
        return mapping.get(value.lower(), cls.NORMAL)
