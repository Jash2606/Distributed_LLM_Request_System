import pytest
from src.models.enums import RequestStatus, JobStatus


def test_terminal_statuses():
    assert RequestStatus.COMPLETED.is_terminal() is True
    assert RequestStatus.FAILED.is_terminal() is True
    assert RequestStatus.PROCESSING.is_terminal() is False
    assert RequestStatus.QUEUED.is_terminal() is False


def test_priority_from_str():
    from src.models.enums import Priority
    assert Priority.from_str("high") == Priority.HIGH
    assert Priority.from_str("normal") == Priority.NORMAL
    assert Priority.from_str("low") == Priority.LOW
    assert Priority.from_str("unknown") == Priority.NORMAL  # default


def test_priority_ordering():
    from src.models.enums import Priority
    assert Priority.HIGH < Priority.NORMAL < Priority.LOW


def test_job_status_values():
    assert JobStatus.QUEUED == "queued"
    assert JobStatus.DEAD == "dead"
