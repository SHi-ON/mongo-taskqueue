from typing import Optional, Type

from mongotq.anomalies import NonPendingAssignedAnomaly
from mongotq.interface import get_task_queue
from mongotq.task import (
    STATUS_FAILED,
    STATUS_NEW,
    STATUS_PENDING,
    STATUS_SUCCESSFUL,
    Task,
)
from mongotq.task_queue import TaskQueue

try:  # pragma: no cover
    from mongotq.asyncio import AsyncTaskQueue as _AsyncTaskQueue
except Exception:  # pragma: no cover
    _AsyncTaskQueue = None

AsyncTaskQueue: Optional[Type[object]] = _AsyncTaskQueue

__all__ = [
    "AsyncTaskQueue",
    "NonPendingAssignedAnomaly",
    "STATUS_FAILED",
    "STATUS_NEW",
    "STATUS_PENDING",
    "STATUS_SUCCESSFUL",
    "Task",
    "TaskQueue",
    "get_task_queue",
]

__version__ = "0.3.1"
