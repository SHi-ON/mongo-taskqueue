from typing import TYPE_CHECKING

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

if TYPE_CHECKING:
    from mongotq.asyncio import AsyncTaskQueue as AsyncTaskQueue
else:  # pragma: no cover
    try:  # pragma: no cover
        from mongotq.asyncio import AsyncTaskQueue as AsyncTaskQueue
    except Exception:  # pragma: no cover
        AsyncTaskQueue = None

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

__version__ = "1.0.1"
