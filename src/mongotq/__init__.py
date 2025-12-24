from mongotq.anomalies import NonPendingAssignedAnomaly
from mongotq.interface import get_task_queue
from mongotq.task import Task, \
    STATUS_NEW, STATUS_PENDING, STATUS_FAILED, STATUS_SUCCESSFUL
from mongotq.task_queue import TaskQueue

try:  # pragma: no cover
    from mongotq.asyncio import AsyncTaskQueue
except Exception:
    AsyncTaskQueue = None

__version__ = '0.3.1'
