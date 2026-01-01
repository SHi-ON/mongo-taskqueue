# Sync API

## get_task_queue
```python
from mongotq import get_task_queue

queue = get_task_queue(
    database_name="app",
    collection_name="jobs",
    host="mongodb://localhost:27017",
    ttl=-1,
)
```

## Core methods
- `append(payload, priority=0, delay_seconds=0, scheduled_at=None,
  dedupe_key=None, rate_limit_key=None) -> bool`
- `append_many(payloads, priority=0, delay_seconds=0, scheduled_at=None) -> None`
- `bulk_append(tasks, ordered=True) -> None`
- `next() -> Task | None`
- `next_many(count=0) -> list[Task]` (count 0 returns all available)
- `pop() -> Task | None`

## Lifecycle helpers
- `on_success(task)`
- `on_failure(task, error_message=None, retry=True)`
- `on_retry(task, delay_seconds=0)`
- `heartbeat(task, extend_by=None)`

## Maintenance
- `refresh()` requeues expired leases, expires pending tasks, and discards
  over-retry tasks.
- `resolve_anomalies(dry_run=True)` fixes unexpected states such as
  `successful` tasks still assigned to a worker.
- `requeue_failed(delay_seconds=0)` requeues failed tasks within retry limits.

## Introspection
- `size()` total tasks in collection
- `status_counts()` counts by status
- `status_info()` prints status table
- `head(n=10)` and `tail(n=10)` prints task samples
