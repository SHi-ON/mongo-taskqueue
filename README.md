# mongo-taskqueue

Queueing data structure on top of a MongoDB collection.

## Overview
mongo-taskqueue stores tasks in a single MongoDB collection and exposes a small
API for enqueueing, leasing, and completing work. It is designed for simple
worker loops that need scheduling, retries, and deduplication without an extra
queue service.

## Features
- Single-collection queue with priority ordering
- Delayed enqueue and scheduled execution
- Visibility timeouts with optional heartbeat extension
- Retry tracking with optional exponential backoff
- Dedupe keys (string) to avoid duplicate in-flight tasks
- Global and per-key rate limiting
- Dead-letter collection for discarded tasks
- Sync and async APIs
- CLI for common operations

## Install
```bash
pip install mongo-taskqueue
```

Async support:
```bash
pip install "mongo-taskqueue[async]"
```

## Quick start
```python
from mongotq import get_task_queue

queue = get_task_queue(
    database_name="app",
    collection_name="jobs",
    host="mongodb://localhost:27017",
    ttl=-1,
)

queue.append({"job": "email", "to": "alice"})

task = queue.next()
if task:
    try:
        # do work
        queue.on_success(task)
    except Exception as exc:
        queue.on_failure(task, error_message=str(exc))
```

## Configuration
`get_task_queue(...)` accepts:
- `ttl`: seconds a pending task can live before being marked failed (`-1` for
  no expiry)
- `max_retries`: maximum failure count before discard
- `discard_strategy`: `keep` or `remove`
- `visibility_timeout`: lease duration for pending tasks
- `retry_backoff_base` and `retry_backoff_max`: exponential backoff controls
- `dead_letter_collection`: where discarded tasks are copied when using
  `discard_strategy="remove"`
- `meta_collection`: metadata collection for rate limits
- `rate_limit_per_second`: global and per-key dequeue limits
- `ensure_indexes`: create indexes if missing

## Task lifecycle
Statuses are available as constants:
`STATUS_NEW`, `STATUS_PENDING`, `STATUS_FAILED`, `STATUS_SUCCESSFUL`.

Common transitions:
- `next()` leases the next available task and marks it `pending`
- `on_success(task)` marks it `successful`
- `on_failure(task)` increments retries and may reschedule or fail
- `on_retry(task)` releases a leased task back to `new`
- `refresh()` requeues expired leases, expires pending tasks (TTL), and
  discards tasks over retry limits

## Delayed tasks
```python
queue.append({"job": "later"}, delay_seconds=30)
queue.append({"job": "at-time"}, scheduled_at=1710000000.0)
```

## Dedupe keys
Dedupe keys are indexed for string values only.
```python
queue.append({"job": "once"}, dedupe_key="job-123")
```
Duplicate inserts return `False`.

## Rate limiting
Set `rate_limit_per_second` to throttle dequeue frequency. When a task has a
`rateLimitKey`, the per-key limit is enforced in addition to the global limit.
If a per-key limit is hit after a task is leased, the task is released and
scheduled slightly in the future.

## Dead-letter collection
```python
queue = get_task_queue(
    database_name="app",
    collection_name="jobs",
    host="mongodb://localhost:27017",
    ttl=-1,
    max_retries=1,
    discard_strategy="remove",
    dead_letter_collection="jobs_dead",
)
```
Discarded tasks are copied to the dead-letter collection during `refresh()`.

## Async usage
```python
from mongotq import AsyncTaskQueue

queue = AsyncTaskQueue(
    database="app",
    collection="jobs",
    host="mongodb://localhost:27017",
    ttl=-1,
)

await queue.append({"job": "async"})

task = await queue.next()
if task:
    await queue.on_success(task)
```

## CLI
The CLI is installed as `mongotq`.

Environment variables:
- `MONGOTQ_HOST` (or `MONGO_URI`)
- `MONGOTQ_DATABASE`
- `MONGOTQ_COLLECTION`
- `MONGOTQ_TTL`

Example:
```bash
mongotq \
  --host mongodb://localhost:27017 \
  --database app \
  --collection jobs \
  append '{"job": "email"}'
```

Common commands:
- `append`, `next`, `pop`, `refresh`, `size`, `status`
- `head`, `tail`, `purge`, `requeue-failed`
- `heartbeat`, `dead-letter-count`, `resolve-anomalies`

## Testing
Tests are designed to run in GitHub Actions. Local runs are skipped unless
`GITHUB_ACTIONS=true` is set.

## License
MIT. See `LICENSE`.
