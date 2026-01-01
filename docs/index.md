# mongo-taskqueue

mongo-taskqueue is a lightweight task queue backed by a single MongoDB
collection. It gives you the basics you need to run workers: enqueue jobs,
lease them, retry with backoff, and mark them as successful or failed.

## Why use it
- Keep queue state inside MongoDB without extra infrastructure.
- Simple API for worker loops and background jobs.
- Supports scheduling, leases, retries, rate limits, and dedupe keys.

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

## Architecture in one paragraph
Each task is a MongoDB document. `next()` finds a task with status `new`,
assigns it to the current worker, and returns it. `on_success()` marks it
`successful`, and `on_failure()` increments the retry counter and either
requeues or fails it. Maintenance operations like `refresh()` handle
expired leases, TTL expiry, and discards.

## What to read next
- Start with `Getting Started` for installation and a full worker loop.
- `Configuration` lists every option and default.
- `Task Lifecycle` explains status transitions and leases.
- `Operations` covers indexes, maintenance, and scaling.
