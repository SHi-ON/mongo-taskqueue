# Getting started

## Requirements
- Python 3.9+
- MongoDB (CI uses MongoDB 7)

## Install
```bash
pip install mongo-taskqueue
```

Async support:
```bash
pip install "mongo-taskqueue[async]"
```

## Create a queue
```python
from mongotq import get_task_queue

queue = get_task_queue(
    database_name="app",
    collection_name="jobs",
    host="mongodb://localhost:27017",
    ttl=-1,
)
```

## Enqueue tasks
```python
queue.append({"job": "email", "to": "alice"})
queue.append({"job": "report", "id": 42}, priority=5)
```

## Worker loop
```python
import time
from mongotq import get_task_queue

queue = get_task_queue(
    database_name="app",
    collection_name="jobs",
    host="mongodb://localhost:27017",
    ttl=-1,
    visibility_timeout=30,
)

while True:
    task = queue.next()
    if task is None:
        time.sleep(0.5)
        continue

    try:
        # do work using task.payload
        queue.on_success(task)
    except Exception as exc:
        queue.on_failure(task, error_message=str(exc))
```

## Common patterns
- Use `visibility_timeout` to prevent multiple workers from processing the
  same task at the same time.
- Call `refresh()` periodically to requeue expired leases and discard tasks
  over retry limits.
- Use `dedupe_key` for idempotent enqueue.
