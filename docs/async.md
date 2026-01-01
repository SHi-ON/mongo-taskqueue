# Async API

The async API mirrors the sync API and uses Motor.

## Install
```bash
pip install "mongo-taskqueue[async]"
```

## Create a queue
```python
from mongotq import AsyncTaskQueue

queue = AsyncTaskQueue(
    database="app",
    collection="jobs",
    host="mongodb://localhost:27017",
    ttl=-1,
)
```

## Example
```python
async def worker(queue):
    task = await queue.next()
    if task is None:
        return
    try:
        # do work
        await queue.on_success(task)
    except Exception as exc:
        await queue.on_failure(task, error_message=str(exc))
```

## Notes
- Close the client with `await queue.close()` when shutting down.
- Async methods match the sync method names and semantics.
- Requires `motor` to be installed.
