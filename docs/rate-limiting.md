# Rate limiting

Rate limiting throttles dequeue rate using a metadata collection.

## Global rate limit
```python
queue = get_task_queue(
    database_name="app",
    collection_name="jobs",
    host="mongodb://localhost:27017",
    ttl=-1,
    rate_limit_per_second=0.5,
)
```

When the global limit is exceeded, a leased task is released and scheduled
slightly in the future.

## Per-key rate limit
```python
queue.append({"job": "a"}, rate_limit_key="tenant-1")
queue.append({"job": "b"}, rate_limit_key="tenant-1")
```

Per-key limits are enforced in addition to the global limit. If the per-key
limit blocks a task, the task is released and rescheduled with a short delay.

## Metadata collection
Rate limit state is stored in a separate metadata collection. Use
`meta_collection` to override its name.
