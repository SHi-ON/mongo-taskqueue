# Dead-letter collection

Discarded tasks can be moved to a separate collection for analysis.

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

## Behavior
- Dead-lettering happens during `refresh()`.
- Tasks are copied into the dead-letter collection with a `discardedAt`
  timestamp, then removed from the main queue.
- If `discard_strategy="keep"`, the dead-letter collection is not used.
