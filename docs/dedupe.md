# Dedupe keys

Dedupe keys provide idempotent enqueue. When `dedupe_key` is supplied,
`append()` returns `False` if a task with the same key is already in flight.

```python
queue.append({"job": "once"}, dedupe_key="job-123")
```

## Important details
- The dedupe index only applies to string values.
- Only tasks in `new`, `pending`, or `failed` are considered for uniqueness.
- When a task reaches `successful`, the dedupe key is no longer reserved.

## When to use it
Use dedupe keys when you must prevent duplicate jobs (for example, one report
per user per day).
