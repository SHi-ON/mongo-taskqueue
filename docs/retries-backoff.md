# Retries and backoff

## Retry counters
Each failure increments `retries`. If `retries` reaches `max_retries`, the task
is discarded during `refresh()`.

## on_failure behavior
```python
queue.on_failure(task, error_message="timeout")
```

`on_failure()`:
- clears assignment fields
- increments `retries`
- optionally schedules a retry based on backoff settings

## Exponential backoff
Backoff uses:

```
delay = retry_backoff_base * (2 ** max(retries - 1, 0))
```

`retry_backoff_max` caps the delay.

## Manual retry
Use `on_retry(task, delay_seconds=...)` to release a leased task back to
`new` with an optional delay.
