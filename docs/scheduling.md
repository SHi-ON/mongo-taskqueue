# Scheduling

## Delay by seconds
```python
queue.append({"job": "later"}, delay_seconds=30)
```

## Schedule at a timestamp
```python
queue.append({"job": "at-time"}, scheduled_at=1710000000.0)
```

## Rules
- `delay_seconds` and `scheduled_at` are mutually exclusive.
- A scheduled task is not eligible for leasing until its time has passed.

## How it works
The scheduler is not a separate process. `next()` ignores tasks with a
`scheduledAt` value in the future.
