# Task lifecycle

Tasks move through four statuses:

- `new`: available for leasing
- `pending`: leased by a worker
- `successful`: completed successfully
- `failed`: permanently failed or expired

## Typical flow
1. `append()` inserts a task in `new`.
2. `next()` leases a task, updates it to `pending`, and sets `assignedTo`.
3. The worker calls `on_success()` or `on_failure()`.
4. `on_success()` updates status to `successful`.
5. `on_failure()` increments retries and either requeues or marks `failed`.

## Leasing and visibility
If `visibility_timeout > 0`, a lease expiration timestamp is stored as
`leaseExpiresAt`. Calling `refresh()` requeues expired leases by setting
`status=new` and clearing assignment fields.

## TTL expiry
If `ttl` is set, tasks that remain `pending` longer than `ttl` are marked
`failed` during `refresh()`.

## Discarding
Tasks with retry count at or above `max_retries` are discarded during
`refresh()`. The action depends on `discard_strategy`:
- `keep`: keep tasks in place and mark them failed
- `remove`: delete tasks, optionally copying to the dead-letter collection
