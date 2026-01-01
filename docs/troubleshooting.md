# Troubleshooting

## Duplicate key errors on insert
If you pass `dedupe_key`, a duplicate enqueue returns `False`. A raw
`BulkWriteError` can occur when using `bulk_append` with duplicate dedupe keys.

## next() returns None unexpectedly
Common causes:
- No tasks in `new` status
- `scheduledAt` is in the future
- `rate_limit_per_second` is throttling dequeue

## Tasks stuck in pending
If a worker crashes without calling `on_failure` or `on_success`, tasks remain
pending. Run `refresh()` to requeue expired leases. Ensure
`visibility_timeout` is set and `refresh()` runs periodically.

## Index creation failures
If you manage indexes manually, ensure the dedupe index uses a string-only
partial filter. See `Operations` for details.

## Tests are skipped locally
Tests check `GITHUB_ACTIONS=true`. Set that environment variable only when you
intend to run the integration suite with Docker Compose.
