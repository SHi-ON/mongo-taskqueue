# Operations

## Indexes
`get_task_queue` creates indexes by default. The key indexes are:
- status/priority/createdAt for dequeue ordering
- lease expiration
- dedupe key (string-only partial unique)

Disable auto index creation by passing `ensure_indexes=False` and manage
indexes manually.

## Maintenance jobs
Consider running a periodic maintenance job:
- `refresh()` requeues expired leases and expires tasks stuck in `pending`.
- `resolve_anomalies(dry_run=True)` detects and optionally fixes unexpected
  states like `successful` tasks with `assignedTo` still set.

Example (cron):
```bash
mongotq --host ... --database app --collection jobs refresh
```

## Scaling notes
- Use multiple worker processes with unique `tag` values or the default
  `consumer_<pid>`.
- Keep visibility timeouts short enough to recover stuck workers.
- If you need strict ordering, keep a single worker.

## Data export
`to_list()` and `to_dataframe()` can export data for analysis. `to_dataframe`
requires pandas.
