# Configuration

`get_task_queue(...)` returns a configured `TaskQueue` instance.

## Parameters

| Name | Default | Notes |
| --- | --- | --- |
| `database_name` | required | MongoDB database name. |
| `collection_name` | required | Collection used to store tasks. |
| `host` | required | MongoDB URI or hostname (string or list). |
| `ttl` | required | Pending task TTL in seconds (`-1` disables expiry). |
| `tag` | `None` | Assignment tag; defaults to `consumer_<pid>`. |
| `max_retries` | `3` | Max retry count before discard. |
| `discard_strategy` | `"keep"` | `keep` leaves failed tasks, `remove` deletes them. |
| `client_options` | `None` | Extra options for `MongoClient`. |
| `ensure_indexes` | `True` | Create indexes if missing. |
| `visibility_timeout` | `0` | Lease duration in seconds for pending tasks. |
| `retry_backoff_base` | `0` | Exponential backoff base in seconds. |
| `retry_backoff_max` | `0` | Max backoff delay in seconds. |
| `dead_letter_collection` | `None` | Target collection for discarded tasks. |
| `meta_collection` | `None` | Metadata collection for rate limits. |
| `rate_limit_per_second` | `None` | Global and per-key dequeue limit. |

## Default MongoClient options
`get_task_queue` uses:
- `tlsAllowInvalidCertificates=True`
- `read_preference=SECONDARY_PREFERRED`

Override these using `client_options`.

## Index management
`ensure_indexes=True` creates indexes automatically. Disable it if you want
manual index management. See `Operations` for details.
