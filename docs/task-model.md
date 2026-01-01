# Task model

A task is a MongoDB document with the following fields.

| Field | Type | Meaning |
| --- | --- | --- |
| `_id` | ObjectId | MongoDB primary key. |
| `payload` | any | Application data for the job. |
| `status` | string | `new`, `pending`, `failed`, `successful`. |
| `priority` | int | Higher values dequeue first. |
| `createdAt` | float | Creation timestamp (seconds). |
| `modifiedAt` | float | Last update timestamp (seconds). |
| `scheduledAt` | float or null | Earliest time the task can be leased. |
| `assignedTo` | string or null | Worker tag that owns the lease. |
| `leaseId` | string or null | Identifier for the lease. |
| `leaseExpiresAt` | float or null | Lease expiration timestamp. |
| `retries` | int | Number of failures so far. |
| `errorMessage` | string or null | Last failure message. |
| `dedupeKey` | string or null | Idempotency key (string only). |
| `rateLimitKey` | string or null | Per-key rate limit identifier. |

`Task` objects in code are a thin wrapper around the underlying document.
They can be converted to dictionaries via `dict(task)`.
