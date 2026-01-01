# CLI

The CLI is installed as `mongotq`.

## Required connection options
You can pass these via flags or environment variables.

| Flag | Env var | Description |
| --- | --- | --- |
| `--host` | `MONGOTQ_HOST` or `MONGO_URI` | MongoDB URI |
| `--database` | `MONGOTQ_DATABASE` | Database name |
| `--collection` | `MONGOTQ_COLLECTION` | Collection name |
| `--ttl` | `MONGOTQ_TTL` | Pending task TTL |

## Examples
Append a task:
```bash
mongotq \
  --host mongodb://localhost:27017 \
  --database app \
  --collection jobs \
  append '{"job": "email"}'
```

Inspect status:
```bash
mongotq --host ... --database app --collection jobs status
```

Lease and pop:
```bash
mongotq --host ... --database app --collection jobs next
mongotq --host ... --database app --collection jobs pop
```

## Commands
- `append` (supports `--priority`, `--delay-seconds`, `--scheduled-at`,
  `--dedupe-key`, `--rate-limit-key`)
- `refresh`, `size`, `status`, `head`, `tail`, `next`, `pop`
- `requeue-failed`, `purge`, `heartbeat`, `dead-letter-count`,
  `resolve-anomalies`
