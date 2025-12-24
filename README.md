# Mongo TaskQueue
[![Python 3.9](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://pypi.org/project/mongo-taskqueue)
[![PyPI version](https://badge.fury.io/py/mongo-taskqueue.svg)](https://badge.fury.io/py/mongo-taskqueue)
![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=flat&logo=mongodb&logoColor=white)

Mongo-TaskQueue is a queueing data structure built on top of a MongoDB 
collection.

## Quick start
Just create an instance of TaskQueue and start to append to your queue:
```python
>>> from mongotq import get_task_queue

>>> task_queue = get_task_queue(
        database_name=YOUR_MONGO_DATABASE_NAME,
        collection_name='queueGeocoding',
        host=YOUR_MONGO_DB_URI,
        ttl=-1  # permanent queue
    )
>>> task_queue.append({'species': 'Great Horned Owl'})
>>> task_queue.append({'species': 'Eastern Screech Owl'})
>>> task_queue.append({'species': 'Northern Saw-whet Owl'})
>>> task_queue.append({'species': 'Snowy Owl'})
>>> task_queue.append({'species': 'Whiskered Screech Owl'})
```

Then you can simply check the tail of your queue:
```python
>>> task_queue.tail(n=5)
```
```python
{'_id': ObjectId('6392375588c63227371c693c'),
 'assignedTo': None,
 'createdAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 99685),
 'errorMessage': None,
 'modifiedAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 99685),
 'payload': {'species': 'Great Horned Owl'},
 'priority': 0,
 'retries': 0,
 'status': 'new'}
{'_id': ObjectId('6392375588c63227371c693d'),
 'assignedTo': None,
 'createdAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 129570),
 'errorMessage': None,
 'modifiedAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 129570),
 'payload': {'species': 'Eastern Screech Owl'},
 'priority': 0,
 'retries': 0,
 'status': 'new'}
{'_id': ObjectId('6392375588c63227371c693e'),
 'assignedTo': None,
 'createdAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 155404),
 'errorMessage': None,
 'modifiedAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 155404),
 'payload': {'species': 'Northern Saw-whet Owl'},
 'priority': 0,
 'retries': 0,
 'status': 'new'}
{'_id': ObjectId('6392375588c63227371c693f'),
 'assignedTo': None,
 'createdAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 179804),
 'errorMessage': None,
 'modifiedAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 179804),
 'payload': {'species': 'Snowy Owl'},
 'priority': 0,
 'retries': 0,
 'status': 'new'}
{'_id': ObjectId('6392375588c63227371c6940'),
 'assignedTo': None,
 'createdAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 204284),
 'errorMessage': None,
 'modifiedAt': datetime.datetime(2022, 12, 8, 14, 13, 25, 204284),
 'payload': {'species': 'Whiskered Screech Owl'},
 'priority': 0,
 'retries': 0,
 'status': 'new'}
5 Task(s) available in the TaskQueue
```


## Installation
The only dependency is [pyMongo](https://pymongo.readthedocs.io/en/stable/).
The easiest way to install Mongo-TaskQueue is using `uv`:
```shell
uv pip install mongo-taskqueue
```

## CLI
Use the `mongotq` CLI to inspect or update a queue:
```shell
mongotq --host mongodb://localhost:27017 --database mydb --collection myqueue size
mongotq --host mongodb://localhost:27017 --database mydb --collection myqueue append '{"job": "hello"}'
mongotq --host mongodb://localhost:27017 --database mydb --collection myqueue next
mongotq --host mongodb://localhost:27017 --database mydb --collection myqueue append '{"job": "later"}' --delay-seconds 30
mongotq --host mongodb://localhost:27017 --database mydb --collection myqueue requeue-failed --delay-seconds 10
mongotq --host mongodb://localhost:27017 --database mydb --collection myqueue dead-letter-count
```

Environment variables:
- `MONGOTQ_HOST` or `MONGO_URI`
- `MONGOTQ_DATABASE`
- `MONGOTQ_COLLECTION`
- `MONGOTQ_TTL`
- `MONGOTQ_TAG`

Examples are available in `examples/producer.py` and `examples/worker.py`.

## Runtime behavior
- `ttl` controls how long a task may remain in `pending` state before it is
  released and marked as `failed`.
- `visibility_timeout` controls how long a task lease lasts before it is
  requeued for another worker.
- `retry_backoff_base` and `retry_backoff_max` control exponential backoff when
  `on_failure` is called.
- `max_retries` caps how many failed attempts a task can have.
- `discard_strategy` can be `keep` or `remove`.
- `client_options` may be passed to `get_task_queue` to configure `MongoClient`.
- `dead_letter_collection` moves discarded tasks into another collection.
- `rate_limit_per_second` limits dequeue frequency (global and per-key).
  Rate limiting uses a companion metadata collection.

## Idempotent enqueue
Use `dedupe_key` with `append` to avoid duplicate tasks:
```python
task_queue.append({"job": 1}, dedupe_key="job-1")
```

## Delayed tasks
Schedule a task for later execution:
```python
task_queue.append({"job": "later"}, delay_seconds=30)
```

## Async usage
Install the async extra and use `AsyncTaskQueue`:
```shell
uv pip install "mongo-taskqueue[async]"
```
```python
from mongotq import AsyncTaskQueue
```

## Testing (CI only)
Tests are designed to run only in GitHub Actions. Local test runs are skipped.

## GitHub Actions
All tests run in GitHub Actions via Docker Compose. Lint, type checks, and
package validation also run in CI.

## Release
Tag a release as `vX.Y.Z` to trigger the publish workflow. The workflow uses
`PYPI_TOKEN` from GitHub Secrets.
