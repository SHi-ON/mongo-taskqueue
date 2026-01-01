# FAQ

## Is this a replacement for a full message broker?
No. It is designed for lightweight task queues that already use MongoDB.

## Does it guarantee strict FIFO order?
No. Tasks are ordered by priority and created time. Concurrency, retries, and
leases mean strict FIFO is not guaranteed.

## Can I change the schema?
Yes, tasks are stored as documents, but keep the core fields if you want the
queue to function correctly.

## How do I delete completed tasks?
Call `pop()` to remove `successful` tasks, or use `purge(status=...)`.

## Can I use transactions?
The queue does not require transactions. If your workload requires them,
wrap your own MongoDB calls separately.
