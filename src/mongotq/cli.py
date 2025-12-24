import argparse
import json
import os
import sys
from typing import Any, Dict, Optional

from bson.objectid import ObjectId

from mongotq import Task, get_task_queue

DEFAULT_TTL = -1


def _get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        raise SystemExit(f"{name} must be an integer")


def _parse_json(value: Optional[str]) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise SystemExit("client options must be a JSON object")
    return parsed


def _parse_payload(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _build_queue(args: argparse.Namespace):
    host = args.host or os.getenv("MONGOTQ_HOST") or os.getenv("MONGO_URI")
    database = args.database or os.getenv("MONGOTQ_DATABASE")
    collection = args.collection or os.getenv("MONGOTQ_COLLECTION")
    if not host or not database or not collection:
        raise SystemExit("host, database, and collection must be provided")

    ttl = args.ttl
    if ttl is None:
        ttl = _get_env_int("MONGOTQ_TTL", DEFAULT_TTL)

    client_options = _parse_json(args.client_options)
    return get_task_queue(
        database_name=database,
        collection_name=collection,
        host=host,
        ttl=ttl,
        tag=args.tag or os.getenv("MONGOTQ_TAG"),
        max_retries=args.max_retries,
        discard_strategy=args.discard_strategy,
        client_options=client_options,
        ensure_indexes=not args.no_indexes,
        visibility_timeout=args.visibility_timeout,
        retry_backoff_base=args.retry_backoff_base,
        retry_backoff_max=args.retry_backoff_max,
        dead_letter_collection=args.dead_letter_collection,
        meta_collection=args.meta_collection,
        rate_limit_per_second=args.rate_limit_per_second,
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Mongo TaskQueue CLI")
    parser.add_argument("--host", help="MongoDB URI")
    parser.add_argument("--database", help="Database name")
    parser.add_argument("--collection", help="Collection name")
    parser.add_argument("--ttl", type=int, help="Pending task TTL in seconds")
    parser.add_argument("--tag", help="Assignment tag")
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--discard-strategy", default="keep")
    parser.add_argument("--visibility-timeout", type=int, default=0)
    parser.add_argument("--retry-backoff-base", type=int, default=0)
    parser.add_argument("--retry-backoff-max", type=int, default=0)
    parser.add_argument("--dead-letter-collection")
    parser.add_argument("--meta-collection")
    parser.add_argument("--rate-limit-per-second", type=float)
    parser.add_argument(
        "--client-options",
        help="JSON object of MongoClient options",
    )
    parser.add_argument(
        "--no-indexes",
        action="store_true",
        help="Skip ensuring indexes",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    append_parser = subparsers.add_parser("append", help="Append a task")
    append_parser.add_argument("payload", help="JSON payload or string")
    append_parser.add_argument("--priority", type=int, default=0)
    append_parser.add_argument("--delay-seconds", type=int, default=0)
    append_parser.add_argument("--scheduled-at", type=float)
    append_parser.add_argument("--dedupe-key")
    append_parser.add_argument("--rate-limit-key")

    subparsers.add_parser("refresh", help="Expire and discard tasks")
    subparsers.add_parser("size", help="Print queue size")
    subparsers.add_parser("status", help="Print status counts")

    head_parser = subparsers.add_parser("head", help="Print first tasks")
    head_parser.add_argument("--count", type=int, default=10)

    tail_parser = subparsers.add_parser("tail", help="Print last tasks")
    tail_parser.add_argument("--count", type=int, default=10)

    subparsers.add_parser("next", help="Fetch next task")
    subparsers.add_parser("pop", help="Pop a successful task")

    requeue_parser = subparsers.add_parser("requeue-failed", help="Requeue failed tasks")
    requeue_parser.add_argument("--delay-seconds", type=int, default=0)

    purge_parser = subparsers.add_parser("purge", help="Purge tasks")
    purge_parser.add_argument("--status")

    heartbeat_parser = subparsers.add_parser("heartbeat", help="Extend task lease")
    heartbeat_parser.add_argument("task_id")
    heartbeat_parser.add_argument("--extend-by", type=int)

    subparsers.add_parser("dead-letter-count", help="Print dead-letter count")

    resolve_parser = subparsers.add_parser(
        "resolve-anomalies", help="Resolve lifecycle anomalies"
    )
    resolve_parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply fixes (default is dry-run)",
    )

    args = parser.parse_args(argv)
    queue = _build_queue(args)

    if args.command == "append":
        inserted = queue.append(
            _parse_payload(args.payload),
            priority=args.priority,
            delay_seconds=args.delay_seconds,
            scheduled_at=args.scheduled_at,
            dedupe_key=args.dedupe_key,
            rate_limit_key=args.rate_limit_key,
        )
        if not inserted:
            print("Duplicate task skipped")
        return 0
    if args.command == "refresh":
        queue.refresh()
        return 0
    if args.command == "size":
        print(queue.size())
        return 0
    if args.command == "status":
        queue.status_info()
        return 0
    if args.command == "head":
        queue.head(n=args.count)
        return 0
    if args.command == "tail":
        queue.tail(n=args.count)
        return 0
    if args.command == "next":
        task = queue.next()
        print(task if task is not None else "No task available")
        return 0
    if args.command == "pop":
        task = queue.pop()
        print(task if task is not None else "No task available")
        return 0
    if args.command == "requeue-failed":
        count = queue.requeue_failed(delay_seconds=args.delay_seconds)
        print(count)
        return 0
    if args.command == "purge":
        count = queue.purge(status=args.status)
        print(count)
        return 0
    if args.command == "heartbeat":
        try:
            task_id = ObjectId(args.task_id)
        except Exception as exc:
            raise SystemExit(f"Invalid task id: {exc}") from exc
        task = Task(_id=task_id)
        updated = queue.heartbeat(task, extend_by=args.extend_by)
        print(updated if updated is not None else "No task updated")
        return 0
    if args.command == "dead-letter-count":
        if queue.dead_letter_collection is None:
            print(0)
        else:
            print(queue.dead_letter_collection.count_documents({}))
        return 0
    if args.command == "resolve-anomalies":
        queue.resolve_anomalies(dry_run=not args.apply)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
