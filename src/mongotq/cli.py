import argparse
import json
import os
import sys
from typing import Any, Dict, Optional

from mongotq import get_task_queue

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

    subparsers.add_parser("refresh", help="Expire and discard tasks")
    subparsers.add_parser("size", help="Print queue size")
    subparsers.add_parser("status", help="Print status counts")

    head_parser = subparsers.add_parser("head", help="Print first tasks")
    head_parser.add_argument("--count", type=int, default=10)

    tail_parser = subparsers.add_parser("tail", help="Print last tasks")
    tail_parser.add_argument("--count", type=int, default=10)

    subparsers.add_parser("next", help="Fetch next task")
    subparsers.add_parser("pop", help="Pop a successful task")

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
        queue.append(_parse_payload(args.payload), priority=args.priority)
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
    if args.command == "resolve-anomalies":
        queue.resolve_anomalies(dry_run=not args.apply)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
