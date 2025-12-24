import datetime
import os
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional, Union

from pymongo.collection import ReturnDocument
from pymongo.errors import DuplicateKeyError
from pymongo.read_preferences import ReadPreference

from mongotq.task import (
    STATUS_FAILED,
    STATUS_NEW,
    STATUS_PENDING,
    STATUS_SUCCESSFUL,
    Task,
)

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except ImportError as exc:  # pragma: no cover
    raise ImportError("Install mongotq[async] to use AsyncTaskQueue") from exc


class AsyncTaskQueue:
    def __init__(
        self,
        database: str,
        collection: str,
        host: Union[str, List[str]],
        tag: Optional[str] = None,
        ttl: int = -1,
        max_retries: int = 3,
        discard_strategy: str = "keep",
        client_options: Optional[Dict[str, Any]] = None,
        visibility_timeout: int = 0,
        retry_backoff_base: int = 0,
        retry_backoff_max: int = 0,
        dead_letter_collection: Optional[str] = None,
        meta_collection: Optional[str] = None,
        rate_limit_per_second: Optional[float] = None,
    ) -> None:
        if ttl < -1:
            raise ValueError("ttl must be -1 or >= 0")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if visibility_timeout < 0:
            raise ValueError("visibility_timeout must be >= 0")
        if retry_backoff_base < 0:
            raise ValueError("retry_backoff_base must be >= 0")
        if retry_backoff_max < 0:
            raise ValueError("retry_backoff_max must be >= 0")
        if rate_limit_per_second is not None and rate_limit_per_second <= 0:
            raise ValueError("rate_limit_per_second must be > 0")

        self.database_name = database
        self.collection_name = collection
        self.host = host
        self.tag = tag
        self.ttl = ttl
        self.max_retries = max_retries
        discard_strategy = (discard_strategy or "keep").lower()
        if discard_strategy not in {"keep", "remove"}:
            raise ValueError("discard_strategy must be \"keep\" or \"remove\"")
        self.discard_strategy = discard_strategy
        self.client_options = client_options or {}
        self.visibility_timeout = visibility_timeout
        self.retry_backoff_base = retry_backoff_base
        self.retry_backoff_max = retry_backoff_max
        self.dead_letter_collection_name = dead_letter_collection
        self.meta_collection_name = meta_collection or f"{collection}_meta"
        self.rate_limit_per_second = rate_limit_per_second

        self._mongo_client: Optional[AsyncIOMotorClient] = None

    def _get_mongo_client(self) -> AsyncIOMotorClient:
        if self._mongo_client is None:
            options = {
                "tlsAllowInvalidCertificates": True,
                "read_preference": ReadPreference.SECONDARY_PREFERRED,
            }
            options.update(self.client_options)
            self._mongo_client = AsyncIOMotorClient(host=self.host, **options)
        return self._mongo_client

    @property
    def database(self):
        return self._get_mongo_client().get_database(self.database_name)

    @property
    def collection(self):
        return self.database.get_collection(self.collection_name)

    @property
    def meta_collection(self):
        return self.database.get_collection(self.meta_collection_name)

    @property
    def dead_letter_collection(self):
        if not self.dead_letter_collection_name:
            return None
        return self.database.get_collection(self.dead_letter_collection_name)

    @property
    def assignment_tag(self) -> str:
        return self.tag or f"consumer_{os.getpid()}"

    @staticmethod
    def _now_timestamp() -> float:
        return datetime.datetime.now().timestamp()

    def _compute_backoff(self, retries: int) -> float:
        if self.retry_backoff_base <= 0:
            return 0.0
        delay = self.retry_backoff_base * (2 ** max(retries - 1, 0))
        if self.retry_backoff_max:
            delay = min(delay, self.retry_backoff_max)
        return float(delay)

    async def _release_task(self, task: Task, delay_seconds: float = 0.0) -> None:
        scheduled_at = None
        if delay_seconds:
            scheduled_at = self._now_timestamp() + delay_seconds
        await self.collection.update_one(
            {
                "_id": task.object_id_,
                "assignedTo": self.assignment_tag,
                "status": STATUS_PENDING,
            },
            {
                "$set": {
                    "assignedTo": None,
                    "leaseId": None,
                    "leaseExpiresAt": None,
                    "scheduledAt": scheduled_at,
                    "modifiedAt": self._now_timestamp(),
                    "status": STATUS_NEW,
                }
            },
        )

    async def _acquire_rate_limit(self, key: Optional[str] = None) -> bool:
        rate_limit = self.rate_limit_per_second
        if rate_limit is None:
            return True
        interval = 1.0 / rate_limit
        now = time.time()
        meta_id = "rate_limit" if key is None else f"rate_limit:{key}"
        doc = await self.meta_collection.find_one({"_id": meta_id})
        last = doc.get("lastDequeuedAt") if doc else None
        if last is not None and last > now - interval:
            return False
        await self.meta_collection.update_one(
            {"_id": meta_id}, {"$set": {"lastDequeuedAt": now}}, upsert=True
        )
        return True

    async def append(
        self,
        payload: Any,
        priority: int = 0,
        delay_seconds: int = 0,
        scheduled_at: Optional[float] = None,
        dedupe_key: Optional[str] = None,
        rate_limit_key: Optional[str] = None,
    ) -> bool:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        if scheduled_at is not None and delay_seconds:
            raise ValueError("use delay_seconds or scheduled_at, not both")
        if delay_seconds:
            scheduled_at = self._now_timestamp() + delay_seconds
        task = Task(
            priority=priority,
            payload=payload,
            scheduledAt=scheduled_at,
            dedupeKey=dedupe_key,
            rateLimitKey=rate_limit_key,
        )
        try:
            await self.collection.insert_one(task)
        except DuplicateKeyError:
            return False
        return True

    async def append_many(
        self,
        payloads: Iterable[Any],
        priority: int = 0,
        delay_seconds: int = 0,
        scheduled_at: Optional[float] = None,
    ) -> None:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        if scheduled_at is not None and delay_seconds:
            raise ValueError("use delay_seconds or scheduled_at, not both")
        if delay_seconds:
            scheduled_at = self._now_timestamp() + delay_seconds
        tasks = [
            Task(priority=priority, payload=payload, scheduledAt=scheduled_at)
            for payload in payloads
        ]
        if not tasks:
            return
        await self.collection.insert_many(tasks)

    async def next(self) -> Optional[Task]:
        if not await self._acquire_rate_limit():
            return None
        now = self._now_timestamp()
        lease_id = uuid.uuid4().hex
        lease_expires_at = None
        if self.visibility_timeout:
            lease_expires_at = now + self.visibility_timeout
        doc = await self.collection.find_one_and_update(
            filter={
                "assignedTo": None,
                "status": STATUS_NEW,
                "retries": {"$lt": self.max_retries},
                "$or": [
                    {"scheduledAt": {"$exists": False}},
                    {"scheduledAt": {"$lte": now}},
                    {"scheduledAt": None},
                ],
            },
            update={
                "$set": {
                    "assignedTo": self.assignment_tag,
                    "leaseId": lease_id,
                    "leaseExpiresAt": lease_expires_at,
                    "modifiedAt": now,
                    "status": STATUS_PENDING,
                }
            },
            sort=[("priority", -1), ("createdAt", 1)],
            return_document=ReturnDocument.AFTER,
        )
        task = Task(**doc) if doc else None
        if task is None:
            return None
        rate_limit = self.rate_limit_per_second
        if task.rateLimitKey and rate_limit is not None:
            if not await self._acquire_rate_limit(task.rateLimitKey):
                await self._release_task(task, delay_seconds=1.0 / rate_limit)
                return None
        return task

    async def on_success(self, task: Task) -> None:
        task.assignedTo = None
        task.leaseId = None
        task.leaseExpiresAt = None
        task.modifiedAt = self._now_timestamp()
        task.status = STATUS_SUCCESSFUL
        task.scheduledAt = None
        await self.collection.update_one(
            {"_id": task.object_id_}, {"$set": dict(task)}
        )

    async def on_failure(
        self,
        task: Task,
        error_message: Optional[str] = None,
    ) -> None:
        task.assignedTo = None
        task.leaseId = None
        task.leaseExpiresAt = None
        task.modifiedAt = self._now_timestamp()
        task.errorMessage = error_message
        task.retries += 1
        delay = self._compute_backoff(task.retries)
        if delay and task.retries < self.max_retries:
            task.status = STATUS_NEW
            task.scheduledAt = self._now_timestamp() + delay
        else:
            task.status = STATUS_FAILED
            task.scheduledAt = None
        await self.collection.update_one(
            {"_id": task.object_id_}, {"$set": dict(task)}
        )

    async def refresh(self) -> None:
        await self._requeue_expired_leases()
        await self._expire_tasks()
        await self._discard_tasks()

    async def _expire_tasks(self) -> None:
        if self.ttl == -1:
            return
        now = self._now_timestamp()
        ttl_ago = now - self.ttl
        await self.collection.update_many(
            filter={
                "assignedTo": {"$ne": None},
                "modifiedAt": {"$lt": ttl_ago},
                "status": {"$eq": STATUS_PENDING},
            },
            update={
                "$set": {
                    "assignedTo": None,
                    "leaseId": None,
                    "leaseExpiresAt": None,
                    "modifiedAt": now,
                    "status": STATUS_FAILED,
                },
                "$inc": {"retries": 1},
            },
        )

    async def _requeue_expired_leases(self) -> None:
        if self.visibility_timeout <= 0:
            return
        now = self._now_timestamp()
        await self.collection.update_many(
            filter={
                "assignedTo": {"$ne": None},
                "leaseExpiresAt": {"$lt": now},
                "status": {"$eq": STATUS_PENDING},
            },
            update={
                "$set": {
                    "assignedTo": None,
                    "leaseId": None,
                    "leaseExpiresAt": None,
                    "scheduledAt": None,
                    "modifiedAt": now,
                    "status": STATUS_NEW,
                },
                "$inc": {"retries": 1},
            },
        )

    async def _discard_tasks(self) -> None:
        if self.discard_strategy == "keep":
            return
        filter_predicate = {
            "status": STATUS_FAILED,
            "retries": {"$gte": self.max_retries},
        }
        if self.dead_letter_collection is not None:
            docs = await self.collection.find(filter_predicate).to_list(None)
            if not docs:
                return
            timestamp = self._now_timestamp()
            for doc in docs:
                doc["discardedAt"] = timestamp
            await self.dead_letter_collection.insert_many(docs)
            await self.collection.delete_many(
                {"_id": {"$in": [doc["_id"] for doc in docs]}}
            )
        else:
            await self.collection.delete_many(filter_predicate)

    async def create_index(self) -> None:
        await self.collection.create_index(
            [("_id", 1), ("assignedTo", 1), ("modifiedAt", 1)],
            background=True,
        )
        await self.collection.create_index(
            [
                ("status", 1),
                ("assignedTo", 1),
                ("scheduledAt", 1),
                ("priority", -1),
                ("createdAt", 1),
            ],
            background=True,
        )
        await self.collection.create_index([("leaseExpiresAt", 1)], background=True)
        await self.collection.create_index(
            [("status", 1), ("retries", -1)], background=True
        )
        await self.collection.create_index(
            [("dedupeKey", 1)],
            unique=True,
            partialFilterExpression={
                "dedupeKey": {"$exists": True},
                "$or": [
                    {"status": STATUS_NEW},
                    {"status": STATUS_PENDING},
                    {"status": STATUS_FAILED},
                ],
            },
            background=True,
        )
        if self.dead_letter_collection is not None:
            await self.dead_letter_collection.create_index(
                [("discardedAt", 1)], background=True
            )

    async def close(self) -> None:
        if self._mongo_client is None:
            return
        self._mongo_client.close()
        self._mongo_client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
