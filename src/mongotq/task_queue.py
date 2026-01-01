import datetime
import logging
import os
import time
import uuid
from collections.abc import Mapping, MutableMapping
from functools import cached_property
from pprint import pprint
from typing import Any, Dict, Generator, Iterable, List, Optional, Union, cast

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection, ReturnDocument
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, InvalidOperation
from pymongo.read_preferences import ReadPreference

from mongotq.anomalies import NonPendingAssignedAnomaly
from mongotq.task import (
    STATUS_FAILED,
    STATUS_NEW,
    STATUS_PENDING,
    STATUS_SUCCESSFUL,
    Task,
)


def get_mongo_client(mongo_host: Union[str, List[str]],
                     client_options: Optional[Dict[str, Any]] = None
                     ) -> MongoClient:
    """
    Returns a MongoDB client instance.

    :param mongo_host: MongoDB instance hostname or a qualified URI.
                       Multiple hosts can be provided as a list.
    :return: the MongoDB client instance
    """
    options: Dict[str, Any] = {
        'tlsAllowInvalidCertificates': True,
        'read_preference': ReadPreference.SECONDARY_PREFERRED,
    }
    if client_options:
        options.update(client_options)
    return MongoClient(host=mongo_host, **options)


class TaskQueue:
    """A queueing data structure on top of a MongoDB collection"""

    def __init__(
            self,
            database: str,
            collection: str,
            host: Union[str, List[str]],
            tag: Optional[str] = None,
            ttl: int = -1,
            max_retries: int = 3,
            discard_strategy: str = 'keep',
            client_options: Optional[Dict[str, Any]] = None,
            visibility_timeout: int = 0,
            retry_backoff_base: int = 0,
            retry_backoff_max: int = 0,
            dead_letter_collection: Optional[str] = None,
            meta_collection: Optional[str] = None,
            rate_limit_per_second: Optional[float] = None,
    ):
        """
        Instantiates a TaskQueue object using the provided MongoDB collection.

        :param database: MongoDB database name where the TaskQueue collection
                         is located at.
        :param collection: MongoDB collection name which acts as
                           the queue data structure.
        :param host: MongoDB instance hostname or a qualified URI.
                     Multiple hosts can be provided as a list.
        :param tag: consumer process tag by which tasks are assigned with.
        :param ttl: timeout in seconds to expire tasks.
                    Expired tasks are defined as tasks which has remained in
                    the `STATUS_PENDING` status for more than `self.ttl`
                    amount of time (ephemeral queue). `self.ttl=-1`
                    indicates that the TaskQueue never expires the
                    tasks (permanent queue).
        :param max_retries: maximum number of retries before discarding a Task.
        :param discard_strategy: whether to "keep" or to "remove" discarded
                                 tasks. Discarded tasks are defined as tasks
                                 which has reached the `self.max_retries`
                                 times of failure. The default strategy is
                                 to keep discarded tasks in the TaskQueue.
        :param client_options: extra keyword arguments to pass to MongoClient.
        :param visibility_timeout: lease duration in seconds for pending tasks.
        :param retry_backoff_base: base delay in seconds for retry backoff.
        :param retry_backoff_max: maximum delay in seconds for retry backoff.
        :param dead_letter_collection: collection name for dead-letter tasks.
        :param meta_collection: collection name for rate limit metadata.
        :param rate_limit_per_second: global dequeue rate limit.
        """
        self.database_name = database
        self.collection_name = collection
        self.host = host
        self.tag = tag
        if ttl < -1:
            raise ValueError('ttl must be -1 or >= 0')
        if max_retries < 0:
            raise ValueError('max_retries must be >= 0')
        if visibility_timeout < 0:
            raise ValueError('visibility_timeout must be >= 0')
        if retry_backoff_base < 0:
            raise ValueError('retry_backoff_base must be >= 0')
        if retry_backoff_max < 0:
            raise ValueError('retry_backoff_max must be >= 0')
        if rate_limit_per_second is not None and rate_limit_per_second <= 0:
            raise ValueError('rate_limit_per_second must be > 0')

        self.ttl = ttl
        self.max_retries = max_retries
        discard_strategy = (discard_strategy or 'keep').lower()
        if discard_strategy not in {'keep', 'remove'}:
            raise ValueError('discard_strategy must be "keep" or "remove"')
        self.discard_strategy = discard_strategy
        self.client_options = client_options or {}
        self.visibility_timeout = visibility_timeout
        self.retry_backoff_base = retry_backoff_base
        self.retry_backoff_max = retry_backoff_max
        self.dead_letter_collection_name = dead_letter_collection
        self.meta_collection_name = meta_collection or f'{collection}_meta'
        self.rate_limit_per_second = rate_limit_per_second

        self._mongo_client: Optional[MongoClient] = None

    def __repr__(self):
        return f'TaskQueue collection name: {self.collection_name}'

    def _get_mongo_client(self) -> MongoClient:
        """
        Returns a MongoDB client instance.

        :return: MongoDB client instance
        """
        if self._mongo_client is None:
            self._mongo_client = get_mongo_client(
                self.host,
                client_options=self.client_options,
            )
        return self._mongo_client

    @cached_property
    def database(self) -> Database:
        client = self._get_mongo_client()
        return client.get_database(self.database_name)

    @cached_property
    def collection(self) -> Collection:
        return self.database.get_collection(self.collection_name)

    @cached_property
    def meta_collection(self) -> Collection:
        return self.database.get_collection(self.meta_collection_name)

    @cached_property
    def dead_letter_collection(self) -> Optional[Collection]:
        if not self.dead_letter_collection_name:
            return None
        return self.database.get_collection(self.dead_letter_collection_name)

    @cached_property
    def assignment_tag(self) -> str:
        return self.tag or f'consumer_{os.getpid()}'

    @cached_property
    def expires(self) -> bool:
        return self.ttl != -1

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

    def _release_task(self, task: Task, delay_seconds: float = 0.0) -> None:
        scheduled_at = None
        if delay_seconds:
            scheduled_at = self._now_timestamp() + delay_seconds
        update_result = self.collection.update_one(
            filter={'_id': task.object_id_,
                    'assignedTo': self.assignment_tag,
                    'status': STATUS_PENDING},
            update={'$set': {'assignedTo': None,
                             'leaseId': None,
                             'leaseExpiresAt': None,
                             'scheduledAt': scheduled_at,
                             'modifiedAt': self._now_timestamp(),
                             'status': STATUS_NEW}},
        )
        if not update_result.acknowledged:
            raise InvalidOperation

    def _acquire_rate_limit(self, key: Optional[str] = None) -> bool:
        rate_limit = self.rate_limit_per_second
        if rate_limit is None:
            return True
        interval = 1.0 / rate_limit
        now = time.time()
        meta_id = 'rate_limit' if key is None else f'rate_limit:{key}'
        doc = self.meta_collection.find_one({'_id': meta_id})
        last = doc.get('lastDequeuedAt') if doc else None
        if last is not None and last > now - interval:
            return False
        self.meta_collection.update_one(
            {'_id': meta_id},
            {'$set': {'lastDequeuedAt': now}},
            upsert=True,
        )
        return True

    def size(self) -> int:
        """
        Returns size of the TaskQueue by counting the number of documents
        in the collection.

        :return: document count
        """
        return self.collection.count_documents({})

    def _expire_tasks(self, force: bool = False) -> None:
        """
        Updates the pending tasks which are already expired. Expired tasks
        are defined as tasks which has remained in the `STATUS_PENDING` status
        for more than `self.ttl` amount of time (ephemeral queue).

        :param force: whether to act even the TaskQueue never expires tasks.
        :return:
        """
        if self.expires or force:
            now = datetime.datetime.now()
            ttl_ago = now - datetime.timedelta(seconds=self.ttl)
            update_result = self.collection.update_many(
                filter={'assignedTo': {'$ne': None},
                        'modifiedAt': {'$lt': ttl_ago.timestamp()},
                        'status': {'$eq': STATUS_PENDING}},
                update={'$set': {'assignedTo': None,
                                 'leaseId': None,
                                 'leaseExpiresAt': None,
                                 'modifiedAt': now.timestamp(),
                                 'status': STATUS_FAILED},
                        '$inc': {'retries': 1}},
            )
            if not update_result.acknowledged:
                raise InvalidOperation
            if not update_result.matched_count:
                logging.info('no expired tasks found')
            else:
                logging.info(f'updated {update_result.modified_count} out of '
                             f'{update_result.matched_count} expired task(s).')

    def _requeue_expired_leases(self) -> None:
        if self.visibility_timeout <= 0:
            return
        now = self._now_timestamp()
        update_result = self.collection.update_many(
            filter={'assignedTo': {'$ne': None},
                    'leaseExpiresAt': {'$lt': now},
                    'status': {'$eq': STATUS_PENDING}},
            update={'$set': {'assignedTo': None,
                             'leaseId': None,
                             'leaseExpiresAt': None,
                             'scheduledAt': None,
                             'modifiedAt': now,
                             'status': STATUS_NEW},
                    '$inc': {'retries': 1}},
        )
        if not update_result.acknowledged:
            raise InvalidOperation
        if not update_result.matched_count:
            logging.info('no expired leases found')
        else:
            logging.info(f'requeued {update_result.modified_count} out of '
                         f'{update_result.matched_count} expired leases.')

    def _discard_tasks(self) -> None:
        """
        Deletes the failed tasks which has reached (or exceeded) the
        `self.max_retries` limit. Discarded tasks are defined as tasks which
        has reached the `self.max_retries` times of failure.

        :return:
        """
        if self.discard_strategy == 'keep':
            logging.info('discard strategy set to keep; skipping discard.')
            return
        filter_predicate = {'status': STATUS_FAILED,
                            'retries': {'$gte': self.max_retries}}
        if self.dead_letter_collection is not None:
            docs = list(self.collection.find(filter_predicate))
            if not docs:
                logging.info('no discardable tasks found')
                return
            timestamp = self._now_timestamp()
            for doc in docs:
                doc['discardedAt'] = timestamp
            insert_result = self.dead_letter_collection.insert_many(docs)
            if not insert_result.acknowledged:
                raise InvalidOperation
            delete_result = self.collection.delete_many(
                {'_id': {'$in': [doc['_id'] for doc in docs]}},
            )
        else:
            delete_result = self.collection.delete_many(
                filter=filter_predicate,
            )
        if not delete_result.acknowledged:
            raise InvalidOperation
        if not delete_result.deleted_count:
            logging.info('no discardable tasks found')
        else:
            logging.info(f'discarded {delete_result.deleted_count} '
                         'failed task(s).')

    def resolve_anomalies(self, dry_run: bool = True) -> None:
        """
        Updates tasks deviating from the defined lifecycle stages.
        Task anomalies could happen due to manual manipulations of
        the underlying MongoDB collection, invoking low-level methods of the
        TaskQueue out of the defined lifecycle, or any unexpected behaviors
        occurred at the backend. Found tasks with anomalies are logged as
        warnings and if `dry_run=False`, the necessary changes will be
        committed to resolve the anomalies.

        :param dry_run: whether to resolve the found anomalies or just
                        log them as warnings without updating the tasks
        :return:
        """
        now_timestamp = datetime.datetime.now().timestamp()
        anomalies = [NonPendingAssignedAnomaly(now_timestamp)]
        for anomaly in anomalies:
            anomaly_name = anomaly.__class__.__name__

            cur = self.collection.find(filter=anomaly.filter)
            if cur is None or not (docs := list(cur)):
                logging.info(f'no {anomaly_name} found')
                continue
            logging.warning(f'{len(docs)} anomalies found:')
            for doc in docs:
                pprint(self._wrap_task(doc))

            # Commit necessary changes
            if not dry_run:
                update_result = self.collection.update_many(
                    filter=anomaly.filter,
                    update=anomaly.update
                )
                if not update_result.acknowledged:
                    raise InvalidOperation
                if not update_result.matched_count:
                    logging.warning(f'no {anomaly_name} found, false alarm?!')
                logging.info(f'fixed {update_result.modified_count} out of '
                             f'{update_result.matched_count} expired tasks.')

    def refresh(self) -> None:
        """
        Update the expired and discarded tasks.
        Expired tasks are defined as tasks which has remained in
        the `STATUS_PENDING` status for more than `self.ttl`
        amount of time (ephemeral queue).
        Discarded tasks are defined as tasks which has reached
        the `self.max_retries` times of failure.

        :return:
        """
        self._requeue_expired_leases()
        self._expire_tasks()
        self._discard_tasks()

    def append(self,
               payload: Any,
               priority: int = 0,
               delay_seconds: int = 0,
               scheduled_at: Optional[float] = None,
               dedupe_key: Optional[str] = None,
               rate_limit_key: Optional[str] = None) -> bool:
        """
        Inserts a Task into the TaskQueue.

        :param payload: Task payload
        :param priority: Task priority
        :param delay_seconds: delay in seconds before task can be leased
        :param scheduled_at: absolute timestamp when task can be leased
        :param dedupe_key: optional idempotency key
        :param rate_limit_key: optional rate limit key
        :return:
        """
        if delay_seconds < 0:
            raise ValueError('delay_seconds must be >= 0')
        if scheduled_at is not None and delay_seconds:
            raise ValueError('use delay_seconds or scheduled_at, not both')
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
            insert_result = self.collection.insert_one(document=task)
        except DuplicateKeyError:
            return False
        if not insert_result.acknowledged:
            raise InvalidOperation
        return True

    def pop(self) -> Optional[Task]:
        """
        Removes a completed Task from the TaskQueue and returns it. The removed
        Task must be in `STATUS_SUCCESSFUL` status.

        :return: the removed Task
        """
        doc = self.collection.find_one_and_delete(
            filter={'status': STATUS_SUCCESSFUL},
            sort=[('priority', DESCENDING),
                  ('createdAt', ASCENDING)],
        )
        return self._wrap_task(doc)

    def bulk_append(self, tasks: Iterable[Task], ordered=True):
        documents = list(tasks)
        if not documents:
            return
        insert_result = self.collection.insert_many(documents=documents,
                                                    ordered=ordered)
        if not insert_result.acknowledged:
            raise InvalidOperation

    def update(self, task: Task) -> None:
        """
        Updates the given Task's attributes in the TaskQueue.

        :param task: the Task object that the original Task needs
                     to be updated with
        :return:
        """
        task.modifiedAt = datetime.datetime.now().timestamp()
        task_data = dict(task)
        task_data.pop('_id', None)
        update_result = self.collection.update_one(
            filter={'_id': task.object_id_},
            update={'$set': task_data},
            upsert=True,
        )
        if not update_result.acknowledged:
            raise InvalidOperation
        if update_result.upserted_id:
            logging.debug('Task upserted!')

    def next(self) -> Optional[Task]:
        """
        Gets the next unassigned Task from the TaskQueue and returns
        a wrapped `Task` object of it.

        :return: the next Task at the front of the TaskQueue
        """
        now = self._now_timestamp()
        lease_id = uuid.uuid4().hex
        lease_expires_at = None
        if self.visibility_timeout:
            lease_expires_at = now + self.visibility_timeout
        doc = self.collection.find_one_and_update(
            filter={'assignedTo': None,
                    'status': STATUS_NEW,
                    'retries': {'$lt': self.max_retries},
                    '$or': [{'scheduledAt': {'$exists': False}},
                            {'scheduledAt': {'$lte': now}},
                            {'scheduledAt': None}]},
            update={'$set': {'assignedTo': self.assignment_tag,
                             'leaseId': lease_id,
                             'leaseExpiresAt': lease_expires_at,
                             'modifiedAt': now,
                             'status': STATUS_PENDING}},
            sort=[('priority', DESCENDING),
                  ('createdAt', ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )
        task = self._wrap_task(doc)
        if task is None:
            return None
        rate_limit = self.rate_limit_per_second
        if rate_limit is not None:
            if not self._acquire_rate_limit():
                self._release_task(task, delay_seconds=1.0 / rate_limit)
                return None
            if task.rateLimitKey and not self._acquire_rate_limit(task.rateLimitKey):
                self._release_task(task, delay_seconds=1.0 / rate_limit)
                return None
        return task

    def next_many(self, count: int = 0) -> List[Task]:
        if count < 0:
            raise ValueError('count must be >= 0')
        tasks: List[Task] = []
        while count == 0 or len(tasks) < count:
            task = self.next()
            if task is None:
                break
            tasks.append(task)
        return tasks

    def head(self, n: int = 10) -> None:
        """
        Prints the n first tasks at the front of the TaskQueue.
        The front of a queue, by definition, is the end that items are removed
        from.

        :param n: number of tasks to be returned
        :return:
        """
        cur = self.collection.find(
            sort=[('priority', DESCENDING),
                  ('createdAt', ASCENDING)],
        )
        if cur is None:
            return
        for doc in cur[:n]:
            pprint(self._wrap_task(doc))
        print(f'{self.size():,} Task(s) available in the TaskQueue')

    def tail(self, n: int = 10) -> None:
        """
        Prints the last n tasks at the back of the TaskQueue.
        The back of a queue, by definition, is the end that items are inserted
        at.

        :param n: number of to be returned
        :return:
        """
        cur = self.collection.find(
            sort=[('priority', DESCENDING),
                  ('createdAt', DESCENDING)]
        )
        if cur is None:
            return
        for doc in reversed(list((cur[:n]))):
            pprint(self._wrap_task(doc))
        print(f'{self.size():,} Task(s) available in the TaskQueue')

    def status_info(self) -> None:
        stats = self.status_counts()
        total = sum(stats.values())
        m_len = len(str(total))
        padding = m_len + m_len // 3
        collection_count = self.size()
        if total != collection_count:
            logging.warning('task counts changed during aggregation')

        print('Tasks status:')
        print('Status'.ljust(15), 'Count'.ljust(padding))
        print(f'{"-" * 10}'.ljust(15), f'{"-" * padding}'.ljust(padding))
        for status, count in sorted(stats.items()):
            print(f'{status}'.ljust(15), f'{count:,}'.rjust(padding))
        print()
        print('Total:'.ljust(15), f'{total:,}'.rjust(padding))

    def status_counts(self) -> Dict[str, int]:
        cur = self.collection.aggregate([{
            '$group': {'_id': '$status',
                       'count': {'$sum': 1}}
        }])
        return {c['_id']: c['count'] for c in cur}

    def delete_iter(self, tasks: List[Task]):
        delete_result = self.collection.delete_many(
            filter={'_id': {'$in': [t['_id'] for t in tasks]}},
        )
        if not delete_result.acknowledged:
            raise InvalidOperation

    def generate_loc(self, *args, **kwargs) -> Generator:
        cur = self.collection.find(*args, **kwargs)
        for c in cur:
            yield self._wrap_task(c)

    def loc(self, *args, **kwargs) -> List[Task]:
        return list(self.generate_loc(*args, **kwargs))

    def to_list(self) -> List[Dict[str, Any]]:
        """
        Returns a list of the Tasks available in the TaskQueue.

        :return: the list of Tasks
        """
        cur = self.collection.find()
        return list(cur) if cur else []

    def to_dataframe(self):
        """
        Returns a DataFrame of the Tasks available in the TaskQueue.

        :return: the DataFrame of Tasks
        """
        df = None
        try:
            import pandas
            tasks = self.to_list()
            df = pandas.DataFrame(tasks)
        except ImportError:
            logging.warning('pandas needs to be installed!')
        return df

    # Task lifecycle
    def on_success(self, task: Task) -> None:
        """
        Task has been successfully executed.

        :param task: the successful Task
        :return:
        """
        task.assignedTo = None
        task.leaseId = None
        task.leaseExpiresAt = None
        task.modifiedAt = datetime.datetime.now().timestamp()
        task.status = STATUS_SUCCESSFUL
        task.scheduledAt = None
        self.update(task)

    def on_failure(
        self,
        task: Task,
        error_message: Optional[str] = None,
        retry: bool = True,
    ) -> None:
        """
        Task execution resulted in failure.

        :param task: the failed Task
        :param error_message: the error message to be stored with the Task
        :param retry: whether to retry with backoff if configured
        :return:
        """
        task.assignedTo = None
        task.leaseId = None
        task.leaseExpiresAt = None
        task.modifiedAt = datetime.datetime.now().timestamp()
        task.errorMessage = error_message
        task.retries += 1
        delay = self._compute_backoff(task.retries) if retry else 0.0
        if retry and delay and task.retries < self.max_retries:
            task.status = STATUS_NEW
            task.scheduledAt = self._now_timestamp() + delay
        else:
            task.status = STATUS_FAILED
            task.scheduledAt = None
        self.update(task)

    def on_retry(
        self,
        task: Task,
        delay_seconds: int = 0,
    ) -> Optional[Mapping[str, Any]]:
        """
        Task is released and its state gets updated.

        :param task: the Task that needs to be retried.
        :param delay_seconds: optional delay before requeue
        :return:
        """
        if delay_seconds < 0:
            raise ValueError('delay_seconds must be >= 0')
        scheduled_at = None
        if delay_seconds:
            scheduled_at = self._now_timestamp() + delay_seconds
        doc = self.collection.find_one_and_update(
            filter={'_id': task.object_id_,
                    'assignedTo': self.assignment_tag},
            update={'$set': {'assignedTo': None,
                             'leaseId': None,
                             'leaseExpiresAt': None,
                             'scheduledAt': scheduled_at,
                             'modifiedAt': datetime.datetime.now().timestamp(),
                             'status': STATUS_NEW},
                    '$inc': {'retries': 1}}
        )
        return cast(Optional[Mapping[str, Any]], doc)

    def heartbeat(
        self,
        task: Task,
        extend_by: Optional[int] = None,
    ) -> Optional[Task]:
        """
        Extends the lease for an assigned task.

        :param task: the Task to extend lease for
        :param extend_by: seconds to extend; defaults to visibility_timeout
        :return: updated Task or None
        """
        if self.visibility_timeout <= 0:
            return None
        extension = extend_by if extend_by is not None else self.visibility_timeout
        if extension <= 0:
            raise ValueError('extend_by must be > 0')
        now = self._now_timestamp()
        lease_expires_at = now + extension
        doc = self.collection.find_one_and_update(
            filter={'_id': task.object_id_,
                    'assignedTo': self.assignment_tag,
                    'status': STATUS_PENDING},
            update={'$set': {'leaseExpiresAt': lease_expires_at,
                             'modifiedAt': now}},
            return_document=ReturnDocument.AFTER,
        )
        return self._wrap_task(doc)

    def requeue_failed(self, delay_seconds: int = 0) -> int:
        """
        Requeues failed tasks that are still within retry limits.

        :param delay_seconds: optional delay before requeue
        :return: number of tasks requeued
        """
        if delay_seconds < 0:
            raise ValueError('delay_seconds must be >= 0')
        scheduled_at = None
        if delay_seconds:
            scheduled_at = self._now_timestamp() + delay_seconds
        update_result = self.collection.update_many(
            filter={'status': STATUS_FAILED,
                    'retries': {'$lt': self.max_retries}},
            update={'$set': {'status': STATUS_NEW,
                             'assignedTo': None,
                             'leaseId': None,
                             'leaseExpiresAt': None,
                             'scheduledAt': scheduled_at,
                             'modifiedAt': self._now_timestamp()}},
        )
        if not update_result.acknowledged:
            raise InvalidOperation
        return update_result.modified_count

    def purge(self, status: Optional[str] = None) -> int:
        """
        Purges tasks by status or all tasks when status is None.

        :param status: optional status to filter
        :return: number of deleted tasks
        """
        filter_predicate: Dict[str, Any] = {}
        if status is not None:
            filter_predicate['status'] = status
        delete_result = self.collection.delete_many(filter_predicate)
        if not delete_result.acknowledged:
            raise InvalidOperation
        return delete_result.deleted_count

    def create_index(self) -> str:
        """
        Creates a MongoDB compound index of query keys.

        :return: name of the created index
        """
        primary_index = self.collection.create_index(
            keys=[('_id', ASCENDING),
                  ('assignedTo', ASCENDING),
                  ('modifiedAt', ASCENDING)],
            background=True,
        )
        self.collection.create_index(
            keys=[('status', ASCENDING),
                  ('assignedTo', ASCENDING),
                  ('scheduledAt', ASCENDING),
                  ('priority', DESCENDING),
                  ('createdAt', ASCENDING)],
            background=True,
        )
        self.collection.create_index(
            keys=[('leaseExpiresAt', ASCENDING)],
            background=True,
        )
        self.collection.create_index(
            keys=[('status', ASCENDING),
                  ('retries', DESCENDING)],
            background=True,
        )
        self.collection.create_index(
            keys=[('dedupeKey', ASCENDING)],
            unique=True,
            partialFilterExpression={
                'dedupeKey': {'$exists': True, '$ne': None},
                '$or': [
                    {'status': STATUS_NEW},
                    {'status': STATUS_PENDING},
                    {'status': STATUS_FAILED},
                ],
            },
            background=True,
        )
        if self.dead_letter_collection is not None:
            self.dead_letter_collection.create_index(
                keys=[('discardedAt', ASCENDING)],
                background=True,
            )
        return primary_index

    @staticmethod
    def _wrap_task(doc: Optional[Mapping[str, Any]]) -> Optional[Task]:
        """
        Creates a Task object from the given document mapping.

        :param doc: MongoDB document
        :return: the created Task
        """
        if not doc or \
                not isinstance(doc, (Mapping, MutableMapping)):
            logging.debug('invalid Task')
            return None
        return Task(**doc)

    def _clear(self) -> None:
        """
        Clears out the Tasks from the TaskQueue by removing the collection from
        MongoDB.
        Should be run with caution!

        :return:
        """
        delete_result = self.collection.delete_many(
            filter={'assignedTo': {'$exists': True},
                    'createdAt': {'$exists': True},
                    'modifiedAt': {'$exists': True},
                    'status': {'$exists': True},
                    'errorMessage': {'$exists': True},
                    'retries': {'$exists': True},
                    'priority': {'$exists': True}},
        )
        if not delete_result.acknowledged:
            raise InvalidOperation

    def append_many(self,
                    payloads: Iterable[Any],
                    priority: int = 0,
                    delay_seconds: int = 0,
                    scheduled_at: Optional[float] = None) -> None:
        if delay_seconds < 0:
            raise ValueError('delay_seconds must be >= 0')
        if scheduled_at is not None and delay_seconds:
            raise ValueError('use delay_seconds or scheduled_at, not both')
        if delay_seconds:
            scheduled_at = self._now_timestamp() + delay_seconds
        tasks = [
            Task(priority=priority, payload=payload, scheduledAt=scheduled_at)
            for payload in payloads
        ]
        self.bulk_append(tasks)

    def close(self) -> None:
        if self._mongo_client is None:
            return
        self._mongo_client.close()
        self._mongo_client = None
        self.__dict__.pop('database', None)
        self.__dict__.pop('collection', None)
        self.__dict__.pop('meta_collection', None)
        self.__dict__.pop('dead_letter_collection', None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
