import datetime
import os
import unittest

from mongotq import (
    STATUS_FAILED,
    STATUS_NEW,
    STATUS_PENDING,
    STATUS_SUCCESSFUL,
    Task,
    TaskQueue,
    get_task_queue,
)

if os.getenv("GITHUB_ACTIONS") != "true":
    raise unittest.SkipTest("Tests run in GitHub Actions only.")

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "mongotq_test")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "taskqueue")


@unittest.skipUnless(MONGO_URI, "MONGO_URI not set")
class TestTaskQueueIntegration(unittest.TestCase):
    def setUp(self):
        collection_name = f"{MONGO_COLLECTION}_{self._testMethodName}"
        self.queue = get_task_queue(
            database_name=MONGO_DATABASE,
            collection_name=collection_name,
            host=MONGO_URI,
            ttl=1,
        )
        self.queue.collection.delete_many({})

    def tearDown(self):
        self.queue.database.drop_collection(self.queue.collection_name)

    def test_append_next_success_pop(self):
        self.queue.append({"job": 1})
        task = self.queue.next()
        self.assertIsNotNone(task)
        self.assertEqual(task.status, STATUS_PENDING)
        self.assertEqual(task.assignedTo, self.queue.assignment_tag)

        self.queue.on_success(task)
        popped = self.queue.pop()
        self.assertIsNotNone(popped)
        self.assertEqual(popped._id, task._id)

    def test_on_retry_requeues_task(self):
        self.queue.append({"job": "retry"})
        task = self.queue.next()
        self.assertIsNotNone(task)

        self.queue.on_retry(task)
        doc = self.queue.collection.find_one({"_id": task._id})
        self.assertIsNotNone(doc)
        self.assertEqual(doc["status"], STATUS_NEW)
        self.assertIsNone(doc["assignedTo"])
        self.assertEqual(doc["retries"], 1)

    def test_expire_tasks_marks_failed(self):
        old_timestamp = datetime.datetime.now().timestamp() - 10
        task = Task(
            assignedTo="worker",
            status=STATUS_PENDING,
            modifiedAt=old_timestamp,
            payload={"job": "expire"},
        )
        self.queue.collection.insert_one(task)

        self.queue.refresh()
        doc = self.queue.collection.find_one({"_id": task._id})
        self.assertIsNotNone(doc)
        self.assertEqual(doc["status"], STATUS_FAILED)
        self.assertIsNone(doc["assignedTo"])
        self.assertEqual(doc["retries"], 1)

    def test_discard_strategy_remove(self):
        queue = TaskQueue(
            database=MONGO_DATABASE,
            collection=f"{self.queue.collection_name}_discard",
            host=MONGO_URI,
            ttl=-1,
            max_retries=1,
            discard_strategy="remove",
        )
        failed = Task(status=STATUS_FAILED, retries=1, payload={"job": "fail"})
        queue.collection.insert_one(failed)

        queue.refresh()
        self.assertEqual(queue.collection.count_documents({}), 0)
        queue.database.drop_collection(queue.collection_name)

    def test_discard_strategy_keep(self):
        queue = TaskQueue(
            database=MONGO_DATABASE,
            collection=f"{self.queue.collection_name}_keep",
            host=MONGO_URI,
            ttl=-1,
            max_retries=1,
            discard_strategy="keep",
        )
        failed = Task(status=STATUS_FAILED, retries=2, payload={"job": "keep"})
        queue.collection.insert_one(failed)

        queue.refresh()
        self.assertEqual(queue.collection.count_documents({}), 1)
        queue.database.drop_collection(queue.collection_name)

    def test_resolve_anomalies_fix(self):
        now = datetime.datetime.now().timestamp()
        task = Task(
            assignedTo="worker",
            status=STATUS_SUCCESSFUL,
            modifiedAt=now - 5,
            payload={"job": "anomaly"},
        )
        self.queue.collection.insert_one(task)

        self.queue.resolve_anomalies(dry_run=False)
        doc = self.queue.collection.find_one({"_id": task._id})
        self.assertIsNotNone(doc)
        self.assertIsNone(doc["assignedTo"])
        self.assertEqual(doc["status"], STATUS_FAILED)
        self.assertEqual(doc["retries"], 1)

    def test_bulk_append_empty(self):
        self.queue.bulk_append([])
        self.assertEqual(self.queue.size(), 0)

    def test_bulk_append_inserts(self):
        tasks = [Task(payload={"job": i}) for i in range(3)]
        self.queue.bulk_append(tasks)
        self.assertEqual(self.queue.size(), 3)

    def test_append_many_inserts(self):
        self.queue.append_many([{ "job": "a" }, { "job": "b" }], priority=1)
        self.assertEqual(self.queue.size(), 2)

    def test_delayed_task(self):
        now = datetime.datetime.now().timestamp()
        scheduled_at = now + 60
        self.queue.append({"job": "delayed"}, scheduled_at=scheduled_at)
        self.assertIsNone(self.queue.next())
        self.queue.collection.update_one(
            {"payload.job": "delayed"},
            {"$set": {"scheduledAt": now - 1}},
        )
        task = self.queue.next()
        self.assertIsNotNone(task)

    def test_dedupe_key(self):
        inserted = self.queue.append({"job": "dedupe"}, dedupe_key="job-1")
        self.assertTrue(inserted)
        inserted = self.queue.append({"job": "dedupe"}, dedupe_key="job-1")
        self.assertFalse(inserted)
        count = self.queue.collection.count_documents({"dedupeKey": "job-1"})
        self.assertEqual(count, 1)

    def test_visibility_timeout_requeues(self):
        queue = get_task_queue(
            database_name=MONGO_DATABASE,
            collection_name=f"{self.queue.collection_name}_lease",
            host=MONGO_URI,
            ttl=-1,
            visibility_timeout=1,
        )
        task = Task(
            assignedTo="worker",
            status=STATUS_PENDING,
            leaseExpiresAt=datetime.datetime.now().timestamp() - 10,
            payload={"job": "lease"},
        )
        queue.collection.insert_one(task)
        queue.refresh()
        doc = queue.collection.find_one({"_id": task._id})
        self.assertEqual(doc["status"], STATUS_NEW)
        self.assertIsNone(doc["assignedTo"])
        queue.database.drop_collection(queue.collection_name)

    def test_heartbeat_extends_lease(self):
        queue = get_task_queue(
            database_name=MONGO_DATABASE,
            collection_name=f"{self.queue.collection_name}_heartbeat",
            host=MONGO_URI,
            ttl=-1,
            visibility_timeout=5,
        )
        now = datetime.datetime.now().timestamp()
        task = Task(
            assignedTo=queue.assignment_tag,
            status=STATUS_PENDING,
            leaseExpiresAt=now + 1,
            payload={"job": "heartbeat"},
        )
        queue.collection.insert_one(task)
        updated = queue.heartbeat(task, extend_by=10)
        self.assertIsNotNone(updated)
        self.assertGreater(updated.leaseExpiresAt, now + 1)
        queue.database.drop_collection(queue.collection_name)

    def test_backoff_on_failure(self):
        queue = get_task_queue(
            database_name=MONGO_DATABASE,
            collection_name=f"{self.queue.collection_name}_backoff",
            host=MONGO_URI,
            ttl=-1,
            retry_backoff_base=2,
            retry_backoff_max=10,
        )
        queue.append({"job": "backoff"})
        task = queue.next()
        self.assertIsNotNone(task)
        queue.on_failure(task, error_message="fail")
        doc = queue.collection.find_one({"_id": task._id})
        self.assertEqual(doc["status"], STATUS_NEW)
        self.assertIsNotNone(doc["scheduledAt"])
        queue.database.drop_collection(queue.collection_name)

    def test_dead_letter_collection(self):
        queue = get_task_queue(
            database_name=MONGO_DATABASE,
            collection_name=f"{self.queue.collection_name}_dead",
            host=MONGO_URI,
            ttl=-1,
            max_retries=1,
            discard_strategy="remove",
            dead_letter_collection=f"{self.queue.collection_name}_dead_letters",
        )
        failed = Task(status=STATUS_FAILED, retries=1, payload={"job": "dead"})
        queue.collection.insert_one(failed)
        queue.refresh()
        self.assertEqual(queue.collection.count_documents({}), 0)
        self.assertEqual(queue.dead_letter_collection.count_documents({}), 1)
        queue.database.drop_collection(queue.collection_name)
        queue.database.drop_collection(queue.dead_letter_collection_name)

    def test_rate_limit_global(self):
        queue = get_task_queue(
            database_name=MONGO_DATABASE,
            collection_name=f"{self.queue.collection_name}_rate",
            host=MONGO_URI,
            ttl=-1,
            rate_limit_per_second=0.1,
        )
        queue.append({"job": "rate-1"})
        queue.append({"job": "rate-2"})
        task = queue.next()
        self.assertIsNotNone(task)
        self.assertIsNone(queue.next())
        queue.database.drop_collection(queue.collection_name)

    def test_rate_limit_key(self):
        queue = get_task_queue(
            database_name=MONGO_DATABASE,
            collection_name=f"{self.queue.collection_name}_ratekey",
            host=MONGO_URI,
            ttl=-1,
            rate_limit_per_second=0.1,
        )
        queue.append({"job": "rate-key-1"}, rate_limit_key="alpha")
        queue.append({"job": "rate-key-2"}, rate_limit_key="alpha")
        task = queue.next()
        self.assertIsNotNone(task)
        self.assertIsNone(queue.next())
        doc = queue.collection.find_one({"payload.job": "rate-key-2"})
        self.assertIsNotNone(doc)
        self.assertIsNotNone(doc.get("scheduledAt"))
        queue.database.drop_collection(queue.collection_name)

    def test_next_many_zero_returns_all(self):
        for i in range(3):
            self.queue.append({"job": i})

        tasks = self.queue.next_many(0)
        self.assertEqual(len(tasks), 3)

    def test_next_many_assigns(self):
        for i in range(3):
            self.queue.append({"job": i})

        tasks = self.queue.next_many(2)
        self.assertEqual(len(tasks), 2)
        ids = {task._id for task in tasks}
        self.assertEqual(len(ids), 2)
        for task in tasks:
            self.assertEqual(task.status, STATUS_PENDING)
            self.assertEqual(task.assignedTo, self.queue.assignment_tag)

        remaining = self.queue.collection.count_documents(
            {"status": STATUS_NEW, "assignedTo": None}
        )
        self.assertEqual(remaining, 1)


if __name__ == "__main__":
    unittest.main()
