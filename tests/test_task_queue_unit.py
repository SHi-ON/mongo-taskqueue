import datetime
import os
import unittest

from mongotq import Task
from mongotq.task_queue import TaskQueue

if os.getenv("GITHUB_ACTIONS") != "true":
    raise unittest.SkipTest("Tests run in GitHub Actions only.")


class TestTaskQueueUnit(unittest.TestCase):
    def test_discard_strategy_validation(self):
        with self.assertRaises(ValueError):
            TaskQueue(
                database="db",
                collection="col",
                host="mongodb://localhost:27017",
                discard_strategy="invalid",
            )

    def test_ttl_validation(self):
        with self.assertRaises(ValueError):
            TaskQueue(
                database="db",
                collection="col",
                host="mongodb://localhost:27017",
                ttl=-2,
            )

    def test_max_retries_validation(self):
        with self.assertRaises(ValueError):
            TaskQueue(
                database="db",
                collection="col",
                host="mongodb://localhost:27017",
                max_retries=-1,
            )

    def test_visibility_timeout_validation(self):
        with self.assertRaises(ValueError):
            TaskQueue(
                database="db",
                collection="col",
                host="mongodb://localhost:27017",
                visibility_timeout=-1,
            )

    def test_rate_limit_validation(self):
        with self.assertRaises(ValueError):
            TaskQueue(
                database="db",
                collection="col",
                host="mongodb://localhost:27017",
                rate_limit_per_second=0,
            )

    def test_next_many_negative_count(self):
        queue = TaskQueue(
            database="db",
            collection="col",
            host="mongodb://localhost:27017",
        )
        with self.assertRaises(ValueError):
            queue.next_many(-1)

    def test_task_repr_handles_datetimes(self):
        now = datetime.datetime.now()
        task = Task(createdAt=now, modifiedAt=now)
        repr(task)

    def test_backoff_computation(self):
        queue = TaskQueue(
            database="db",
            collection="col",
            host="mongodb://localhost:27017",
            retry_backoff_base=2,
            retry_backoff_max=5,
        )
        self.assertEqual(queue._compute_backoff(1), 2.0)
        self.assertEqual(queue._compute_backoff(2), 4.0)
        self.assertEqual(queue._compute_backoff(3), 5.0)


if __name__ == "__main__":
    unittest.main()
