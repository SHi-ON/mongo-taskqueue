import datetime
import unittest

from mongotq import Task
from mongotq.task_queue import TaskQueue


class TestTaskQueueUnit(unittest.TestCase):
    def test_discard_strategy_validation(self):
        with self.assertRaises(ValueError):
            TaskQueue(
                database="db",
                collection="col",
                host="mongodb://localhost:27017",
                discard_strategy="invalid",
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


if __name__ == "__main__":
    unittest.main()
