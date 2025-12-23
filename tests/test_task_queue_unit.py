import unittest

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


if __name__ == "__main__":
    unittest.main()
