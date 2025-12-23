from mongotq import get_task_queue

queue = get_task_queue(
    database_name="mydb",
    collection_name="myqueue",
    host="mongodb://localhost:27017",
    ttl=-1,
)

for i in range(5):
    queue.append({"job": i})

print("Enqueued 5 jobs.")
