from mongotq import get_task_queue

queue = get_task_queue(
    database_name="mydb",
    collection_name="myqueue",
    host="mongodb://localhost:27017",
    ttl=60,
)

while True:
    task = queue.next()
    if task is None:
        print("No tasks available.")
        break
    print(f"Processing {task.payload}")
    queue.on_success(task)
