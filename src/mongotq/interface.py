import logging
from typing import Any, Dict, List, Optional, Union

from mongotq.task_queue import TaskQueue


def get_task_queue(database_name: str,
                   collection_name: str,
                   host: Union[str, List[str]],
                   ttl: int,
                   tag: Optional[str] = None,
                   max_retries: int = 3,
                   discard_strategy: str = 'keep',
                   client_options: Optional[Dict[str, Any]] = None,
                   ensure_indexes: bool = True) -> TaskQueue:
    """
    Returns a TaskQueue instance for the given collection name.
    If the TaskQueue collection does not exist, a new MongoDB collection
    will be created.

    :param tag: consumer process tag by which tasks are assigned with.
    :param max_retries: maximum number of retries before discarding a Task.
    :param discard_strategy: whether to "keep" or to "remove" discarded tasks.
    :param client_options: MongoClient keyword arguments.
    :param ensure_indexes: whether to create indexes if missing.
    :return: a TaskQueue instance
    """
    queue = TaskQueue(
        database=database_name,
        collection=collection_name,
        host=host,
        tag=tag,
        ttl=ttl,
        max_retries=max_retries,
        discard_strategy=discard_strategy,
        client_options=client_options,
    )
    collection_names = queue.database.list_collection_names()
    # Creates a new collection and an index
    if collection_name not in collection_names:
        queue.database.create_collection(name=collection_name)
        if ensure_indexes:
            queue.create_index()
        logging.info(f'TaskQueue collection created: {collection_name}')
    elif ensure_indexes:
        queue.create_index()
    return queue
