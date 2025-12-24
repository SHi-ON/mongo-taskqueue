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
                   ensure_indexes: bool = True,
                   visibility_timeout: int = 0,
                   retry_backoff_base: int = 0,
                   retry_backoff_max: int = 0,
                   dead_letter_collection: Optional[str] = None,
                   meta_collection: Optional[str] = None,
                   rate_limit_per_second: Optional[float] = None) -> TaskQueue:
    """
    Returns a TaskQueue instance for the given collection name.
    If the TaskQueue collection does not exist, a new MongoDB collection
    will be created.

    :param tag: consumer process tag by which tasks are assigned with.
    :param max_retries: maximum number of retries before discarding a Task.
    :param discard_strategy: whether to "keep" or to "remove" discarded tasks.
    :param client_options: MongoClient keyword arguments.
    :param ensure_indexes: whether to create indexes if missing.
    :param visibility_timeout: lease duration in seconds for pending tasks.
    :param retry_backoff_base: base delay in seconds for retry backoff.
    :param retry_backoff_max: maximum delay in seconds for retry backoff.
    :param dead_letter_collection: collection name for dead-letter tasks.
    :param meta_collection: collection name for rate limit metadata.
    :param rate_limit_per_second: global dequeue rate limit.
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
        visibility_timeout=visibility_timeout,
        retry_backoff_base=retry_backoff_base,
        retry_backoff_max=retry_backoff_max,
        dead_letter_collection=dead_letter_collection,
        meta_collection=meta_collection,
        rate_limit_per_second=rate_limit_per_second,
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
