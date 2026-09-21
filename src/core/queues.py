from typing import Optional
from redis import Redis
from rq import Queue

from src.core.config import settings

QUEUE_LEGACY = "vod_tasks"
QUEUE_PRIORITY = "vod_priority"
QUEUE_BATCH = "vod_batch"

ALL_QUEUES = [QUEUE_PRIORITY, QUEUE_BATCH, QUEUE_LEGACY]

def get_redis_connection(url: Optional[str] = None) -> Redis:
    redis_url = url or settings.REDIS_URL
    return Redis.from_url(redis_url)

def get_queue(queue_name: str, connection: Optional[Redis] = None) -> Queue:
    conn = connection or get_redis_connection()
    return Queue(name=queue_name, connection=conn)

def get_all_queues(connection: Optional[Redis] = None) -> dict[str, Queue]:
    conn = connection or get_redis_connection()
    return {
        QUEUE_LEGACY: Queue(name=QUEUE_LEGACY, connection=conn),
        QUEUE_PRIORITY: Queue(name=QUEUE_PRIORITY, connection=conn),
        QUEUE_BATCH: Queue(name=QUEUE_BATCH, connection=conn),
    }

def get_queue_depths(connection: Optional[Redis] = None) -> dict[str, int]:
    queues = get_all_queues(connection)
    return {q_name: q.count for q_name, q in queues.items()}
