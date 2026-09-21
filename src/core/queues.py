from typing import Optional
from redis import Redis
from rq import Queue

from src.core.config import settings

QUEUE_LEGACY = "vod_tasks"
QUEUE_PRIORITY = "vod_priority"
QUEUE_INGEST = "vod_ingest"
QUEUE_BATCH = "vod_batch"

# Post-processing queues
QUEUE_BACKUP = "vod_backup"
QUEUE_SUBTITLES = "vod_subtitles"
QUEUE_SYNC = "vod_sync"

TRANSCODE_QUEUES = [QUEUE_PRIORITY, QUEUE_INGEST, QUEUE_BATCH, QUEUE_LEGACY]
POST_PROCESS_QUEUES = [QUEUE_BACKUP, QUEUE_SUBTITLES, QUEUE_SYNC]
ALL_QUEUES = [
    QUEUE_PRIORITY,
    QUEUE_INGEST,
    QUEUE_BATCH,
    QUEUE_LEGACY,
    QUEUE_BACKUP,
    QUEUE_SUBTITLES,
    QUEUE_SYNC,
]

def get_redis_connection(url: Optional[str] = None) -> Redis:
    redis_url = url or settings.REDIS_URL
    return Redis.from_url(redis_url)

def get_queue(queue_name: str, connection: Optional[Redis] = None) -> Queue:
    conn = connection or get_redis_connection()
    return Queue(name=queue_name, connection=conn)

def get_all_queues(connection: Optional[Redis] = None) -> dict[str, Queue]:
    conn = connection or get_redis_connection()
    return {q_name: Queue(name=q_name, connection=conn) for q_name in ALL_QUEUES}

def get_queue_depths(connection: Optional[Redis] = None) -> dict[str, int]:
    queues = get_all_queues(connection)
    return {q_name: q.count for q_name, q in queues.items()}
