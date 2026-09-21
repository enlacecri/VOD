import sys
import argparse
import logging
from redis import Redis
from rq import Worker
from src.core.config import settings
from src.core.queues import QUEUE_LEGACY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run(burst: bool = False, redis_url: str = None, queue_name: str = None, worker_name: str = None):
    target_queue = queue_name or settings.RQ_QUEUE_NAME or QUEUE_LEGACY
    url = redis_url or settings.REDIS_URL
    logger.info(f"Starting RQ Worker on queue '{target_queue}' (name: {worker_name or 'default'})...")
    redis_conn = Redis.from_url(url)
    
    worker = Worker([target_queue], name=worker_name, connection=redis_conn)
    worker.work(with_scheduler=True, burst=burst)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run an RQ worker for a specific queue.")
    parser.add_argument("--queue", default=None, help="Queue name to listen on (default: RQ_QUEUE_NAME / vod_tasks)")
    parser.add_argument("--name", default=None, help="Worker name identifier")
    parser.add_argument("--burst", action="store_true", help="Run worker in burst mode (exit after queue is empty)")
    args = parser.parse_args()

    run(burst=args.burst, queue_name=args.queue, worker_name=args.name)

