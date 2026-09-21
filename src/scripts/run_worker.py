import logging
from redis import Redis
from rq import Worker
from src.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run(burst: bool = False, redis_url: str = None):
    logger.info("Starting RQ Worker...")
    url = redis_url or settings.REDIS_URL
    redis_conn = Redis.from_url(url)
    
    worker = Worker([settings.RQ_QUEUE_NAME], connection=redis_conn)
    worker.work(with_scheduler=True, burst=burst)

if __name__ == '__main__':
    run()
