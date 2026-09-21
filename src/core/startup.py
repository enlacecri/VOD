import os
import stat
import logging
from src.core.config import settings

logger = logging.getLogger(__name__)

def get_device(path: str) -> int:
    try:
        return os.stat(path).st_dev
    except FileNotFoundError:
        return -1

def validate_environment():
    logger.info("Validating environment...")
    
    os.makedirs(settings.INGEST_ROOT, exist_ok=True)
    os.makedirs(settings.STAGING_ROOT, exist_ok=True)
    os.makedirs(settings.OUTPUT_ROOT, exist_ok=True)
    os.makedirs(settings.PROCESSED_ROOT, exist_ok=True)
    os.makedirs(settings.LOG_ROOT, exist_ok=True)
    os.makedirs(settings.PROGRESSIVE_ROOT, exist_ok=True)
    
    dev_staging = get_device(settings.STAGING_ROOT)
    dev_output = get_device(settings.OUTPUT_ROOT)
    
    if dev_staging != dev_output:
        msg = f"CRITICAL: STAGING ({settings.STAGING_ROOT}) and OUTPUT ({settings.OUTPUT_ROOT}) are on different filesystems/devices. Atomic rename is impossible."
        logger.error(msg)
        raise RuntimeError(msg)
    else:
        logger.info("STAGING and OUTPUT are on the same filesystem. Atomic rename is supported.")
