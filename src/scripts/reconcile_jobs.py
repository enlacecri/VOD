import time
import os
import argparse
import hashlib
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
import logging

from rq import Queue
from rq.job import Job as RQJob
from rq.exceptions import NoSuchJobError
from redis import Redis, exceptions as redis_exceptions

from src.core.database import SessionLocal
from src.core.config import settings
from src.models.job import Job
from src.models.asset import Asset
from src.models.asset_event import AssetEvent
from src.models.enums import JobStatus, VideoStatus, EventType, JobType
from src.core.state import transition_asset, fail_asset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def is_safe_to_delete(target_path: Path, staging_root: Path) -> bool:
    try:
        resolved = target_path.resolve(strict=False)
        resolved.relative_to(staging_root)
        if target_path.is_symlink():
            return False
        return True
    except (ValueError, RuntimeError):
        return False

def safe_rmtree(target_path: Path, staging_root: Path):
    if not is_safe_to_delete(target_path, staging_root):
        logger.error(f"Unsafe deletion prevented for {target_path}")
        return
        
    for item in target_path.iterdir():
        if item.is_symlink():
            item.unlink()
        elif item.is_dir():
            safe_rmtree(item, staging_root)
        else:
            item.unlink()
    target_path.rmdir()

def reconcile_staging(dry_run: bool = True):
    logger.info(f"Starting staging reconciliation (dry_run={dry_run})...")
    db = SessionLocal()
    staging_root = Path(settings.STAGING_ROOT).resolve()
    
    jobs_dir = staging_root / "jobs"
    assets_dir = staging_root / "assets"
    now = datetime.now(timezone.utc)
    now_ts = now.timestamp()
    
    # 1. Clean old job temps
    if jobs_dir.exists():
        for job_temp in jobs_dir.iterdir():
            if not job_temp.is_dir():
                continue
            
            job_id_str = job_temp.name
            job = None
            try:
                job_uuid = uuid.UUID(job_id_str)
                job = db.query(Job).filter(Job.id == job_uuid).first()
            except Exception:
                pass
                
            if job:
                # Do not delete if pending or processing with recent heartbeat
                if job.status in [JobStatus.PENDING, JobStatus.PROCESSING]:
                    if job.heartbeat:
                        age_sec = (now - job.heartbeat).total_seconds()
                        if age_sec < settings.RQ_JOB_TIMEOUT_SECONDS * 2:
                            continue
                    else:
                        age_sec = (now - job.updated_at).total_seconds()
                        if age_sec < settings.RQ_JOB_TIMEOUT_SECONDS * 2:
                            continue
            
            mtime = job_temp.stat().st_mtime
            age = now_ts - mtime
            if age > settings.STAGING_ORPHAN_AGE_SECONDS and (not job or job.status not in [JobStatus.PENDING, JobStatus.PROCESSING]):
                logger.info(f"[jobs] Found orphan temp dir: {job_temp} (Age: {age:.0f}s)")
                if not dry_run:
                    safe_rmtree(job_temp, staging_root)
                    logger.info(f"[jobs] Deleted {job_temp}")

    # 2. Clean unreferenced stable assets
    if assets_dir.exists():
        for asset_temp in assets_dir.iterdir():
            if not asset_temp.is_dir():
                continue
            
            asset_uuid = asset_temp.name
            asset = db.query(Asset).filter(Asset.vod_uuid == asset_uuid).first()
            
            referenced = False
            if asset and asset.staged_source_path:
                try:
                    ref_path = (staging_root / asset.staged_source_path).resolve(strict=True)
                    # Use exact match of parents instead of startswith
                    if asset_temp.resolve(strict=True) in ref_path.parents:
                        referenced = True
                except (FileNotFoundError, RuntimeError):
                    pass
            
            if not referenced:
                mtime = asset_temp.stat().st_mtime
                age = now_ts - mtime
                if age > settings.STAGING_ORPHAN_AGE_SECONDS:
                    logger.info(f"[assets] Found unreferenced asset dir: {asset_temp} (Age: {age:.0f}s)")
                    if not dry_run:
                        safe_rmtree(asset_temp, staging_root)
                        logger.info(f"[assets] Deleted {asset_temp}")

    db.close()
    logger.info("Staging reconciliation complete.")

def validate_asset_for_queued(asset: Asset) -> bool:
    if not asset.source_sha256 or not asset.size or not asset.source_width or not asset.source_height or not asset.staged_source_path:
        return False
        
    staging_root = Path(settings.STAGING_ROOT).resolve()
    try:
        candidate = (staging_root / asset.staged_source_path).resolve(strict=True)
        candidate.relative_to(staging_root)
    except (FileNotFoundError, ValueError, RuntimeError):
        return False
        
    if not candidate.is_file():
        return False
        
    # Recalculate hash to verify
    hasher = hashlib.sha256()
    try:
        with open(candidate, "rb") as f:
            for chunk in iter(lambda: f.read(settings.HASH_CHUNK_SIZE), b""):
                hasher.update(chunk)
        if hasher.hexdigest() != asset.source_sha256:
            return False
    except OSError:
        return False
        
    return True

def reconcile_published_assets():
    logger.info("Starting published assets reconciliation...")
    db = SessionLocal()
    output_root = Path(settings.OUTPUT_ROOT).resolve()
    
    try:
        assets_to_check = db.query(Asset).filter(
            Asset.status.in_([VideoStatus.PROCESSING, VideoStatus.VALIDATING, VideoStatus.FAILED])
        ).all()
        
        for asset in assets_to_check:
            uuid_upper = str(asset.vod_uuid).upper()
            manifest_path = output_root / "EnlacePlus" / "_definst_" / f"amlst:{uuid_upper}" / str(asset.enlace_id) / "manifest.m3u8"
            if manifest_path.exists() and manifest_path.is_file():
                logger.info(f"Asset {asset.id} has published files but status is {asset.status.value}. Fixing to READY.")
                transition_asset(db, asset, VideoStatus.READY)
                asset.progress = 100
                db.commit()
    finally:
        db.close()
    logger.info("Published assets reconciliation complete.")

def reconcile_jobs():
    logger.info("Starting job reconciliation...")
    
    try:
        redis_conn = Redis.from_url(settings.REDIS_URL)
        redis_conn.ping()
    except redis_exceptions.RedisError as e:
        logger.error(f"Redis unavailable, aborting job reconciliation: {e}")
        return

    q = Queue(name=settings.RQ_QUEUE_NAME, connection=redis_conn)
    
    db = SessionLocal()
    try:
        # Fetch IDs of active jobs (no lock yet, just a snapshot)
        job_ids = [r[0] for r in db.query(Job.id).filter(
            Job.status.in_([JobStatus.PENDING, JobStatus.PROCESSING])
        ).all()]
    finally:
        db.close()
        
    if not job_ids:
        logger.info("No active jobs to reconcile.")
        return
        
    logger.info(f"Found {len(job_ids)} jobs to check.")
    
    for current_job_id in job_ids:
        db = SessionLocal()
        try:
            # Short claim transaction
            job = db.query(Job).filter(Job.id == current_job_id).with_for_update(skip_locked=True).first()
            
            if not job or job.status not in [JobStatus.PENDING, JobStatus.PROCESSING]:
                continue # Already processed or locked by another reconciler
                
            job_status_db = job.status
            job_rq_id = job.rq_job_id
            job_updated_at = job.updated_at
            job_heartbeat = job.heartbeat
            asset_id = job.asset_id
            
            # Commit to release row lock temporarily
            db.commit()
            
        except Exception as e:
            logger.error(f"Error claiming job {current_job_id}: {e}")
            db.rollback()
            continue
        finally:
            db.close()

        # Query Redis outside transaction
        redis_status = None
        rq_job = None
        try:
            rq_job = RQJob.fetch(str(current_job_id), connection=redis_conn)
            redis_status = rq_job.get_status()
        except NoSuchJobError:
            redis_status = 'missing'
            
        # Re-open transaction to apply changes
        db = SessionLocal()
        try:
            # Re-lock to verify it hasn't changed
            job = db.query(Job).filter(Job.id == current_job_id).with_for_update().first()
            if not job or job.status != job_status_db:
                continue # Changed concurrently
                
            asset = db.query(Asset).filter(Asset.id == asset_id).first()
            
            if redis_status == 'missing':
                logger.warning(f"Job {job.id} not found in Redis.")
                if job.status == JobStatus.PENDING:
                    target_queue = job.queue_name if getattr(job, "queue_name", None) else settings.RQ_QUEUE_NAME
                    logger.info(f"Re-enqueueing PENDING job {job.id} to queue '{target_queue}'")
                    task_func = (
                        "src.worker.tasks.progressive_transcode_asset_job"
                        if target_queue in ("vod_priority", "vod_batch") or job.type == JobType.TRANSCODE
                        else "src.worker.tasks.probe_and_prepare_job"
                    )
                    timeout = (
                        settings.FFMPEG_TIMEOUT_SECONDS + 300
                        if job.type == JobType.TRANSCODE
                        else settings.RQ_JOB_TIMEOUT_SECONDS
                    )
                    target_q = Queue(name=target_queue, connection=redis_conn)
                    new_rq_job = target_q.enqueue(
                        task_func,
                        args=(job.id,),
                        job_id=str(job.id),
                        job_timeout=timeout,
                        result_ttl=86400
                    )
                    job.rq_job_id = new_rq_job.id
                    db.commit()
                else:
                    fail_stuck_job(db, job, asset, "Lost from Redis while PROCESSING")
                continue
                
            logger.info(f"Job {job.id} RQ status: {redis_status}")
            
            if not job.rq_job_id and rq_job:
                job.rq_job_id = rq_job.id
                db.commit()
                
            if redis_status in ['queued', 'deferred', 'scheduled']:
                pass
            elif redis_status == 'started':
                timeout = settings.RQ_JOB_TIMEOUT_SECONDS
                if job_heartbeat:
                    age = (datetime.now(timezone.utc) - job_heartbeat).total_seconds()
                    if age > timeout * 2:
                        fail_stuck_job(db, job, asset, f"Heartbeat timeout (age {age}s)")
                else:
                    age = (datetime.now(timezone.utc) - job_updated_at).total_seconds()
                    if age > timeout * 2:
                        fail_stuck_job(db, job, asset, f"Started but no heartbeat (age {age}s)")
            elif redis_status == 'finished':
                if asset.status != VideoStatus.QUEUED:
                    if validate_asset_for_queued(asset):
                        logger.info(f"RQ finished and asset valid for job {job.id}. Fixing state.")
                        job.status = JobStatus.COMPLETED
                        job.finished_at = datetime.now(timezone.utc)
                        transition_asset(db, asset, VideoStatus.QUEUED)
                        db.commit()
                    else:
                        fail_stuck_job(db, job, asset, "RQ finished but asset validation failed")
            elif redis_status in ['failed', 'stopped', 'canceled']:
                fail_stuck_job(db, job, asset, f"RQ Job {redis_status}")
                
        except Exception as e:
            logger.error(f"Error applying reconciliation for job {current_job_id}: {e}")
            db.rollback()
        finally:
            db.close()

    logger.info("Job reconciliation complete.")

def fail_stuck_job(db, job: Job, asset: Asset, reason: str):
    logger.info(f"Failing job {job.id}: {reason}")
    job.status = JobStatus.FAILED
    job.error_code = "E_JOB_RECONCILED"
    job.error_message = reason
    job.finished_at = datetime.now(timezone.utc)
    
    if asset.status in [VideoStatus.CREATED, VideoStatus.PROBING]:
        fail_asset(db, asset, "E_JOB_RECONCILED", reason)
    
    db.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reconcile jobs and staging.")
    parser.add_argument("--clean-staging", action="store_true", help="Perform actual deletion in staging")
    args = parser.parse_args()
    
    reconcile_jobs()
    reconcile_staging(dry_run=not args.clean_staging)
    reconcile_published_assets()
