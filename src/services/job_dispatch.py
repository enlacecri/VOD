import uuid
import logging
from typing import Optional, Union, Any
from datetime import datetime, timezone

from redis import Redis, exceptions as redis_exceptions
from rq import Queue
from rq.job import Job as RQJob, JobStatus as RQJobStatus
from rq.exceptions import NoSuchJobError
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.core.config import settings
from src.core.queues import (
    QUEUE_LEGACY,
    QUEUE_PRIORITY,
    QUEUE_INGEST,
    QUEUE_BATCH,
    get_redis_connection,
    get_queue,
)
from src.core.canonical import (
    build_canonical_manifest_path,
    build_canonical_manifest_url,
)
from src.models.asset import Asset
from src.models.job import Job
from src.models.asset_event import AssetEvent
from src.models.enums import VideoStatus, JobType, JobStatus, EventType

logger = logging.getLogger(__name__)

class DispatchError(Exception):
    """Base exception for job dispatch errors."""
    pass

class QueueUnavailableError(DispatchError):
    """Raised when Redis queue is unreachable."""
    pass

def dispatch_progressive_job(
    asset: Asset,
    queue_name: str,
    db: Session,
    context: str = "prepare-playback",
    redis_conn: Optional[Redis] = None
) -> Job:
    """
    Central dispatch authority for progressive transcoding jobs.
    Order of operations:
      1. Prepare canonical manifest attributes if missing.
      2. Set asset status = QUEUED.
      3. Create and commit Job record in PostgreSQL (PENDING, queue_name).
      4. Enqueue to Redis queue.
      5. Commit rq_job_id to PostgreSQL.
    If Redis fails after step 3, Job is marked FAILED or left for reconciler,
    ensuring workers never read uncommitted database state.
    """
    if queue_name not in (QUEUE_PRIORITY, QUEUE_INGEST, QUEUE_BATCH, QUEUE_LEGACY):
        raise ValueError(f"Invalid queue name: {queue_name}")

    if asset.status == VideoStatus.FAILED:
        raise DispatchError(
            f"Asset {asset.vod_uuid} is in FAILED state. Requires explicit operator retry."
        )

    if not asset.manifest_url or not asset.manifest_path:
        asset.manifest_url = build_canonical_manifest_url(asset.vod_uuid, asset.enlace_id)
        asset.manifest_path = build_canonical_manifest_path(asset.vod_uuid, asset.enlace_id)

    asset.status = VideoStatus.QUEUED
    asset.error_code = None
    asset.error_message = None

    new_job = Job(
        asset_id=asset.id,
        type=JobType.TRANSCODE,
        status=JobStatus.PENDING,
        queue_name=queue_name,
        attempt=1,
        max_attempts=settings.MAX_TRANSCODE_ATTEMPTS
    )
    db.add(new_job)
    db.add(AssetEvent(
        asset_id=asset.id,
        event_type=EventType.TRANSITION,
        details={
            "new_status": "QUEUED",
            "job_id": str(new_job.id),
            "queue_name": queue_name,
            "context": context
        }
    ))
    # Commit first so worker can always find the Job in PostgreSQL
    db.commit()
    db.refresh(new_job)

    conn = redis_conn or get_redis_connection()
    try:
        q = get_queue(queue_name, connection=conn)
        existing_rq_job = q.fetch_job(str(new_job.id))
        if existing_rq_job:
            rq_job = existing_rq_job
        else:
            rq_job = q.enqueue(
                "src.worker.tasks.progressive_transcode_asset_job",
                args=(new_job.id,),
                job_id=str(new_job.id),
                job_timeout=settings.FFMPEG_TIMEOUT_SECONDS + 300,
                result_ttl=86400
            )
        new_job.rq_job_id = rq_job.id
        db.commit()
        return new_job
    except Exception as e:
        logger.warning(
            f"Redis unavailable while enqueuing job {new_job.id} to {queue_name}: {e}. "
            f"Job remains PENDING in PostgreSQL for reconciler recovery."
        )
        if not new_job.rq_job_id:
            new_job.rq_job_id = str(new_job.id)
            db.commit()
        raise QueueUnavailableError(f"Failed to enqueue job to Redis: {e}")

def promote_pending_job_to_priority(
    active_job: Job,
    asset: Asset,
    db: Session,
    redis_conn: Optional[Redis] = None
) -> dict:
    """
    Safely promotes a PENDING transcode job (from vod_batch or vod_ingest) to vod_priority.
    Evaluates the real RQ state to protect against concurrency races:
      - If already PROCESSING in DB or STARTED in RQ: do NOT promote, reuse processing.
      - If QUEUED in source queue: remove from source queue and promote to priority.
      - If removal returns 0: re-check status (worker may have just started).
    Maintains the exact same PostgreSQL Job record (active_job.id).
    """
    source_queue = active_job.queue_name or QUEUE_BATCH

    # 1. Check DB state
    if active_job.status == JobStatus.PROCESSING:
        return {
            "action": "REUSE_PROCESSING",
            "job": active_job,
            "detail": f"Job in {source_queue} is already PROCESSING, reusing ongoing transcode."
        }

    if active_job.status != JobStatus.PENDING:
        return {
            "action": "NOOP",
            "job": active_job,
            "detail": f"Job status is {active_job.status.value}, cannot promote."
        }

    if active_job.queue_name == QUEUE_PRIORITY:
        return {
            "action": "ALREADY_PRIORITY",
            "job": active_job,
            "detail": "Job is already assigned to vod_priority."
        }

    conn = redis_conn or get_redis_connection()
    target_rq_id = active_job.rq_job_id or str(active_job.id)

    # 2. Inspect real RQ Job state
    try:
        rq_job = RQJob.fetch(target_rq_id, connection=conn)
    except NoSuchJobError:
        # Check if the job might already be running in StartedJobRegistry of any queue
        q_src = get_queue(source_queue, connection=conn)
        q_prio = get_queue(QUEUE_PRIORITY, connection=conn)
        in_started = (
            target_rq_id in q_src.started_job_registry.get_job_ids() or
            target_rq_id in q_prio.started_job_registry.get_job_ids() or
            str(active_job.id) in q_src.started_job_registry.get_job_ids() or
            str(active_job.id) in q_prio.started_job_registry.get_job_ids()
        )
        if in_started:
            logger.info(f"Promotion: Job {active_job.id} found in started registry. Reusing ongoing execution.")
            active_job.status = JobStatus.PROCESSING
            db.commit()
            return {
                "action": "REUSE_STARTED",
                "job": active_job,
                "detail": "Worker already started execution, reusing ongoing transcode."
            }

        logger.warning(f"Promotion: RQ job {target_rq_id} not found in Redis. Safely re-enqueueing to {QUEUE_PRIORITY}.")
        # Re-enqueue in priority queue
        active_job.queue_name = QUEUE_PRIORITY
        db.commit()
        try:
            new_rq = q_prio.enqueue(
                "src.worker.tasks.progressive_transcode_asset_job",
                args=(active_job.id,),
                job_id=str(active_job.id),
                job_timeout=settings.FFMPEG_TIMEOUT_SECONDS + 300,
                result_ttl=86400
            )
            active_job.rq_job_id = new_rq.id
            db.commit()
            return {
                "action": "PROMOTED_RECREATED",
                "job": active_job,
                "detail": "Missing RQ job recreated safely in vod_priority."
            }
        except Exception as e:
            logger.error(f"Failed to re-enqueue missing job to priority: {e}")
            raise QueueUnavailableError(str(e))

    rq_status = rq_job.get_status()

    # Case C: Worker already started executing the job
    if rq_status == RQJobStatus.STARTED or rq_status == "started":
        logger.info(f"Promotion: Job {active_job.id} already STARTED by worker on {source_queue}. Reusing.")
        active_job.status = JobStatus.PROCESSING
        db.commit()
        return {
            "action": "REUSE_STARTED",
            "job": active_job,
            "detail": f"Worker on {source_queue} already started execution, reusing ongoing transcode."
        }

    # Case D: Already finished
    if rq_status == RQJobStatus.FINISHED or rq_status == "finished":
        return {
            "action": "FINISHED",
            "job": active_job,
            "detail": "Job already finished."
        }

    # Case E: Failed / Canceled / Stopped
    if rq_status in (RQJobStatus.FAILED, RQJobStatus.CANCELED, RQJobStatus.STOPPED, "failed", "canceled", "stopped"):
        return {
            "action": "FAILED_NOOP",
            "job": active_job,
            "detail": f"RQ job is {rq_status}, cannot promote."
        }

    # Case A: RQ job is QUEUED in source queue
    q_src = get_queue(source_queue, connection=conn)
    removed_count = q_src.remove(rq_job.id)

    if removed_count == 1:
        # Successfully removed from source queue before worker could pop it
        q_prio = get_queue(QUEUE_PRIORITY, connection=conn)
        rq_job.origin = QUEUE_PRIORITY
        rq_job.save()
        q_prio.enqueue_job(rq_job)

        old_queue = active_job.queue_name
        active_job.queue_name = QUEUE_PRIORITY
        context_str = "batch_to_priority_promotion" if old_queue == QUEUE_BATCH else f"{old_queue}_to_priority_promotion"
        db.add(AssetEvent(
            asset_id=asset.id,
            event_type=EventType.TRANSITION,
            details={
                "job_id": str(active_job.id),
                "old_queue": old_queue,
                "new_queue": QUEUE_PRIORITY,
                "context": context_str
            }
        ))
        db.commit()
        db.refresh(active_job)
        logger.info(f"Job {active_job.id} successfully PROMOTED from {old_queue} to {QUEUE_PRIORITY}.")
        return {
            "action": "PROMOTED",
            "job": active_job,
            "detail": "Job successfully promoted to vod_priority."
        }
    else:
        # Case B: remove returned 0! Check if worker popped it just now
        try:
            rq_job.refresh()
            new_status = rq_job.get_status()
        except Exception:
            new_status = "unknown"

        if new_status in (RQJobStatus.STARTED, "started"):
            logger.info(f"Promotion race: worker started job {active_job.id} during remove. Reusing.")
            active_job.status = JobStatus.PROCESSING
            db.commit()
            return {
                "action": "REUSE_STARTED",
                "job": active_job,
                "detail": f"Worker on {source_queue} popped job during promotion attempt, reusing."
            }
        else:
            logger.warning(f"Promotion: could not remove from {source_queue} (status: {new_status}).")
            return {
                "action": "REMOVE_FAILED",
                "job": active_job,
                "detail": f"Could not remove from {source_queue} (status: {new_status})."
            }

promote_batch_to_priority = promote_pending_job_to_priority

def batch_enqueue_asset(
    asset_identifier: Union[str, uuid.UUID],
    db: Session,
    redis_conn: Optional[Redis] = None
) -> dict:
    """
    Idempotent batch enqueue operation for a video asset.
    Allowed status:
      - COLD: enqueued into vod_batch
      - QUEUED: reported as already queued (distinguishes priority vs batch)
      - PROCESSING: reported as already processing
      - PLAYABLE / VALIDATING: reported as already playable
      - READY: reported as ALREADY_READY
      - FAILED: reported as REQUIRES_EXPLICIT_RETRY
    """
    query = db.query(Asset)
    try:
        val_uuid = uuid.UUID(str(asset_identifier))
        asset = query.filter(Asset.vod_uuid == val_uuid).with_for_update().first()
    except (ValueError, AttributeError):
        asset = query.filter(Asset.enlace_id == str(asset_identifier)).with_for_update().first()

    if not asset:
        return {
            "status": "NOT_FOUND",
            "asset_identifier": str(asset_identifier),
            "detail": "Asset not found in database."
        }

    if asset.status == VideoStatus.READY:
        return {
            "status": "ALREADY_READY",
            "vod_uuid": str(asset.vod_uuid),
            "enlace_id": asset.enlace_id,
            "asset_status": asset.status.value,
            "detail": "Asset is already published and READY."
        }

    if asset.status in (VideoStatus.PLAYABLE, VideoStatus.VALIDATING):
        return {
            "status": "ALREADY_PLAYABLE",
            "vod_uuid": str(asset.vod_uuid),
            "enlace_id": asset.enlace_id,
            "asset_status": asset.status.value,
            "detail": "Asset is already playable."
        }

    if asset.status in (VideoStatus.QUEUED, VideoStatus.PROCESSING):
        active_job = db.query(Job).filter(
            Job.asset_id == asset.id,
            Job.status.in_([JobStatus.PENDING, JobStatus.PROCESSING])
        ).first()

        if active_job:
            queue_name = active_job.queue_name or QUEUE_LEGACY
            status_label = f"ALREADY_{active_job.status.value.upper()}_{queue_name.upper()}"
            return {
                "status": status_label,
                "vod_uuid": str(asset.vod_uuid),
                "enlace_id": asset.enlace_id,
                "asset_status": asset.status.value,
                "job_id": str(active_job.id),
                "rq_job_id": active_job.rq_job_id,
                "queue_name": queue_name,
                "detail": f"Asset already has active transcode job in {queue_name}."
            }

    if asset.status == VideoStatus.FAILED:
        return {
            "status": "REQUIRES_EXPLICIT_RETRY",
            "vod_uuid": str(asset.vod_uuid),
            "enlace_id": asset.enlace_id,
            "asset_status": asset.status.value,
            "detail": "Asset is in FAILED state. Requires explicit operator retry."
        }

    if asset.status == VideoStatus.COLD or asset.status == VideoStatus.CREATED:
        try:
            job = dispatch_progressive_job(
                asset=asset,
                queue_name=QUEUE_BATCH,
                db=db,
                context="batch-enqueue",
                redis_conn=redis_conn
            )
            return {
                "status": "ENQUEUED_BATCH",
                "vod_uuid": str(asset.vod_uuid),
                "enlace_id": asset.enlace_id,
                "asset_status": asset.status.value,
                "job_id": str(job.id),
                "rq_job_id": job.rq_job_id,
                "queue_name": QUEUE_BATCH,
                "detail": "Asset successfully enqueued to vod_batch."
            }
        except QueueUnavailableError as e:
            # Job was committed in PostgreSQL as PENDING and will be enqueued by reconciler
            job = db.query(Job).filter(Job.asset_id == asset.id).order_by(Job.created_at.desc()).first()
            return {
                "status": "ENQUEUED_BATCH_REDIS_OFFLINE",
                "vod_uuid": str(asset.vod_uuid),
                "enlace_id": asset.enlace_id,
                "asset_status": asset.status.value,
                "job_id": str(job.id) if job else None,
                "rq_job_id": job.rq_job_id if job else None,
                "queue_name": QUEUE_BATCH,
                "detail": "Job persisted as PENDING in PostgreSQL, but Redis was unavailable. Reconciler will enqueue."
            }

    return {
        "status": f"UNSUPPORTED_STATUS_{asset.status.value}",
        "vod_uuid": str(asset.vod_uuid),
        "enlace_id": asset.enlace_id,
        "asset_status": asset.status.value,
        "detail": f"Cannot batch enqueue asset in status {asset.status.value}."
    }
