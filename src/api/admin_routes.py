from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from redis import Redis
from rq import Queue, Worker
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from src.core.admin_auth import require_admin_api_key
from src.core.config import settings
from src.core.database import get_db
from src.models.asset import Asset
from src.models.enums import JobStatus, VideoStatus, JobType
from src.models.job import Job


router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(require_admin_api_key)],
)


def _enum_counts(db: Session, model, column) -> dict[str, int]:
    rows = db.query(column, func.count(model.id)).group_by(column).all()
    return {status.value: count for status, count in rows}


def _calculate_metrics(asset: Asset) -> dict:
    if not asset.jobs:
        return {
            "queue_wait_seconds": None,
            "probe_processing_seconds": None,
            "transcode_processing_seconds": None,
            "total_processing_seconds": None,
            "elapsed_wall_seconds": None,
        }

    first_job = min(asset.jobs, key=lambda j: j.created_at)

    probe_jobs = [j for j in asset.jobs if j.type == JobType.PROBE and j.status == JobStatus.COMPLETED]
    transcode_jobs = [j for j in asset.jobs if j.type == JobType.TRANSCODE and j.status == JobStatus.COMPLETED]

    latest_probe = max(probe_jobs, key=lambda j: j.attempt) if probe_jobs else None
    latest_transcode = max(transcode_jobs, key=lambda j: j.attempt) if transcode_jobs else None

    queue_wait = 0.0
    valid_wait = False

    probe_processing = None
    if latest_probe and latest_probe.started_at:
        valid_wait = True
        queue_wait += (latest_probe.started_at - latest_probe.created_at).total_seconds()
        if latest_probe.finished_at:
            probe_processing = (latest_probe.finished_at - latest_probe.started_at).total_seconds()

    transcode_processing = None
    if latest_transcode and latest_transcode.started_at:
        valid_wait = True
        queue_wait += (latest_transcode.started_at - latest_transcode.created_at).total_seconds()
        if latest_transcode.finished_at:
            transcode_processing = (latest_transcode.finished_at - latest_transcode.started_at).total_seconds()

    total_processing = None
    if probe_processing is not None or transcode_processing is not None:
        total_processing = (probe_processing or 0.0) + (transcode_processing or 0.0)

    elapsed_wall = None
    end_time = asset.published_at
    if not end_time and latest_transcode and latest_transcode.finished_at:
        end_time = latest_transcode.finished_at

    if end_time:
        elapsed_wall = (end_time - first_job.created_at).total_seconds()

    return {
        "queue_wait_seconds": queue_wait if valid_wait else None,
        "probe_processing_seconds": probe_processing,
        "transcode_processing_seconds": transcode_processing,
        "total_processing_seconds": total_processing,
        "elapsed_wall_seconds": elapsed_wall,
    }


@router.get("/dashboard")
def dashboard(db: Session = Depends(get_db)):
    from src.core.queues import QUEUE_LEGACY, QUEUE_PRIORITY, QUEUE_BATCH
    redis_conn = Redis.from_url(settings.REDIS_URL)
    legacy_q = Queue(name=QUEUE_LEGACY, connection=redis_conn)
    priority_q = Queue(name=QUEUE_PRIORITY, connection=redis_conn)
    batch_q = Queue(name=QUEUE_BATCH, connection=redis_conn)

    workers = Worker.all(connection=redis_conn)
    stale_before = datetime.now(timezone.utc) - timedelta(
        seconds=settings.RQ_JOB_TIMEOUT_SECONDS * 2
    )
    stale_jobs = db.query(func.count(Job.id)).filter(
        Job.status == JobStatus.PROCESSING,
        func.coalesce(Job.heartbeat, Job.updated_at) < stale_before,
    ).scalar() or 0

    recent_failures = db.query(Asset).filter(
        Asset.status == VideoStatus.FAILED
    ).order_by(Asset.updated_at.desc()).limit(10).all()

    total_depth = legacy_q.count + priority_q.count + batch_q.count

    return {
        "generated_at": datetime.now(timezone.utc),
        "assets": _enum_counts(db, Asset, Asset.status),
        "jobs": _enum_counts(db, Job, Job.status),
        "queue": {
            "name": settings.RQ_QUEUE_NAME,
            "depth": legacy_q.count,
            "legacy_queue_depth": legacy_q.count,
            "priority_queue_depth": priority_q.count,
            "batch_queue_depth": batch_q.count,
            "total_queue_depth": total_depth,
            "workers": len(workers),
            "stale_jobs": stale_jobs,
        },
        "recent_failures": [
            {
                "vod_uuid": asset.vod_uuid,
                "enlace_id": asset.enlace_id,
                "error_code": asset.error_code,
                "error_message": asset.error_message,
                "updated_at": asset.updated_at,
            }
            for asset in recent_failures
        ],
    }


@router.get("/assets")
def list_assets(
    asset_status: VideoStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(Asset).options(
        joinedload(Asset.renditions),
        joinedload(Asset.jobs)
    )
    if asset_status is not None:
        query = query.filter(Asset.status == asset_status)
    total = query.count()
    assets = query.order_by(Asset.updated_at.desc()).offset(offset).limit(limit).all()
    return {
        "total": total,
        "items": [
            {
                "vod_uuid": asset.vod_uuid,
                "enlace_id": asset.enlace_id,
                "status": asset.status,
                "progress": asset.progress,
                "error_code": asset.error_code,
                "error_message": asset.error_message,
                "created_at": asset.created_at,
                "updated_at": asset.updated_at,
                "duration_seconds": asset.duration_seconds,
                "source_width": asset.source_width,
                "source_height": asset.source_height,
                "manifest_path": asset.manifest_path,
                "manifest_url": asset.manifest_url,
                "playback_url": f"{settings.HLS_PLAYBACK_BASE_URL.rstrip('/')}/{asset.manifest_path.lstrip('/')}" if asset.manifest_path else None,
                "processed_source_path": asset.processed_source_path,
                "video_codec": asset.video_codec,
                "audio_codec": asset.audio_codec,
                "published_at": asset.published_at,
                "variants": [
                    {
                        "name": r.name,
                        "width": r.width,
                        "height": r.height,
                        "video_bitrate": r.video_bitrate,
                        "audio_bitrate": r.audio_bitrate
                    }
                    for r in asset.renditions
                ],
                **_calculate_metrics(asset)
            }
            for asset in assets
        ],
    }


@router.get("/jobs")
def list_jobs(
    job_status: JobStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(Job)
    if job_status is not None:
        query = query.filter(Job.status == job_status)
    total = query.count()
    jobs = query.order_by(Job.updated_at.desc()).offset(offset).limit(limit).all()
    return {
        "total": total,
        "items": [
            {
                "id": job.id,
                "asset_id": job.asset_id,
                "type": job.type,
                "status": job.status,
                "attempt": job.attempt,
                "worker_id": job.worker_id,
                "queue_name": getattr(job, "queue_name", "vod_tasks"),
                "heartbeat": job.heartbeat,
                "error_code": job.error_code,
                "error_message": job.error_message,
                "updated_at": job.updated_at,
            }
            for job in jobs
        ],
    }
