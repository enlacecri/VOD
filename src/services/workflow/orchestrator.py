import logging
from typing import Optional, List
from sqlalchemy.orm import Session
from redis import Redis

from src.core.config import settings
from src.core.queues import (
    QUEUE_BACKUP,
    QUEUE_SUBTITLES,
    QUEUE_SYNC,
    get_redis_connection,
    get_queue,
)
from src.models.asset import Asset
from src.models.ingest_item import IngestItem
from src.models.workflow_step import AssetWorkflowStep
from src.models.enums import WorkflowStepType, WorkflowStepStatus

logger = logging.getLogger(__name__)

WORKFLOW_SPECS = [
    (WorkflowStepType.AZURE_BACKUP, "default", QUEUE_BACKUP),
    (WorkflowStepType.SUBTITLES, "default", QUEUE_SUBTITLES),
    (WorkflowStepType.ENLACE_SYNC, "default", QUEUE_SYNC),
]

def schedule_post_ready_work(
    asset: Asset,
    db: Session,
    redis_conn: Optional[Redis] = None,
) -> List[AssetWorkflowStep]:
    """
    Idempotently schedules post-READY workflow tasks for an asset.
    Only triggers for assets originating from the new video pipeline (IngestItem present).
    Follows reliable DB -> Redis order:
      1. Commits AssetWorkflowStep records in DB (PENDING).
      2. Enqueues tasks to their respective Redis queues.
      3. If Redis fails, steps remain in DB for reconciler recovery.
    """
    # 1. Verify this asset is from the new video pipeline
    ingest_item = db.query(IngestItem).filter(IngestItem.asset_id == asset.id).first()
    if not ingest_item:
        logger.debug(f"Asset {asset.vod_uuid} is not from new video pipeline. Skipping post-ready work.")
        return []

    scheduled_steps: List[AssetWorkflowStep] = []
    new_steps: List[AssetWorkflowStep] = []

    for step_type, scope_key, queue_name in WORKFLOW_SPECS:
        existing_step = db.query(AssetWorkflowStep).filter(
            AssetWorkflowStep.asset_id == asset.id,
            AssetWorkflowStep.step_type == step_type,
            AssetWorkflowStep.scope_key == scope_key,
        ).first()

        if existing_step:
            scheduled_steps.append(existing_step)
            # If it's still PENDING or QUEUED but has no rq_job_id, we can attempt enqueue
            if existing_step.status in (WorkflowStepStatus.PENDING, WorkflowStepStatus.QUEUED):
                new_steps.append(existing_step)
        else:
            step = AssetWorkflowStep(
                asset_id=asset.id,
                step_type=step_type,
                scope_key=scope_key,
                status=WorkflowStepStatus.PENDING,
                queue_name=queue_name,
                attempt_count=0,
            )
            db.add(step)
            scheduled_steps.append(step)
            new_steps.append(step)

    # Commit first so workers / reconcilers find steps in PostgreSQL
    db.commit()
    for s in scheduled_steps:
        db.refresh(s)

    # Enqueue to Redis
    for step in new_steps:
        conn = redis_conn or get_redis_connection()
        try:
            q = get_queue(step.queue_name, connection=conn)
            existing_rq_job = q.fetch_job(str(step.id))
            if existing_rq_job:
                rq_id = existing_rq_job.id
            else:
                rq_job = q.enqueue(
                    "src.worker.workflow_tasks.execute_workflow_step_job",
                    args=(step.id,),
                    job_id=str(step.id),
                    job_timeout=3600,
                    result_ttl=86400,
                )
                rq_id = rq_job.id

            step.rq_job_id = rq_id
            step.status = WorkflowStepStatus.QUEUED
            db.commit()
        except Exception as e:
            logger.warning(
                f"Redis unavailable while enqueuing step {step.id} to {step.queue_name}: {e}. "
                f"Step remains {step.status.value} in DB for reconciler."
            )
            if not step.rq_job_id:
                step.rq_job_id = str(step.id)
                db.commit()

    return scheduled_steps
