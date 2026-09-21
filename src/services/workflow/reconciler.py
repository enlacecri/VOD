import logging
from typing import Optional
from sqlalchemy.orm import Session
from redis import Redis, exceptions as redis_exceptions
from rq import Queue
from rq.job import Job as RQJob
from rq.exceptions import NoSuchJobError

from src.core.config import settings
from src.core.queues import (
    ALL_QUEUES,
    get_redis_connection,
    get_queue,
)
from src.models.asset import Asset
from src.models.ingest_item import IngestItem
from src.models.workflow_step import AssetWorkflowStep
from src.models.enums import VideoStatus, WorkflowStepStatus
from src.services.workflow.orchestrator import schedule_post_ready_work

logger = logging.getLogger(__name__)

def reconcile_workflow_steps(
    db: Optional[Session] = None,
    redis_conn: Optional[Redis] = None,
) -> int:
    """
    Idempotent and race-safe reconciler for post-READY workflow steps.
    Guarantees:
      1. Missing Steps Recovery: Any READY asset from new video pipeline that crashed
         before post-ready work was scheduled will have its missing steps created.
      2. Redis Failure Recovery: Any step PENDING/QUEUED in DB whose RQ job is missing
         from Redis will be safely re-enqueued to step.queue_name without duplication.
      3. Repeated executions maintain exactly 1 executable job per step.
    """
    logger.info("Starting workflow steps reconciliation...")
    close_db_on_exit = False
    if db is None:
        from src.core.database import SessionLocal
        db = SessionLocal()
        close_db_on_exit = True

    conn = redis_conn or get_redis_connection()
    try:
        conn.ping()
    except redis_exceptions.RedisError as e:
        logger.error(f"Redis unavailable, aborting workflow reconciliation: {e}")
        if close_db_on_exit:
            db.close()
        return 0

    reconciled_count = 0
    try:
        # 1. Recover missing post-READY steps for new pipeline assets
        ready_new_assets = db.query(Asset).join(
            IngestItem, IngestItem.asset_id == Asset.id
        ).filter(
            Asset.status == VideoStatus.READY
        ).all()

        for asset in ready_new_assets:
            # Check if all 3 expected steps exist
            step_count = db.query(AssetWorkflowStep).filter(
                AssetWorkflowStep.asset_id == asset.id
            ).count()
            if step_count < 3:
                logger.info(f"Asset {asset.vod_uuid} is READY but has only {step_count}/3 workflow steps. Scheduling missing steps...")
                schedule_post_ready_work(asset, db, redis_conn=conn)
                reconciled_count += 1

        # 2. Check active (PENDING, QUEUED) workflow steps in PostgreSQL
        active_steps = db.query(AssetWorkflowStep).filter(
            AssetWorkflowStep.status.in_([WorkflowStepStatus.PENDING, WorkflowStepStatus.QUEUED])
        ).all()

        for step in active_steps:
            target_queue = step.queue_name
            q = get_queue(target_queue, connection=conn)

            target_id = step.rq_job_id or str(step.id)
            candidate_ids = {target_id, str(step.id)}

            # Check all queues and started registries for flight
            in_flight = False
            for qname in ALL_QUEUES:
                chk_q = Queue(name=qname, connection=conn)
                chk_ids = set(chk_q.get_job_ids()) | set(chk_q.started_job_registry.get_job_ids())
                if candidate_ids & chk_ids:
                    in_flight = True
                    break

            if in_flight:
                continue

            # Check if job exists in Redis
            try:
                rq_j = RQJob.fetch(str(target_id), connection=conn)
                rq_status = rq_j.get_status()
                if rq_status in ("queued", "deferred", "scheduled", "started"):
                    continue
            except NoSuchJobError:
                pass

            # Not in flight and missing in Redis! Safely re-enqueue
            logger.warning(f"Workflow step {step.id} ({step.step_type.value}) missing from Redis. Re-enqueueing to {target_queue}...")
            try:
                new_rq = q.enqueue(
                    "src.worker.workflow_tasks.execute_workflow_step_job",
                    args=(step.id,),
                    job_id=str(step.id),
                    job_timeout=3600,
                    result_ttl=86400,
                )
                step.rq_job_id = new_rq.id
                step.status = WorkflowStepStatus.QUEUED
                db.commit()
                reconciled_count += 1
            except Exception as enq_err:
                logger.error(f"Failed to re-enqueue workflow step {step.id}: {enq_err}")

    finally:
        if close_db_on_exit:
            db.close()

    logger.info(f"Workflow reconciliation complete. (Reconciled actions: {reconciled_count})")
    return reconciled_count
