import sys
import uuid
import argparse
import logging
from sqlalchemy.orm import Session
from redis import Redis

from src.core.database import SessionLocal
from src.core.queues import get_redis_connection, get_queue
from src.models.asset import Asset
from src.models.workflow_step import AssetWorkflowStep
from src.models.enums import WorkflowStepType, WorkflowStepStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def retry_workflow_step(
    asset_identifier: str,
    step_type_str: str,
    scope_key: str = "default",
    force: bool = False,
    db: Session = None,
    redis_conn: Redis = None,
) -> dict:
    """
    Administratively retries an individual workflow step.
    By default, only steps in FAILED status can be retried.
    Increments attempt_count, clears last_error, sets QUEUED, and enqueues to its dedicated queue.
    """
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        # Resolve Asset
        try:
            val_uuid = uuid.UUID(str(asset_identifier))
            asset = db.query(Asset).filter(Asset.vod_uuid == val_uuid).first()
        except (ValueError, AttributeError):
            asset = db.query(Asset).filter(Asset.enlace_id == str(asset_identifier)).first()

        if not asset:
            return {"status": "ERROR", "message": f"Asset '{asset_identifier}' not found."}

        # Validate step type
        try:
            step_type_enum = WorkflowStepType(step_type_str.upper())
        except ValueError:
            valid_types = [t.value for t in WorkflowStepType]
            return {
                "status": "ERROR",
                "message": f"Invalid step type '{step_type_str}'. Valid types: {valid_types}",
            }

        # Fetch workflow step with lock
        step = db.query(AssetWorkflowStep).filter(
            AssetWorkflowStep.asset_id == asset.id,
            AssetWorkflowStep.step_type == step_type_enum,
            AssetWorkflowStep.scope_key == scope_key,
        ).with_for_update().first()

        if not step:
            return {
                "status": "ERROR",
                "message": f"Workflow step '{step_type_str}' for asset '{asset.vod_uuid}' does not exist.",
            }

        if step.status == WorkflowStepStatus.COMPLETED and not force:
            return {
                "status": "REJECTED",
                "message": f"Workflow step '{step_type_str}' is already COMPLETED. Use --force to override.",
            }

        if step.status in (WorkflowStepStatus.PENDING, WorkflowStepStatus.QUEUED, WorkflowStepStatus.PROCESSING) and not force:
            return {
                "status": "REJECTED",
                "message": f"Workflow step '{step_type_str}' is currently in progress ({step.status.value}). Use --force to override.",
            }

        # Apply retry state
        step.status = WorkflowStepStatus.QUEUED
        step.last_error = None
        step.started_at = None
        step.completed_at = None
        step.attempt_count = (step.attempt_count or 0) + 1
        db.commit()

        # Enqueue to Redis
        conn = redis_conn or get_redis_connection()
        q = get_queue(step.queue_name, connection=conn)
        rq_job = q.enqueue(
            "src.worker.workflow_tasks.execute_workflow_step_job",
            args=(step.id,),
            job_id=str(step.id),
            job_timeout=3600,
            result_ttl=86400,
        )
        step.rq_job_id = rq_job.id
        db.commit()

        logger.info(
            f"Successfully retried step '{step_type_str}' for asset {asset.vod_uuid} in queue {step.queue_name}."
        )
        return {
            "status": "RETRIED",
            "step_id": str(step.id),
            "step_type": step.step_type.value,
            "queue_name": step.queue_name,
            "attempt_count": step.attempt_count,
            "rq_job_id": rq_job.id,
            "vod_uuid": str(asset.vod_uuid),
            "enlace_id": asset.enlace_id,
        }

    finally:
        if close_db:
            db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Administrative retry for a workflow step.")
    parser.add_argument("asset", help="Asset VOD_UUID or ENLACE_ID")
    parser.add_argument("step_type", help="Workflow step type (AZURE_BACKUP, SUBTITLES, ENLACE_SYNC)")
    parser.add_argument("--scope", default="default", help="Scope key (default: 'default')")
    parser.add_argument("--force", action="store_true", help="Force retry even if not FAILED")
    args = parser.parse_args()

    result = retry_workflow_step(
        asset_identifier=args.asset,
        step_type_str=args.step_type,
        scope_key=args.scope,
        force=args.force,
    )
    print(result)
    if result.get("status") in ("ERROR", "REJECTED"):
        sys.exit(1)
