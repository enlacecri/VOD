import logging
from uuid import UUID
from datetime import datetime, timezone
from pathlib import Path

from src.core.database import SessionLocal
from src.core.config import settings
from src.models.asset import Asset
from src.models.workflow_step import AssetWorkflowStep
from src.models.enums import WorkflowStepType, WorkflowStepStatus
from src.services.backup.factory import get_backup_provider
from src.services.subtitles.service import SubtitleService
from src.services.sync.factory import get_sync_provider

logger = logging.getLogger(__name__)

def execute_workflow_step_job(step_id: UUID) -> None:
    """
    Worker task for executing an independent post-READY workflow step.
    Handles:
      - AZURE_BACKUP
      - SUBTITLES
      - ENLACE_SYNC
    Failure of any workflow step NEVER alters Asset.status (remains READY).
    """
    db = SessionLocal()
    try:
        step = db.query(AssetWorkflowStep).filter(AssetWorkflowStep.id == step_id).with_for_update().first()
        if not step:
            logger.error(f"Workflow step not found: {step_id}")
            return

        if step.status == WorkflowStepStatus.COMPLETED:
            logger.info(f"Workflow step {step_id} ({step.step_type.value}) is already COMPLETED. No-op.")
            return

        # Mark PROCESSING
        step.status = WorkflowStepStatus.PROCESSING
        step.started_at = datetime.now(timezone.utc)
        step.attempt_count = (step.attempt_count or 0) + 1
        db.commit()

        asset = db.query(Asset).filter(Asset.id == step.asset_id).first()
        if not asset:
            raise ValueError(f"Asset not found for workflow step: {step.asset_id}")

        result_metadata = {}

        if step.step_type == WorkflowStepType.AZURE_BACKUP:
            provider = get_backup_provider()
            cand_path = Path(settings.VOD_NEW_INGEST_ROOT).resolve() / (asset.source_uri or "")
            if not cand_path.exists():
                cand_path = Path(settings.INGEST_ROOT).resolve() / (asset.source_uri or "")
            if not cand_path.exists():
                cand_path = Path(asset.source_uri or "video.mp4")
            
            res = provider.backup_original(asset=asset, source_path=cand_path)
            result_metadata = res.to_dict()

        elif step.step_type == WorkflowStepType.SUBTITLES:
            service = SubtitleService()
            res = service.process_subtitles(asset=asset, db=db)
            result_metadata = res

        elif step.step_type == WorkflowStepType.ENLACE_SYNC:
            provider = get_sync_provider()
            res = provider.sync_ready_asset(asset=asset)
            result_metadata = res.to_dict()

        else:
            raise ValueError(f"Unsupported workflow step type: {step.step_type}")

        # Mark COMPLETED
        step.status = WorkflowStepStatus.COMPLETED
        step.completed_at = datetime.now(timezone.utc)
        step.last_error = None
        step.metadata_json = result_metadata
        db.commit()
        logger.info(f"Workflow step {step_id} ({step.step_type.value}) COMPLETED successfully.")

    except Exception as e:
        logger.exception(f"Error executing workflow step {step_id}: {e}")
        try:
            db.rollback()
            step = db.query(AssetWorkflowStep).filter(AssetWorkflowStep.id == step_id).first()
            if step:
                step.status = WorkflowStepStatus.FAILED
                step.last_error = str(e)[:settings.ERROR_MESSAGE_MAX_LENGTH]
                db.commit()
                logger.info(f"Marked workflow step {step_id} as FAILED. Asset remains READY.")
        except Exception as commit_err:
            logger.error(f"Failed to record FAILED status for workflow step {step_id}: {commit_err}")
    finally:
        db.close()
