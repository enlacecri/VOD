import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, DateTime, Enum, JSON, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from src.core.database import Base
from src.models.enums import WorkflowStepType, WorkflowStepStatus

class AssetWorkflowStep(Base):
    __tablename__ = "asset_workflow_steps"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_id = Column(PGUUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False, index=True)
    step_type = Column(Enum(WorkflowStepType), nullable=False)
    scope_key = Column(String(64), nullable=False, default="default")
    status = Column(Enum(WorkflowStepStatus), default=WorkflowStepStatus.PENDING, nullable=False, index=True)
    queue_name = Column(String(64), nullable=False)
    rq_job_id = Column(String(128), nullable=True)
    attempt_count = Column(Integer, default=0, nullable=False)
    last_error = Column(String, nullable=True)
    metadata_json = Column(JSON, nullable=True)
    
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    asset = relationship("Asset", backref="workflow_steps")

    __table_args__ = (
        UniqueConstraint("asset_id", "step_type", "scope_key", name="uq_asset_step_scope"),
    )
