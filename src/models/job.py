import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Enum, UniqueConstraint, Index, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from src.core.database import Base
from src.models.enums import JobType, JobStatus

class Job(Base):
    __tablename__ = "jobs"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_id = Column(PGUUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)
    
    type = Column(Enum(JobType), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING, nullable=False)
    queue_name = Column(String(64), nullable=False, default="vod_tasks", server_default="vod_tasks")
    
    rq_job_id = Column(String, nullable=True, index=True)
    worker_id = Column(String, nullable=True)
    
    attempt = Column(Integer, default=1, nullable=False)
    max_attempts = Column(Integer, default=3, nullable=False)
    
    heartbeat = Column(DateTime(timezone=True), nullable=True)
    
    log_path = Column(String, nullable=True)
    error_code = Column(String, nullable=True)
    error_message = Column(String, nullable=True)
    
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    asset = relationship("Asset", back_populates="jobs")

    __table_args__ = (
        # Postgres ignora los NULL en restricciones UNIQUE estándar
        UniqueConstraint('rq_job_id', name='uq_jobs_rq_job_id'),
        Index('idx_unique_active_job', 'asset_id', 'type', unique=True, postgresql_where=text("status IN ('PENDING', 'PROCESSING')")),
    )
