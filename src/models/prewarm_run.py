import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, DateTime, Text, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from src.core.database import Base

class PrewarmRun(Base):
    __tablename__ = "prewarm_runs"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ranking_snapshot_id = Column(
        PGUUID(as_uuid=True),
        ForeignKey("ranking_snapshots.id", ondelete="SET NULL"),
        nullable=True,
        index=True
    )
    started_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    target_top_n = Column(Integer, nullable=False)
    enqueue_limit = Column(Integer, nullable=False)
    batch_queue_depth_before = Column(Integer, default=0, nullable=False)
    batch_queue_depth_after = Column(Integer, default=0, nullable=False)
    examined = Column(Integer, default=0, nullable=False)
    matched = Column(Integer, default=0, nullable=False)
    already_ready = Column(Integer, default=0, nullable=False)
    active = Column(Integer, default=0, nullable=False)
    cold_candidates = Column(Integer, default=0, nullable=False)
    failed_skipped = Column(Integer, default=0, nullable=False)
    missing_catalog = Column(Integer, default=0, nullable=False)
    enqueued = Column(Integer, default=0, nullable=False)
    status = Column(String(32), default="RUNNING", nullable=False)
    error = Column(Text, nullable=True)

    snapshot = relationship("RankingSnapshot", back_populates="prewarm_runs")
