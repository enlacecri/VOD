import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Float, DateTime, JSON, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from src.core.database import Base

class RankingSnapshot(Base):
    __tablename__ = "ranking_snapshots"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source = Column(String(64), nullable=False)
    period_start = Column(DateTime(timezone=True), nullable=True)
    period_end = Column(DateTime(timezone=True), nullable=True)
    generated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    item_count = Column(Integer, default=0, nullable=False)
    extra_metadata = Column(JSON, nullable=True)

    items = relationship("RankingSnapshotItem", back_populates="snapshot", cascade="all, delete-orphan")
    prewarm_runs = relationship("PrewarmRun", back_populates="snapshot")

class RankingSnapshotItem(Base):
    __tablename__ = "ranking_snapshot_items"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    snapshot_id = Column(PGUUID(as_uuid=True), ForeignKey("ranking_snapshots.id", ondelete="CASCADE"), nullable=False, index=True)
    enlace_id = Column(String(128), nullable=False, index=True)
    rank = Column(Integer, nullable=False)
    score = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("snapshot_id", "enlace_id", name="uq_snapshot_enlace_id"),
    )

    snapshot = relationship("RankingSnapshot", back_populates="items")
