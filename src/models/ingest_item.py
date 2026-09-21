import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, BigInteger, Float, DateTime, Enum, JSON, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from src.core.database import Base
from src.models.enums import IngestStatus

class IngestItem(Base):
    __tablename__ = "ingest_items"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    relative_path = Column(String(512), nullable=False, index=True)
    filename = Column(String(255), nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
    mtime = Column(Float, nullable=False)
    
    # Lightweight fingerprint: sha256(f"{relative_path}:{size_bytes}:{mtime}")
    # Decouples identity from purely relative_path to allow future replacements safely
    source_fingerprint = Column(String(64), nullable=False, index=True)
    
    status = Column(Enum(IngestStatus), default=IngestStatus.WAITING_STABLE, nullable=False, index=True)
    asset_id = Column(PGUUID(as_uuid=True), ForeignKey("assets.id", ondelete="SET NULL"), nullable=True, index=True)
    metadata_snapshot = Column(JSON, nullable=True)
    
    first_observed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    last_observed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    stable_at = Column(DateTime(timezone=True), nullable=True)
    registered_at = Column(DateTime(timezone=True), nullable=True)
    
    last_error = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    asset = relationship("Asset", backref="ingest_items")
