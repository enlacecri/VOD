import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Text, Boolean, DateTime, JSON, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from src.core.database import Base

class AssetTranscript(Base):
    __tablename__ = "asset_transcripts"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_id = Column(PGUUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False, index=True)
    source_language = Column(String(16), nullable=False, default="es")
    transcript_text = Column(Text, nullable=False)
    segments_json = Column(JSON, nullable=True)
    provider = Column(String(64), nullable=True)
    metadata_json = Column(JSON, nullable=True)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    asset = relationship("Asset", backref="transcripts")
    subtitle_tracks = relationship("AssetSubtitleTrack", back_populates="transcript", cascade="all, delete-orphan")


class AssetSubtitleTrack(Base):
    __tablename__ = "asset_subtitle_tracks"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_id = Column(PGUUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False, index=True)
    transcript_id = Column(PGUUID(as_uuid=True), ForeignKey("asset_transcripts.id", ondelete="SET NULL"), nullable=True, index=True)
    language = Column(String(16), nullable=False)
    vtt_path = Column(String(512), nullable=False)
    is_master = Column(Boolean, default=False, nullable=False)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    asset = relationship("Asset", backref="subtitle_tracks")
    transcript = relationship("AssetTranscript", back_populates="subtitle_tracks")

    __table_args__ = (
        UniqueConstraint("asset_id", "language", name="uq_asset_subtitle_language"),
    )
