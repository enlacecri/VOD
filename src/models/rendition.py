import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Float, DateTime, ForeignKey, UniqueConstraint, CheckConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from src.core.database import Base

class Rendition(Base):
    __tablename__ = "renditions"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_id = Column(PGUUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)
    
    name = Column(String, nullable=False) # e.g., "0", "1", "2"
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    
    video_bitrate = Column(Integer, nullable=True)
    audio_bitrate = Column(Integer, nullable=True)
    
    playlist_path = Column(String, nullable=False)
    segment_count = Column(Integer, default=0)
    duration_seconds = Column(Float, nullable=True)
    
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    
    asset = relationship("Asset", back_populates="renditions")

    __table_args__ = (
        UniqueConstraint('asset_id', 'name', name='uq_rendition_asset_name'),
        CheckConstraint('width >= 0', name='chk_rendition_width'),
        CheckConstraint('height >= 0', name='chk_rendition_height'),
        CheckConstraint('duration_seconds >= 0', name='chk_rendition_duration'),
        CheckConstraint('video_bitrate >= 0', name='chk_rendition_vbr'),
        CheckConstraint('audio_bitrate >= 0', name='chk_rendition_abr'),
    )
