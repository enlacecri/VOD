import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Float, DateTime, Enum, JSON, BigInteger, UniqueConstraint, CheckConstraint, Boolean
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from src.core.database import Base
from src.models.enums import VideoStatus

class Asset(Base):
    __tablename__ = "assets"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    vod_uuid = Column(PGUUID(as_uuid=True), unique=True, nullable=False, index=True)
    
    # Única restricción única para enlace_id
    enlace_id = Column(String(128), unique=True, nullable=False, index=True)
    
    source_uri = Column(String, nullable=False)
    source_sha256 = Column(String(64), nullable=True)
    size = Column(BigInteger, nullable=True)
    
    status = Column(Enum(VideoStatus), default=VideoStatus.CREATED, nullable=False)
    probe_metadata = Column(JSON, nullable=True)
    progress = Column(Integer, default=0)
    
    manifest_path = Column(String, nullable=True)
    manifest_url = Column(String, nullable=True)
    staged_source_path = Column(String, nullable=True)
    processed_source_path = Column(String, nullable=True)
    
    error_code = Column(String, nullable=True)
    error_message = Column(String, nullable=True)
    
    duration_seconds = Column(Float, nullable=True)
    source_width = Column(Integer, nullable=True)
    source_height = Column(Integer, nullable=True)
    source_fps = Column(Float, nullable=True)
    video_codec = Column(String, nullable=True)
    audio_codec = Column(String, nullable=True)
    has_audio = Column(Boolean, nullable=True)
    
    published_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)
    
    jobs = relationship("Job", back_populates="asset", cascade="all, delete-orphan")
    renditions = relationship("Rendition", back_populates="asset", cascade="all, delete-orphan")
    events = relationship("AssetEvent", back_populates="asset", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint('progress >= 0 AND progress <= 100', name='chk_asset_progress'),
        CheckConstraint('size >= 0', name='chk_asset_size'),
        CheckConstraint('duration_seconds >= 0', name='chk_asset_duration'),
        CheckConstraint('source_width >= 0', name='chk_asset_width'),
        CheckConstraint('source_height >= 0', name='chk_asset_height'),
    )
