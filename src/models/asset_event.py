import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, DateTime, ForeignKey, Enum, JSON
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import relationship

from src.core.database import Base
from src.models.enums import EventType

class AssetEvent(Base):
    __tablename__ = "asset_events"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_id = Column(PGUUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)
    
    event_type = Column(Enum(EventType), nullable=False)
    details = Column(JSON, nullable=True)
    
    timestamp = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    
    asset = relationship("Asset", back_populates="events")
