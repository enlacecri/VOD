from sqlalchemy.orm import Session
from src.models.asset import Asset
from src.models.asset_event import AssetEvent
from src.models.enums import EventType, VideoStatus

def transition_asset(db: Session, asset: Asset, new_status: VideoStatus, details: dict = None):
    """
    Transitions the asset status and creates an audit event.
    """
    asset.status = new_status
    
    event = AssetEvent(
        asset_id=asset.id,
        event_type=EventType.TRANSITION,
        details={
            "new_status": new_status.value,
            **(details or {})
        }
    )
    db.add(event)
    
def fail_asset(db: Session, asset: Asset, error_code: str, error_message: str):
    """
    Transitions the asset to FAILED, sets error info, and creates an audit event.
    """
    asset.status = VideoStatus.FAILED
    asset.error_code = error_code
    asset.error_message = error_message
    
    event = AssetEvent(
        asset_id=asset.id,
        event_type=EventType.ERROR,
        details={
            "error_code": error_code,
            "error_message": error_message
        }
    )
    db.add(event)
