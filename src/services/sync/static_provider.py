from datetime import datetime, timezone
from typing import List, Dict, Any
from src.models.asset import Asset
from src.services.sync.base import EnlaceSyncProvider, SyncResult

class SyncError(Exception):
    pass

class StaticEnlaceSyncProvider(EnlaceSyncProvider):
    """
    In-memory sync provider for testing.
    Records synchronized payloads and allows simulating failures.
    """
    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.synced_history: List[Dict[str, Any]] = []

    def sync_ready_asset(self, asset: Asset) -> SyncResult:
        if self.should_fail:
            raise SyncError("Simulated Enlace+ sync failure for testing.")

        payload = {
            "enlace_id": asset.enlace_id,
            "vod_uuid": str(asset.vod_uuid),
            "status": asset.status.value if hasattr(asset.status, "value") else str(asset.status),
            "manifest_url": asset.manifest_url,
            "duration_seconds": asset.duration_seconds,
            "width": asset.source_width,
            "height": asset.source_height,
            "published_at": asset.published_at.isoformat() if asset.published_at else None,
        }
        result = SyncResult(
            enlace_id=asset.enlace_id,
            vod_uuid=str(asset.vod_uuid),
            status="SYNCED",
            synced_at=datetime.now(timezone.utc),
            payload=payload,
        )
        self.synced_history.append(result.to_dict())
        return result

    def clear(self):
        self.synced_history.clear()
