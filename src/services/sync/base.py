from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from src.models.asset import Asset

@dataclass
class SyncResult:
    enlace_id: str
    vod_uuid: str
    status: str
    synced_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enlace_id": self.enlace_id,
            "vod_uuid": self.vod_uuid,
            "status": self.status,
            "synced_at": self.synced_at.isoformat(),
            "payload": self.payload,
        }

class EnlaceSyncProvider(ABC):
    @abstractmethod
    def sync_ready_asset(self, asset: Asset) -> SyncResult:
        """
        Synchronizes the processed and ready VOD asset with the Enlace+ database/service.
        """
        pass
