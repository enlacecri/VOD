from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

from src.models.asset import Asset

@dataclass
class BackupResult:
    backup_uri: str
    size_bytes: int
    checksum: Optional[str] = None
    completed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "backup_uri": self.backup_uri,
            "size_bytes": self.size_bytes,
            "checksum": self.checksum,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "metadata": self.metadata,
        }

class BackupProvider(ABC):
    @abstractmethod
    def backup_original(self, asset: Asset, source_path: Path) -> BackupResult:
        """
        Safely copies/uploads the original media source file to backup storage.
        Original source file MUST be preserved (never deleted or moved).
        """
        pass
