from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any, Dict

@dataclass
class NewVideoMetadata:
    enlace_id: str
    title: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    publication_date: Optional[datetime] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enlace_id": self.enlace_id,
            "title": self.title,
            "description": self.description,
            "category": self.category,
            "publication_date": self.publication_date.isoformat() if self.publication_date else None,
            "extra": self.extra,
        }

class MetadataProvider(ABC):
    @abstractmethod
    def get_metadata(self, relative_path: str, filename: str) -> Optional[NewVideoMetadata]:
        """
        Fetch authoritative metadata for a new video file.
        Returns NewVideoMetadata if metadata exists in the provider, or None if not found.
        """
        pass
