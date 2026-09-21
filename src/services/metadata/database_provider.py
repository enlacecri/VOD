from typing import Optional
from src.services.metadata.base import MetadataProvider, NewVideoMetadata

class DatabaseXProvider(MetadataProvider):
    """
    Placeholder provider for Database X (external production metadata authority).
    Raises NotImplementedError until external connection and schema are defined.
    """
    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string

    def get_metadata(self, relative_path: str, filename: str) -> Optional[NewVideoMetadata]:
        raise NotImplementedError("Database X adapter not configured. External system not connected.")
