from typing import Optional, Dict, Union
from src.services.metadata.base import MetadataProvider, NewVideoMetadata

class StaticMetadataProvider(MetadataProvider):
    """
    In-memory controlled metadata provider for testing and controlled environments.
    Mappings can be keyed by filename or relative_path.
    """
    def __init__(self, mappings: Optional[Dict[str, Union[NewVideoMetadata, dict]]] = None):
        self._mappings: Dict[str, NewVideoMetadata] = {}
        if mappings:
            for k, v in mappings.items():
                self.register(k, v)

    def register(self, key: str, metadata: Union[NewVideoMetadata, dict]) -> None:
        if isinstance(metadata, dict):
            meta_obj = NewVideoMetadata(
                enlace_id=metadata["enlace_id"],
                title=metadata.get("title"),
                description=metadata.get("description"),
                category=metadata.get("category"),
                publication_date=metadata.get("publication_date"),
                extra=metadata.get("extra", {})
            )
        else:
            meta_obj = metadata
        self._mappings[key] = meta_obj

    def clear(self) -> None:
        self._mappings.clear()

    def get_metadata(self, relative_path: str, filename: str) -> Optional[NewVideoMetadata]:
        # Check by relative path first, then filename
        if relative_path in self._mappings:
            return self._mappings[relative_path]
        if filename in self._mappings:
            return self._mappings[filename]
        return None
