from typing import Optional
from src.services.metadata.base import MetadataProvider
from src.services.metadata.static_provider import StaticMetadataProvider

_current_metadata_provider: Optional[MetadataProvider] = None

def get_metadata_provider() -> MetadataProvider:
    global _current_metadata_provider
    if _current_metadata_provider is None:
        _current_metadata_provider = StaticMetadataProvider()
    return _current_metadata_provider

def set_metadata_provider(provider: Optional[MetadataProvider]) -> None:
    global _current_metadata_provider
    _current_metadata_provider = provider
