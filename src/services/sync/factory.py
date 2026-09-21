from typing import Optional
from src.services.sync.base import EnlaceSyncProvider
from src.services.sync.static_provider import StaticEnlaceSyncProvider

_current_sync_provider: Optional[EnlaceSyncProvider] = None

def get_sync_provider() -> EnlaceSyncProvider:
    global _current_sync_provider
    if _current_sync_provider is None:
        _current_sync_provider = StaticEnlaceSyncProvider()
    return _current_sync_provider

def set_sync_provider(provider: Optional[EnlaceSyncProvider]) -> None:
    global _current_sync_provider
    _current_sync_provider = provider
