from src.models.asset import Asset
from src.services.sync.base import EnlaceSyncProvider, SyncResult

class DatabaseEnlaceProvider(EnlaceSyncProvider):
    """
    Placeholder provider for Enlace+ external database synchronization.
    Raises NotImplementedError until external connection string and schema are defined.
    """
    def __init__(self, connection_string: str = ""):
        self.connection_string = connection_string

    def sync_ready_asset(self, asset: Asset) -> SyncResult:
        raise NotImplementedError("Enlace+ database sync not configured. External system not connected.")
