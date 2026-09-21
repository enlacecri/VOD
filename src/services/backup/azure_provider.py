from pathlib import Path
from src.models.asset import Asset
from src.services.backup.base import BackupProvider, BackupResult

class AzureBlobBackupProvider(BackupProvider):
    """
    Placeholder provider for Azure Blob Storage.
    Raises NotImplementedError until Azure credentials and container are configured.
    """
    def __init__(self, connection_string: str = "", container_name: str = ""):
        self.connection_string = connection_string
        self.container_name = container_name

    def backup_original(self, asset: Asset, source_path: Path) -> BackupResult:
        raise NotImplementedError("Azure Blob Backup not yet configured. External system not connected.")
