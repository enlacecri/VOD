from typing import Optional
from src.services.backup.base import BackupProvider
from src.services.backup.local_provider import LocalBackupProvider

_current_backup_provider: Optional[BackupProvider] = None

def get_backup_provider() -> BackupProvider:
    global _current_backup_provider
    if _current_backup_provider is None:
        _current_backup_provider = LocalBackupProvider()
    return _current_backup_provider

def set_backup_provider(provider: Optional[BackupProvider]) -> None:
    global _current_backup_provider
    _current_backup_provider = provider
