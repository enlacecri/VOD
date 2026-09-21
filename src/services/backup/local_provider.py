import shutil
import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.core.config import settings
from src.models.asset import Asset
from src.services.backup.base import BackupProvider, BackupResult

class BackupError(Exception):
    pass

class LocalBackupProvider(BackupProvider):
    """
    Local filesystem backup provider for development, testing and staging.
    Uses safe copy semantics:
      temp file -> copy -> verify size/hash -> atomic rename.
    Never modifies or deletes the original source file.
    Supports failure simulation for testing.
    """
    def __init__(self, backup_dir: Optional[str] = None, should_fail: bool = False):
        self.backup_dir = Path(backup_dir or settings.VOD_BACKUP_STORAGE_DIR).resolve()
        self.should_fail = should_fail

    def backup_original(self, asset: Asset, source_path: Path) -> BackupResult:
        if self.should_fail:
            raise BackupError("Simulated backup failure for testing.")

        resolved_source = source_path.resolve()
        if not resolved_source.exists() or not resolved_source.is_file():
            raise BackupError(f"Original source file not found: {resolved_source}")

        source_size = resolved_source.stat().st_size
        
        # Calculate target backup path
        rel_uri = asset.source_uri if asset.source_uri else resolved_source.name
        # Sanitize rel_uri to be relative
        clean_rel = Path(rel_uri).name
        dest_dir = self.backup_dir / str(asset.vod_uuid)
        dest_dir.mkdir(parents=True, exist_ok=True)
        final_dest = dest_dir / clean_rel
        temp_dest = dest_dir / f"{clean_rel}.{uuid.uuid4().hex[:8]}.tmp"

        try:
            # 1. Copy to temp file
            shutil.copyfile(resolved_source, temp_dest)

            # 2. Verify size
            copied_size = temp_dest.stat().st_size
            if copied_size != source_size:
                raise BackupError(
                    f"Backup size mismatch: original {source_size} bytes vs copied {copied_size} bytes"
                )

            # 3. Checksum if asset has one or calculate lightweight sha256
            hasher = hashlib.sha256()
            with open(temp_dest, "rb") as f:
                for chunk in iter(lambda: f.read(settings.HASH_CHUNK_SIZE), b""):
                    hasher.update(chunk)
            computed_checksum = hasher.hexdigest()

            if asset.source_sha256 and computed_checksum != asset.source_sha256:
                raise BackupError(
                    f"Backup checksum mismatch: expected {asset.source_sha256}, got {computed_checksum}"
                )

            # 4. Atomic replace/rename
            temp_dest.replace(final_dest)

            return BackupResult(
                backup_uri=f"file://{final_dest.resolve()}",
                size_bytes=copied_size,
                checksum=computed_checksum,
                completed_at=datetime.now(timezone.utc),
                metadata={
                    "provider": "LocalBackupProvider",
                    "backup_path": str(final_dest),
                    "original_path": str(resolved_source),
                }
            )
        except Exception:
            if temp_dest.exists():
                try:
                    temp_dest.unlink()
                except OSError:
                    pass
            raise
