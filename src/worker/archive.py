import os
import shutil
import hashlib
import logging
import tempfile
from pathlib import Path
from sqlalchemy.orm import Session

from src.core.config import settings
from src.models.asset import Asset
from src.models.asset_event import AssetEvent
from src.models.enums import EventType

logger = logging.getLogger(__name__)

def compute_sha256(filepath: str) -> str:
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(settings.HASH_CHUNK_SIZE), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def is_safe_path(base: Path, target: Path) -> bool:
    try:
        # Check for symlinks in any part of the path
        current = target
        while current != current.parent:
            if current.is_symlink():
                return False
            current = current.parent
            
        resolved = target.resolve(strict=False)
        return resolved.is_relative_to(base)
    except Exception:
        return False

def archive_source_file(db: Session, asset: Asset):
    """
    Intenta archivar el archivo de entrada.
    Actualiza asset.processed_source_path si tiene éxito.
    Registra AssetEvent si falla.
    El caller debe hacer el db.commit().
    """
    def log_failure(msg: str):
        logger.warning(f"Archive failed for asset {asset.id}: {msg}")
        details = {
            "error_code": "E_SOURCE_ARCHIVE_FAILED",
            "error_message": msg[:settings.ERROR_MESSAGE_MAX_LENGTH]
        }
        try:
            if asset.source_uri:
                details["source_path"] = str(Path(asset.source_uri))
                details["processed_path"] = str(Path(asset.source_uri))
        except Exception:
            pass
            
        event = AssetEvent(
            asset_id=asset.id,
            event_type=EventType.ERROR,
            details=details
        )
        db.add(event)

    try:
        ingest_root = Path(settings.INGEST_ROOT).resolve(strict=True)
        processed_root = Path(settings.PROCESSED_ROOT).resolve(strict=True)
        
        if not asset.source_uri:
            log_failure("source_uri is empty")
            return
            
        source_path = Path(asset.source_uri)
        if source_path.is_absolute():
            log_failure("source_uri is absolute")
            return
            
        src_raw = ingest_root / source_path
        dst_raw = processed_root / source_path
        
        if not is_safe_path(ingest_root, src_raw):
            log_failure("Source path is outside INGEST_ROOT or contains symlinks")
            return
            
        if not is_safe_path(processed_root, dst_raw):
            log_failure("Destination path is outside PROCESSED_ROOT or contains symlinks")
            return
            
        src_full = src_raw.resolve(strict=False)
        dst_full = dst_raw.resolve(strict=False)
            
        if not src_full.exists():
            if dst_full.exists() and dst_full.is_file():
                dst_size = dst_full.stat().st_size
                dst_sha = compute_sha256(str(dst_full))
                if dst_size == asset.size and dst_sha == asset.source_sha256:
                    logger.info(f"Asset {asset.id} already archived. Reconciling DB.")
                    asset.processed_source_path = str(source_path)
                    return
            log_failure("Source file does not exist and valid destination not found")
            return
            
        if not src_full.is_file():
            log_failure("Source is not a regular file")
            return
            
        src_size = src_full.stat().st_size
        if src_size != asset.size:
            log_failure("Source file size changed after ingest")
            return
            
        src_sha = compute_sha256(str(src_full))
        if src_sha != asset.source_sha256:
            log_failure("Source file SHA-256 changed after ingest")
            return
            
        if dst_full.exists():
            if not dst_full.is_file():
                log_failure("Destination exists but is not a regular file")
                return
            dst_size = dst_full.stat().st_size
            dst_sha = compute_sha256(str(dst_full))
            if dst_size == asset.size and dst_sha == asset.source_sha256:
                logger.info(f"Asset {asset.id} destination already matches. Reconciling DB.")
                src_full.unlink(missing_ok=True)
                asset.processed_source_path = str(source_path)
                return
            else:
                log_failure("Destination already exists with a different size/hash. Refusing to overwrite.")
                return
                
        dst_full.parent.mkdir(parents=True, exist_ok=True)
        
        src_dev = os.stat(src_full).st_dev
        dst_dev = os.stat(dst_full.parent).st_dev
        
        if src_dev == dst_dev:
            os.replace(src_full, dst_full)
        else:
            fd, tmp_dst_str = tempfile.mkstemp(dir=dst_full.parent, prefix=".archive_", suffix=".tmp")
            os.close(fd)
            tmp_dst = Path(tmp_dst_str)
            shutil.copyfile(src_full, tmp_dst)
            
            tmp_size = tmp_dst.stat().st_size
            tmp_sha = compute_sha256(str(tmp_dst))
            if tmp_size != asset.size or tmp_sha != asset.source_sha256:
                tmp_dst.unlink(missing_ok=True)
                log_failure("Cross-filesystem copy corrupted. SHA-256 or size mismatch.")
                return
                
            os.replace(tmp_dst, dst_full)
            
            with open(dst_full, 'r+b') as f:
                os.fsync(f.fileno())
                
            try:
                dir_fd = os.open(str(dst_full.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception as e:
                logger.debug(f"Directory fsync not supported: {e}")
                
            src_full.unlink()
            
        asset.processed_source_path = str(source_path)
        logger.info(f"Successfully archived asset {asset.id} to {dst_full}")

    except Exception as e:
        logger.exception(f"Unexpected error archiving asset {asset.id}")
        log_failure(str(e))
