import os
import hashlib
from pathlib import Path
from src.core.config import settings

class SecurityError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)

class FileModifiedError(SecurityError):
    def __init__(self, message: str):
        super().__init__("E_FILE_MODIFIED", message)

class FileSizeLimitError(SecurityError):
    def __init__(self, message: str):
        super().__init__("E_FILE_TOO_LARGE", message)

def _get_file_identity(fd: int) -> tuple:
    """Returns a tuple uniquely identifying the state of an open file descriptor."""
    stat = os.fstat(fd)
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)

def secure_resolve(root_path: Path, relative_uri: str) -> Path:
    """
    Validates that a given URI safely resolves inside root_path and is a regular file.
    Returns the resolved Path object.
    Raises SecurityError with specific codes.
    """
    try:
        candidate = (root_path / relative_uri).resolve(strict=True)
    except FileNotFoundError:
        raise SecurityError("E_SOURCE_NOT_FOUND", "Invalid source_uri: File does not exist")
    except RuntimeError:
        raise SecurityError("E_SOURCE_ESCAPES_ROOT", "Invalid source_uri: Symlink resolution error")
        
    try:
        candidate.relative_to(root_path)
    except ValueError:
        raise SecurityError("E_SOURCE_ESCAPES_ROOT", "Invalid source_uri: escapes root")
        
    if not candidate.is_file():
        raise SecurityError("E_SOURCE_NOT_REGULAR", "Invalid source_uri: Not a regular file")
        
    return candidate

def validate_ingest_path(source_uri: str) -> Path:
    try:
        ingest_root = Path(settings.INGEST_ROOT).resolve(strict=True)
    except FileNotFoundError:
        raise SecurityError("E_INGEST_ROOT_NOT_FOUND", "INGEST_ROOT does not exist")
    return secure_resolve(ingest_root, source_uri)

def secure_copy_and_hash(source_path: Path, dest_path: Path, heartbeat_callback=None) -> tuple[str, int]:
    import time
    if not source_path.is_file():
        raise SecurityError("E_SOURCE_NOT_REGULAR", f"Target is not a regular file: {source_path}")
        
    try:
        fd_src = os.open(str(source_path), os.O_RDONLY)
    except OSError as e:
        raise SecurityError("E_SOURCE_NOT_FOUND", f"Could not open source file: {e}")
        
    fd_dest = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, 'O_NOFOLLOW'):
            flags |= os.O_NOFOLLOW
            
        try:
            fd_dest = os.open(str(dest_path), flags, 0o600)
        except FileExistsError:
            raise SecurityError("E_DEST_EXISTS", f"Destination file already exists: {dest_path}")
        
        identity_before = _get_file_identity(fd_src)
        
        # Check size limits
        size = identity_before[2]
        if size > settings.MAX_FILE_SIZE_BYTES:
            raise FileSizeLimitError(f"File exceeds maximum allowed size of {settings.MAX_FILE_SIZE_BYTES} bytes")
            
        hasher = hashlib.sha256()
        total_copied = 0
        last_heartbeat = time.time()
        
        while True:
            chunk = os.read(fd_src, settings.HASH_CHUNK_SIZE)
            if not chunk:
                break
            
            view = memoryview(chunk)
            written = 0
            while written < len(view):
                n = os.write(fd_dest, view[written:])
                if n == 0:
                    raise SecurityError("E_COPY_INCOMPLETE", "Write returned 0 bytes")
                written += n
                
            hasher.update(chunk)
            total_copied += written
            
            if heartbeat_callback and (time.time() - last_heartbeat > settings.RQ_JOB_TIMEOUT_SECONDS * 0.25):
                heartbeat_callback()
                last_heartbeat = time.time()
            
        if total_copied != size:
            raise SecurityError("E_COPY_INCOMPLETE", f"Bytes copied ({total_copied}) do not match file size ({size})")
            
        # fsync the destination to ensure it's written to disk
        os.fsync(fd_dest)
            
        identity_after = _get_file_identity(fd_src)
        
        if identity_before != identity_after:
            raise FileModifiedError("File was modified during hash calculation and copy")
            
        return hasher.hexdigest(), size
    finally:
        os.close(fd_src)
        if fd_dest is not None:
            os.close(fd_dest)
