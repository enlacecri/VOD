import os
import re
import json
import uuid
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union, Callable, Any
from dataclasses import dataclass, field, asdict

from sqlalchemy.orm import Session

from src.core.config import settings
from src.core.database import SessionLocal
from src.core.canonical import (
    normalize_vod_uuid,
    build_canonical_manifest_path,
    build_canonical_manifest_url,
)
from src.core.security import secure_resolve, SecurityError
from src.models.asset import Asset
from src.models.enums import VideoStatus
from src.schemas.asset import ENLACE_ID_REGEX

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {".mp4", ".mov", ".mkv", ".mxf", ".avi", ".m4v"}

IGNORED_DIR_NAMES = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    "staging",
    "output",
    "progressive",
    "processed",
    "logs",
    "run",
}

def derive_enlace_id(path: Union[str, Path]) -> Optional[str]:
    """
    Centralized function to derive and validate an enlace_id from a file path.
    Rule:
      1. Filename stem (without extension).
      2. Must have an allowed video extension.
      3. Must NOT be a hidden file (e.g. .DS_Store, .gitkeep).
      4. Must match ENLACE_ID_REGEX: ^[A-Za-z0-9_-]{1,128}$
    Returns the exact enlace_id string if valid, or None if invalid.
    """
    p = Path(path)
    filename = p.name

    # Reject hidden files
    if filename.startswith("."):
        return None

    ext = p.suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return None

    stem = p.stem
    if not ENLACE_ID_REGEX.match(stem):
        return None

    return stem

@dataclass
class CatalogImportResult:
    root_path: str
    dry_run: bool
    limit: Optional[int]
    total_scanned: int = 0
    created: int = 0
    already_exists: int = 0
    conflicts: int = 0
    invalid: int = 0
    errors: int = 0
    duration_seconds: float = 0.0
    assets_per_second: float = 0.0
    report_file: Optional[str] = None
    conflict_details: list[dict] = field(default_factory=list)
    invalid_details: list[dict] = field(default_factory=list)
    error_details: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

def scan_catalog_files(
    root_dir: Path,
    limit: Optional[int] = None,
    progress_callback: Optional[Callable[[str], None]] = None
) -> tuple[list[tuple[str, str]], list[dict], list[dict]]:
    """
    Recursively scans root_dir for valid video files without following symlinks.
    Returns:
      - candidates: list of (rel_path_posix, enlace_id)
      - invalid_items: list of dicts describing skipped/invalid files
      - scan_conflicts: list of dicts describing duplicates detected within the scan
    """
    resolved_root = root_dir.resolve(strict=True)
    all_raw_candidates: list[tuple[str, str]] = []
    invalid_items: list[dict] = []

    if progress_callback:
        progress_callback(f"Scanning directory {resolved_root} (followlinks=False)...")

    for dirpath, dirnames, filenames in os.walk(resolved_root, followlinks=False):
        # Prune hidden and special directories in place
        dirnames[:] = [
            d for d in dirnames 
            if not d.startswith(".") and d.lower() not in IGNORED_DIR_NAMES
        ]

        # Check if dirpath itself contains any symlink in its chain from resolved_root
        current_dir = Path(dirpath)
        dir_has_symlink = False
        check_dir = current_dir
        while check_dir != resolved_root:
            if check_dir.is_symlink():
                dir_has_symlink = True
                break
            if check_dir.parent == check_dir:
                break
            check_dir = check_dir.parent

        if dir_has_symlink:
            # Skip this entire directory tree
            dirnames.clear()
            continue

        for filename in filenames:
            if filename.startswith("."):
                continue

            file_path = current_dir / filename

            # Reject symlinks
            if file_path.is_symlink():
                try:
                    rel_str = file_path.relative_to(resolved_root).as_posix()
                except ValueError:
                    rel_str = filename
                invalid_items.append({
                    "path": rel_str,
                    "reason": "SYMLINK_REJECTED",
                    "detail": "Symlinks are strictly prohibited"
                })
                continue

            # Validate path security and traversal
            try:
                rel_path = file_path.relative_to(resolved_root).as_posix()
                # Security resolution check
                secure_resolve(resolved_root, rel_path)
            except (SecurityError, ValueError) as e:
                invalid_items.append({
                    "path": filename,
                    "reason": "SECURITY_VIOLATION",
                    "detail": str(e)
                })
                continue

            # Check extension
            ext = file_path.suffix.lower()
            if ext not in ALLOWED_EXTENSIONS:
                invalid_items.append({
                    "path": rel_path,
                    "reason": "SKIPPED_UNSUPPORTED_EXTENSION",
                    "detail": f"Extension '{ext}' not in supported video formats"
                })
                continue

            # Derive enlace_id
            enlace_id = derive_enlace_id(file_path)
            if not enlace_id:
                invalid_items.append({
                    "path": rel_path,
                    "reason": "SKIPPED_INVALID_ENLACE_ID",
                    "detail": f"Filename '{filename}' cannot derive a valid enlace_id matching ^[A-Za-z0-9_-]{{1,128}}$"
                })
                continue

            all_raw_candidates.append((rel_path, enlace_id))

    # In-memory duplicate detection within the scan
    seen_ids: dict[str, str] = {}
    duplicate_ids: set[str] = set()
    scan_conflicts: list[dict] = []

    for rel_path, enlace_id in all_raw_candidates:
        if enlace_id in seen_ids:
            duplicate_ids.add(enlace_id)
            scan_conflicts.append({
                "enlace_id": enlace_id,
                "reason": "CONFLICT_DUPLICATE_ENLACE_ID",
                "first_path": seen_ids[enlace_id],
                "conflicting_path": rel_path
            })
        else:
            seen_ids[enlace_id] = rel_path

    # Filter out any enlace_id that had collisions in the scan
    valid_candidates: list[tuple[str, str]] = [
        (path, eid) for (path, eid) in all_raw_candidates 
        if eid not in duplicate_ids
    ]

    # Sort for deterministic execution
    valid_candidates.sort(key=lambda x: (x[1], x[0]))

    if limit is not None and limit > 0:
        valid_candidates = valid_candidates[:limit]

    return valid_candidates, invalid_items, scan_conflicts

def import_catalog_cold_assets(
    root_dir: Optional[Union[str, Path]] = None,
    dry_run: bool = False,
    limit: Optional[int] = None,
    batch_size: int = 250,
    db: Optional[Session] = None,
    progress_callback: Optional[Callable[[str], None]] = None
) -> CatalogImportResult:
    """
    Orchestrates the bulk registration of catalog assets as COLD.
    Idempotent, safe, batched, with dry-run support.
    """
    start_time = time.time()
    target_root = Path(root_dir if root_dir else settings.INGEST_ROOT).resolve()

    if not target_root.exists() or not target_root.is_dir():
        raise FileNotFoundError(f"Catalog root directory does not exist: {target_root}")

    # Discover and sanitize candidate files
    candidates, invalid_items, scan_conflicts = scan_catalog_files(
        root_dir=target_root,
        limit=limit,
        progress_callback=progress_callback
    )

    result = CatalogImportResult(
        root_path=str(target_root),
        dry_run=dry_run,
        limit=limit,
        total_scanned=len(candidates) + len(invalid_items) + len(scan_conflicts),
        invalid=len(invalid_items),
        conflicts=len(scan_conflicts),
        invalid_details=invalid_items,
        conflict_details=list(scan_conflicts)
    )

    if progress_callback:
        progress_callback(
            f"Found {len(candidates)} valid candidates "
            f"({len(invalid_items)} invalid/skipped, {len(scan_conflicts)} scan collisions)"
        )

    own_session = False
    if db is None:
        db = SessionLocal()
        own_session = True

    try:
        total_candidates = len(candidates)
        processed_count = 0
        new_assets_batch: list[Asset] = []

        # Process in chunks to minimize query count
        chunk_size = max(1, batch_size)
        for i in range(0, total_candidates, chunk_size):
            chunk = candidates[i:i + chunk_size]
            chunk_eids = [eid for _, eid in chunk]

            # Query existing assets in bulk for this chunk
            existing_assets = db.query(Asset).filter(Asset.enlace_id.in_(chunk_eids)).all()
            existing_by_eid = {a.enlace_id: a for a in existing_assets}

            for rel_path, enlace_id in chunk:
                processed_count += 1
                existing = existing_by_eid.get(enlace_id)

                if existing:
                    if existing.source_uri == rel_path:
                        result.already_exists += 1
                    else:
                        result.conflicts += 1
                        result.conflict_details.append({
                            "enlace_id": enlace_id,
                            "reason": "CONFLICT_EXISTING_ASSET_DIFFERENT_SOURCE",
                            "existing_source_uri": existing.source_uri,
                            "new_source_uri": rel_path
                        })
                else:
                    if dry_run:
                        result.created += 1
                    else:
                        vod_uuid = uuid.uuid4()
                        manifest_url = build_canonical_manifest_url(vod_uuid, enlace_id)
                        manifest_path = build_canonical_manifest_path(vod_uuid, enlace_id)

                        new_asset = Asset(
                            vod_uuid=vod_uuid,
                            enlace_id=enlace_id,
                            source_uri=rel_path,
                            status=VideoStatus.COLD,
                            progress=0,
                            manifest_url=manifest_url,
                            manifest_path=manifest_path
                        )
                        new_assets_batch.append(new_asset)
                        result.created += 1

            # Commit batch if not dry_run
            if not dry_run and new_assets_batch:
                try:
                    db.add_all(new_assets_batch)
                    db.commit()
                except Exception as e:
                    db.rollback()
                    logger.error(f"Batch insert failed, falling back to row-by-row: {e}")
                    # Fallback row-by-row to isolate failures
                    for asset_item in new_assets_batch:
                        try:
                            db.add(asset_item)
                            db.commit()
                        except Exception as row_e:
                            db.rollback()
                            result.created -= 1
                            result.errors += 1
                            result.error_details.append({
                                "enlace_id": asset_item.enlace_id,
                                "source_uri": asset_item.source_uri,
                                "error": str(row_e)
                            })
                new_assets_batch.clear()

            if progress_callback and (processed_count % 100 == 0 or processed_count == total_candidates):
                progress_callback(
                    f"[{processed_count}/{total_candidates}] "
                    f"Created: {result.created} | Existing: {result.already_exists} | "
                    f"Conflicts: {result.conflicts} | Errors: {result.errors}"
                )

    finally:
        if own_session:
            db.close()

    elapsed = time.time() - start_time
    result.duration_seconds = round(elapsed, 4)
    if elapsed > 0:
        result.assets_per_second = round((result.created + result.already_exists) / elapsed, 2)

    # Persist JSON report to storage/logs/
    try:
        log_dir = Path(settings.LOG_ROOT).resolve()
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        report_filename = f"catalog-import-{timestamp_str}.json"
        report_path = log_dir / report_filename

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, indent=2)

        result.report_file = str(report_path)
    except Exception as e:
        logger.warning(f"Could not write catalog import report: {e}")

    return result
