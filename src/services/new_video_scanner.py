import os
import re
import uuid
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session
from redis import Redis

from src.core.config import settings
from src.core.queues import QUEUE_INGEST, get_redis_connection
from src.core.canonical import build_canonical_manifest_path, build_canonical_manifest_url
from src.models.asset import Asset
from src.models.ingest_item import IngestItem
from src.models.enums import VideoStatus, IngestStatus
from src.services.metadata.base import MetadataProvider, NewVideoMetadata
from src.services.metadata.factory import get_metadata_provider
from src.services.job_dispatch import dispatch_progressive_job

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {".mp4", ".mov", ".mkv", ".mxf", ".avi", ".m4v"}
STRICT_ENLACE_ID_REGEX = re.compile(r"^[a-zA-Z0-9_\-\.]+$")

def compute_source_fingerprint(relative_path: str, size_bytes: int, mtime: float) -> str:
    content = f"{relative_path}:{size_bytes}:{mtime:.4f}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()

def is_safe_relative_path(root: Path, target: Path) -> bool:
    try:
        current = target
        while current != current.parent:
            if current.is_symlink():
                return False
            current = current.parent
        resolved = target.resolve(strict=False)
        return resolved.is_relative_to(root)
    except Exception:
        return False

@dataclass
class ScanResult:
    scanned: int = 0
    waiting_stable: int = 0
    metadata_pending: int = 0
    registered: int = 0
    dispatched: int = 0
    conflicts: int = 0
    failed: int = 0

    def print_summary(self) -> str:
        lines = [
            "NEW VIDEO INGEST SUMMARY",
            f"Scanned: {self.scanned}",
            f"Waiting stable: {self.waiting_stable}",
            f"Metadata pending: {self.metadata_pending}",
            f"Registered: {self.registered}",
            f"Dispatched: {self.dispatched}",
            f"Conflicts: {self.conflicts}",
            f"Failed: {self.failed}",
        ]
        return "\n".join(lines)


def scan_new_videos(
    root_path: Optional[str] = None,
    dry_run: bool = False,
    metadata_provider: Optional[MetadataProvider] = None,
    stable_seconds: Optional[int] = None,
    db: Optional[Session] = None,
    redis_conn: Optional[Redis] = None,
) -> ScanResult:
    """
    Idempotent scanner for new incoming videos in VOD_NEW_INGEST_ROOT.
    Ensures:
      - Real observation window for stability (never processes on first observation).
      - Strict security (no symlinks, no path traversal, ignores temp/hidden files).
      - Authoritative metadata fetch (never invents enlace_id from filename).
      - Conflict detection for duplicate enlace_id.
      - Safe registration and dispatch to vod_ingest queue.
      - 100% dry-run capability (no writes to DB or Redis).
    """
    result = ScanResult()
    ingest_root_str = root_path or settings.VOD_NEW_INGEST_ROOT
    ingest_root = Path(ingest_root_str).resolve()
    window_sec = stable_seconds if stable_seconds is not None else settings.VOD_INGEST_STABLE_SECONDS
    meta_prov = metadata_provider or get_metadata_provider()

    if not ingest_root.exists() or not ingest_root.is_dir():
        logger.warning(f"Ingest root does not exist or is not a directory: {ingest_root}")
        return result

    # Discover candidate files safely
    candidate_files: List[Path] = []
    for root_dir, dirnames, filenames in os.walk(ingest_root, followlinks=False):
        # Exclude hidden directories
        dirnames[:] = [d for d in dirnames if not d.startswith(".") and not Path(root_dir, d).is_symlink()]

        for fname in filenames:
            if fname.startswith("."):
                continue
            if fname.endswith((".tmp", ".part", ".partial")):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext not in ALLOWED_EXTENSIONS:
                continue

            file_path = Path(root_dir) / fname
            if file_path.is_symlink():
                continue
            if not is_safe_relative_path(ingest_root, file_path):
                continue

            candidate_files.append(file_path)

    candidate_files.sort()
    result.scanned = len(candidate_files)

    if result.scanned == 0:
        return result

    # If no DB session passed, create local session
    close_db_on_exit = False
    if db is None:
        from src.core.database import SessionLocal
        db = SessionLocal()
        close_db_on_exit = True

    try:
        now = datetime.now(timezone.utc)

        for fpath in candidate_files:
            try:
                rel_path = str(fpath.relative_to(ingest_root))
                stat = fpath.stat()
                size_bytes = stat.st_size
                mtime = stat.st_mtime
                fname = fpath.name
                fingerprint = compute_source_fingerprint(rel_path, size_bytes, mtime)

                # Look for existing IngestItem for this relative_path
                existing_item: Optional[IngestItem] = db.query(IngestItem).filter(
                    IngestItem.relative_path == rel_path
                ).order_by(IngestItem.created_at.desc()).first()

                if not existing_item:
                    # First observation of this file!
                    result.waiting_stable += 1
                    if not dry_run:
                        new_item = IngestItem(
                            relative_path=rel_path,
                            filename=fname,
                            size_bytes=size_bytes,
                            mtime=mtime,
                            source_fingerprint=fingerprint,
                            status=IngestStatus.WAITING_STABLE,
                            first_observed_at=now,
                            last_observed_at=now,
                        )
                        db.add(new_item)
                        db.commit()
                    continue

                # Existing item found. Inspect state and fingerprint:
                if existing_item.status in (IngestStatus.REGISTERED, IngestStatus.DISPATCHED):
                    if existing_item.source_fingerprint == fingerprint:
                        result.dispatched += 1
                        continue
                    else:
                        # File changed after dispatch! Record source changed / conflict
                        logger.warning(
                            f"File {rel_path} changed on disk after previous version was dispatched."
                        )
                        result.conflicts += 1
                        if not dry_run:
                            changed_item = IngestItem(
                                relative_path=rel_path,
                                filename=fname,
                                size_bytes=size_bytes,
                                mtime=mtime,
                                source_fingerprint=fingerprint,
                                status=IngestStatus.SOURCE_CHANGED,
                                first_observed_at=now,
                                last_observed_at=now,
                                last_error="File changed on disk after previous version was dispatched. Replacement review required.",
                            )
                            db.add(changed_item)
                            db.commit()
                        continue

                if existing_item.status == IngestStatus.CONFLICT:
                    result.conflicts += 1
                    continue

                if existing_item.status == IngestStatus.FAILED:
                    result.failed += 1
                    continue

                # Item is WAITING_STABLE or METADATA_PENDING
                # Check if size or mtime changed since last observation
                if size_bytes != existing_item.size_bytes or abs(mtime - existing_item.mtime) > 0.001:
                    result.waiting_stable += 1
                    if not dry_run:
                        existing_item.size_bytes = size_bytes
                        existing_item.mtime = mtime
                        existing_item.source_fingerprint = fingerprint
                        existing_item.first_observed_at = now
                        existing_item.last_observed_at = now
                        existing_item.status = IngestStatus.WAITING_STABLE
                        db.commit()
                    continue

                # File size and mtime are unchanged. Check elapsed observation window:
                elapsed = (now - existing_item.first_observed_at).total_seconds()
                if not dry_run:
                    existing_item.last_observed_at = now
                    db.commit()

                if elapsed < window_sec:
                    result.waiting_stable += 1
                    continue

                # File is STABLE!
                if not existing_item.stable_at and not dry_run:
                    existing_item.stable_at = now
                    db.commit()

                # Query authoritative metadata
                metadata: Optional[NewVideoMetadata] = meta_prov.get_metadata(rel_path, fname)
                if metadata is None:
                    result.metadata_pending += 1
                    if not dry_run and existing_item.status != IngestStatus.METADATA_PENDING:
                        existing_item.status = IngestStatus.METADATA_PENDING
                        db.commit()
                    continue

                # Metadata available! Validate strict enlace_id
                enlace_id = metadata.enlace_id.strip() if metadata.enlace_id else ""
                if not enlace_id or not STRICT_ENLACE_ID_REGEX.match(enlace_id):
                    result.failed += 1
                    if not dry_run:
                        existing_item.status = IngestStatus.FAILED
                        existing_item.last_error = f"Invalid enlace_id format: '{enlace_id}'"
                        db.commit()
                    continue

                # Check if enlace_id already exists in assets
                existing_asset = db.query(Asset).filter(Asset.enlace_id == enlace_id).first()
                if existing_asset:
                    if existing_item.asset_id == existing_asset.id:
                        # Already linked
                        result.dispatched += 1
                        continue
                    else:
                        result.conflicts += 1
                        if not dry_run:
                            existing_item.status = IngestStatus.CONFLICT
                            existing_item.last_error = f"Conflict: enlace_id '{enlace_id}' already assigned to asset {existing_asset.vod_uuid}"
                            db.commit()
                        continue

                # Ready to register and dispatch!
                if dry_run:
                    result.registered += 1
                    result.dispatched += 1
                    continue

                # In real mode: create Asset (COLD)
                new_vod_uuid = uuid.uuid4()
                asset = Asset(
                    vod_uuid=new_vod_uuid,
                    enlace_id=enlace_id,
                    source_uri=rel_path,
                    status=VideoStatus.COLD,
                    size=size_bytes,
                    manifest_path=build_canonical_manifest_path(new_vod_uuid, enlace_id),
                    manifest_url=build_canonical_manifest_url(new_vod_uuid, enlace_id),
                )
                db.add(asset)
                db.commit()
                db.refresh(asset)

                existing_item.asset_id = asset.id
                existing_item.status = IngestStatus.REGISTERED
                existing_item.registered_at = datetime.now(timezone.utc)
                existing_item.metadata_snapshot = metadata.to_dict()
                db.commit()
                result.registered += 1

                # Dispatch to vod_ingest queue
                try:
                    dispatch_progressive_job(
                        asset=asset,
                        queue_name=QUEUE_INGEST,
                        db=db,
                        context="new-video-ingest",
                        redis_conn=redis_conn,
                    )
                    existing_item.status = IngestStatus.DISPATCHED
                    db.commit()
                    result.dispatched += 1
                except Exception as e:
                    logger.error(f"Failed to dispatch asset {asset.vod_uuid} to {QUEUE_INGEST}: {e}")
                    # Asset remains in DB, IngestItem remains registered for reconciler/retry
                    db.rollback()

            except Exception as item_err:
                logger.exception(f"Error processing file {fpath}: {item_err}")
                result.failed += 1
                if not dry_run:
                    db.rollback()

    finally:
        if close_db_on_exit:
            db.close()

    return result
