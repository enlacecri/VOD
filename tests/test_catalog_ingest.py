import os
import uuid
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
from redis import Redis
from rq import Queue

from src.core.config import settings
from src.core.database import SessionLocal
from src.core.canonical import build_canonical_manifest_url, build_canonical_manifest_path
from src.models.asset import Asset
from src.models.job import Job
from src.models.enums import VideoStatus, JobType
from src.services.catalog_ingest import (
    derive_enlace_id,
    scan_catalog_files,
    import_catalog_cold_assets,
    CatalogImportResult,
    ALLOWED_EXTENSIONS
)
from tests.conftest import TestingSessionLocal

@pytest.fixture
def test_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

# 1. Descubrimiento recursivo de videos
def test_recursive_discovery(tmp_path):
    sub1 = tmp_path / "sub1"
    sub2 = tmp_path / "sub1" / "sub2"
    sub2.mkdir(parents=True)
    
    (tmp_path / "root.mp4").write_bytes(b"dummy")
    (sub1 / "level1.mov").write_bytes(b"dummy")
    (sub2 / "level2.mkv").write_bytes(b"dummy")

    candidates, invalid, conflicts = scan_catalog_files(tmp_path)
    found_paths = [c[0] for c in candidates]
    assert "root.mp4" in found_paths
    assert "sub1/level1.mov" in found_paths
    assert "sub1/sub2/level2.mkv" in found_paths
    assert len(candidates) == 3

# 2 & 17. No seguir symlinks (ignorados / rechazados)
def test_symlinks_not_followed_and_rejected(tmp_path):
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    (real_dir / "target.mp4").write_bytes(b"dummy")

    # Symlink to file
    symlink_file = tmp_path / "symlink_video.mp4"
    symlink_file.symlink_to(real_dir / "target.mp4")

    # Symlink to dir
    symlink_dir = tmp_path / "sym_dir"
    symlink_dir.symlink_to(real_dir)

    candidates, invalid, conflicts = scan_catalog_files(tmp_path)
    candidate_paths = [c[0] for c in candidates]

    # Target in real dir is found
    assert "real/target.mp4" in candidate_paths
    # Symlink file is NOT accepted as candidate
    assert "symlink_video.mp4" not in candidate_paths
    # Symlink directory is not traversed
    assert "sym_dir/target.mp4" not in candidate_paths

    # Invalid items record the rejected symlink
    reasons = [inv["reason"] for inv in invalid]
    assert "SYMLINK_REJECTED" in reasons

# 3. Derivación correcta de enlace_id
def test_derive_enlace_id_valid():
    assert derive_enlace_id("PREDI-VICTO89.mp4") == "PREDI-VICTO89"
    assert derive_enlace_id("/path/to/PREDI-BAYLE539.mov") == "PREDI-BAYLE539"
    assert derive_enlace_id("sub/dir/TEST_123-abc.mkv") == "TEST_123-abc"
    assert derive_enlace_id("VIDEO_UPPER.MP4") == "VIDEO_UPPER"

# 4. Archivo inválido -> skipped
def test_invalid_files_skipped(tmp_path):
    (tmp_path / "invalid spaces.mp4").write_bytes(b"dummy")
    (tmp_path / "invalid$char.mp4").write_bytes(b"dummy")
    (tmp_path / "doc.pdf").write_bytes(b"dummy")
    (tmp_path / ".hidden.mp4").write_bytes(b"dummy")

    candidates, invalid, conflicts = scan_catalog_files(tmp_path)
    assert len(candidates) == 0
    reasons = {inv["reason"] for inv in invalid}
    assert "SKIPPED_UNSUPPORTED_EXTENSION" in reasons
    assert "SKIPPED_INVALID_ENLACE_ID" in reasons

# 5, 6, 7, 8, 9. Asset nuevo -> COLD, con URL canónica, sin Jobs, sin Redis, sin FFmpeg
def test_new_asset_registered_as_cold_without_jobs_or_transcode(tmp_path, test_db):
    (tmp_path / "PREDI-COLD100.mp4").write_bytes(b"dummy")

    with patch("subprocess.Popen") as mock_popen, \
         patch("redis.Redis.from_url") as mock_redis:

        result = import_catalog_cold_assets(
            root_dir=tmp_path,
            dry_run=False,
            db=test_db
        )

        assert result.created == 1
        assert result.conflicts == 0
        assert result.errors == 0

        # Verify DB asset
        asset = test_db.query(Asset).filter(Asset.enlace_id == "PREDI-COLD100").first()
        assert asset is not None
        assert asset.status == VideoStatus.COLD
        assert asset.progress == 0
        assert asset.playable is False
        assert "*definst*" in asset.manifest_url
        assert "_definst_" in asset.manifest_path

        # Verify 0 jobs
        jobs = test_db.query(Job).filter(Job.asset_id == asset.id).all()
        assert len(jobs) == 0

        # Verify NO Redis calls to enqueue
        mock_redis.assert_not_called()

        # Verify NO FFmpeg subprocess calls
        mock_popen.assert_not_called()

# 10 & 11. Idempotencia: segunda importación con misma ruta -> ALREADY_EXISTS
def test_idempotent_second_run_already_exists(tmp_path, test_db):
    (tmp_path / "IDEMP-TEST01.mp4").write_bytes(b"dummy")

    # First run
    res1 = import_catalog_cold_assets(root_dir=tmp_path, dry_run=False, db=test_db)
    assert res1.created == 1
    assert res1.already_exists == 0

    asset1 = test_db.query(Asset).filter(Asset.enlace_id == "IDEMP-TEST01").first()
    uuid1 = asset1.vod_uuid

    # Second run
    res2 = import_catalog_cold_assets(root_dir=tmp_path, dry_run=False, db=test_db)
    assert res2.created == 0
    assert res2.already_exists == 1
    assert res2.conflicts == 0

    asset2 = test_db.query(Asset).filter(Asset.enlace_id == "IDEMP-TEST01").first()
    assert asset2.vod_uuid == uuid1

# 12. Mismo enlace_id + distinta ruta -> CONFLICT
def test_same_enlace_id_different_path_conflict(tmp_path, test_db):
    folder1 = tmp_path / "folder1"
    folder1.mkdir()
    (folder1 / "PREDI-DIFFPATH.mp4").write_bytes(b"dummy")

    # First run imports folder1/PREDI-DIFFPATH.mp4
    res1 = import_catalog_cold_assets(root_dir=tmp_path, dry_run=False, db=test_db)
    assert res1.created == 1

    # Remove folder1 and create folder2 with same filename
    shutil.rmtree(folder1)
    folder2 = tmp_path / "folder2"
    folder2.mkdir()
    (folder2 / "PREDI-DIFFPATH.mp4").write_bytes(b"dummy")

    # Second run detects existing asset with different source_uri
    res2 = import_catalog_cold_assets(root_dir=tmp_path, dry_run=False, db=test_db)
    assert res2.created == 0
    assert res2.conflicts == 1
    assert res2.conflict_details[0]["reason"] == "CONFLICT_EXISTING_ASSET_DIFFERENT_SOURCE"

# 13. Duplicados dentro del mismo scan -> CONFLICT_DUPLICATE_ENLACE_ID
def test_duplicates_within_same_scan_conflict(tmp_path, test_db):
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()

    (dir_a / "DUPLICATE-SCAN.mp4").write_bytes(b"dummy")
    (dir_b / "DUPLICATE-SCAN.mov").write_bytes(b"dummy")

    res = import_catalog_cold_assets(root_dir=tmp_path, dry_run=False, db=test_db)
    assert res.created == 0
    assert res.conflicts == 1
    assert res.conflict_details[0]["reason"] == "CONFLICT_DUPLICATE_ENLACE_ID"

    # Confirm neither was inserted into DB
    asset = test_db.query(Asset).filter(Asset.enlace_id == "DUPLICATE-SCAN").first()
    assert asset is None

# 14. --dry-run no modifica la base de datos
def test_dry_run_does_not_modify_db(tmp_path, test_db):
    (tmp_path / "DRYRUN-TEST.mp4").write_bytes(b"dummy")

    count_before = test_db.query(Asset).count()

    res = import_catalog_cold_assets(root_dir=tmp_path, dry_run=True, db=test_db)
    assert res.dry_run is True
    assert res.created == 1

    count_after = test_db.query(Asset).count()
    assert count_after == count_before

    asset = test_db.query(Asset).filter(Asset.enlace_id == "DRYRUN-TEST").first()
    assert asset is None

# 15. --limit limita la cantidad procesada
def test_limit_option(tmp_path, test_db):
    for i in range(10):
        (tmp_path / f"LIMIT-{i:02d}.mp4").write_bytes(b"dummy")

    res = import_catalog_cold_assets(
        root_dir=tmp_path,
        dry_run=False,
        limit=3,
        db=test_db
    )
    assert res.limit == 3
    assert res.created == 3

# 16. Path traversal bloqueado
def test_path_traversal_blocked(tmp_path, test_db):
    # Try traversing with dotdot
    invalid_file = tmp_path / ".." / "traversal_attempt.mp4"
    try:
        invalid_file.write_bytes(b"evil")
    except Exception:
        pass

    candidates, invalid, conflicts = scan_catalog_files(tmp_path)
    for path, _ in candidates:
        assert ".." not in path
        assert not path.startswith("/")

# 18. URLs canónicas usan *definst*
def test_canonical_urls_use_definst(tmp_path, test_db):
    (tmp_path / "CANON-URL-01.mp4").write_bytes(b"dummy")

    res = import_catalog_cold_assets(root_dir=tmp_path, dry_run=False, db=test_db)
    assert res.created == 1

    asset = test_db.query(Asset).filter(Asset.enlace_id == "CANON-URL-01").first()
    assert asset.manifest_url == f"/EnlacePlus/*definst*/amlst:{str(asset.vod_uuid).upper()}/CANON-URL-01/manifest.m3u8"
    assert asset.manifest_path == f"EnlacePlus/_definst_/amlst:{str(asset.vod_uuid).upper()}/CANON-URL-01/manifest.m3u8"
