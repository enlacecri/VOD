import os
import shutil
import pytest
from unittest import mock
from pathlib import Path
from sqlalchemy.orm import Session

from src.models.asset import Asset
from src.models.enums import VideoStatus
from src.worker.archive import archive_source_file, compute_sha256
from src.models.asset_event import AssetEvent

@pytest.fixture
def mock_settings(tmp_path, monkeypatch):
    ingest = tmp_path / "ingest"
    processed = tmp_path / "processed"
    ingest.mkdir()
    processed.mkdir()
    monkeypatch.setattr("src.worker.archive.settings.INGEST_ROOT", str(ingest))
    monkeypatch.setattr("src.worker.archive.settings.PROCESSED_ROOT", str(processed))
    return ingest, processed

@pytest.fixture
def setup_asset(mock_settings, db_session: Session):
    ingest, processed = mock_settings
    
    file_content = b"test video data"
    test_file = ingest / "subdir" / "test.mp4"
    test_file.parent.mkdir()
    test_file.write_bytes(file_content)
    
    sha256 = compute_sha256(str(test_file))
    
    import uuid
    asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="TEST-ARCHIVE",
        source_uri="subdir/test.mp4",
        status=VideoStatus.READY,
        size=len(file_content),
        source_sha256=sha256
    )
    db_session.add(asset)
    db_session.commit()
    
    return ingest, processed, asset, test_file

def test_archive_success_same_filesystem(db_session, setup_asset):
    ingest, processed, asset, test_file = setup_asset
    
    archive_source_file(db_session, asset)
    db_session.commit()
    
    assert asset.processed_source_path == "subdir/test.mp4"
    assert not test_file.exists()
    
    dst_file = processed / "subdir" / "test.mp4"
    assert dst_file.exists()
    assert dst_file.read_bytes() == b"test video data"
    
    events = db_session.query(AssetEvent).filter_by(asset_id=asset.id).all()
    assert len(events) == 0

def test_archive_different_filesystem_success(db_session, setup_asset, monkeypatch):
    ingest, processed, asset, test_file = setup_asset
    
    # Mock os.stat to simulate different filesystems
    original_stat = os.stat
    class FakeStat:
        def __init__(self, st, new_dev):
            for k in dir(st):
                if not k.startswith('_'):
                    setattr(self, k, getattr(st, k))
            self.st_dev = new_dev

    def mock_stat(path, *args, **kwargs):
        st = original_stat(path, *args, **kwargs)
        if "ingest" in str(path):
            return FakeStat(st, 1)
        if "processed" in str(path):
            return FakeStat(st, 2)
        return st
        
    monkeypatch.setattr("src.worker.archive.os.stat", mock_stat)
    
    archive_source_file(db_session, asset)
    db_session.commit()
    
    if asset.processed_source_path is None:
        event = db_session.query(AssetEvent).filter_by(asset_id=asset.id).first()
        if event:
            print(f"FAILED WITH EVENT: {event.details}")
            
    assert asset.processed_source_path == "subdir/test.mp4"
    assert not test_file.exists()
    
    dst_file = processed / "subdir" / "test.mp4"
    assert dst_file.exists()

def test_archive_source_modified_size(db_session, setup_asset):
    ingest, processed, asset, test_file = setup_asset
    
    # Modify size
    test_file.write_bytes(b"test video data modified")
    
    archive_source_file(db_session, asset)
    db_session.commit()
    
    assert asset.processed_source_path is None
    assert test_file.exists()
    
    event = db_session.query(AssetEvent).filter_by(asset_id=asset.id).first()
    assert event is not None
    assert event.details["error_code"] == "E_SOURCE_ARCHIVE_FAILED"
    assert "size changed" in event.details["error_message"]

def test_archive_source_modified_hash(db_session, setup_asset):
    ingest, processed, asset, test_file = setup_asset
    
    # Modify content but keep same size
    test_file.write_bytes(b"test video alt!")
    
    archive_source_file(db_session, asset)
    db_session.commit()
    
    assert asset.processed_source_path is None
    assert test_file.exists()
    
    event = db_session.query(AssetEvent).filter_by(asset_id=asset.id).first()
    assert event is not None
    assert "SHA-256 changed" in event.details["error_message"]

def test_archive_destination_exists_match(db_session, setup_asset):
    ingest, processed, asset, test_file = setup_asset
    
    dst_file = processed / "subdir" / "test.mp4"
    dst_file.parent.mkdir()
    dst_file.write_bytes(b"test video data") # Exact match
    
    archive_source_file(db_session, asset)
    db_session.commit()
    
    assert asset.processed_source_path == "subdir/test.mp4"
    assert not test_file.exists()

def test_archive_destination_exists_mismatch(db_session, setup_asset):
    ingest, processed, asset, test_file = setup_asset
    
    dst_file = processed / "subdir" / "test.mp4"
    dst_file.parent.mkdir()
    dst_file.write_bytes(b"different data")
    
    archive_source_file(db_session, asset)
    db_session.commit()
    
    assert asset.processed_source_path is None
    assert test_file.exists()
    
    event = db_session.query(AssetEvent).filter_by(asset_id=asset.id).first()
    assert "different size/hash" in event.details["error_message"]

def test_archive_idempotent_source_missing_dest_match(db_session, setup_asset):
    ingest, processed, asset, test_file = setup_asset
    
    # Simulate moved but DB commit failed
    dst_file = processed / "subdir" / "test.mp4"
    dst_file.parent.mkdir()
    shutil.move(str(test_file), str(dst_file))
    
    assert not test_file.exists()
    
    archive_source_file(db_session, asset)
    db_session.commit()
    
    assert asset.processed_source_path == "subdir/test.mp4"
    assert dst_file.exists()

def test_archive_symlink_in_path(db_session, setup_asset):
    ingest, processed, asset, test_file = setup_asset
    
    # Create a symlink
    symlink_dir = ingest / "symlink_dir"
    symlink_dir.symlink_to(ingest / "subdir")
    
    asset.source_uri = "symlink_dir/test.mp4"
    
    archive_source_file(db_session, asset)
    db_session.commit()
    
    assert asset.processed_source_path is None
    event = db_session.query(AssetEvent).filter_by(asset_id=asset.id).first()
    assert "contains symlinks" in event.details["error_message"]

def test_archive_missing_hash(db_session, setup_asset):
    ingest, processed, asset, test_file = setup_asset
    
    asset.source_sha256 = None
    
    archive_source_file(db_session, asset)
    db_session.commit()
    
    assert asset.processed_source_path is None
    event = db_session.query(AssetEvent).filter_by(asset_id=asset.id).first()
    assert "SHA-256 changed" in event.details["error_message"]
