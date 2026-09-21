import os
import shutil
import pytest
import uuid
from pathlib import Path
from datetime import datetime, timezone

from redis import Redis
from rq import Queue

from src.core.database import SessionLocal, Base
from src.core.config import settings
from src.models.job import Job
from src.models.asset import Asset
from src.models.rendition import Rendition
from src.models.enums import JobStatus, VideoStatus, JobType
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging

FIXTURES_DIR = Path(__file__).parent / "fixtures"

SQLALCHEMY_DATABASE_URL = settings.DATABASE_URL.replace("5433/vod", "5433/vod_test")
engine = create_engine(SQLALCHEMY_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

log = logging.getLogger("test")

@pytest.mark.parametrize("fixture_name, expected_variants", [
    ("320x240.mp4", ["source"]),
    ("640x360.mp4", ["3"]),
    ("1280x720.mp4", ["1", "2", "3"]),
    ("1920x1080.mp4", ["0", "1", "2", "3"]),
    ("1080x1920.mp4", ["0", "1", "2", "3"]),
    ("1280x720-no-audio.mp4", ["1", "2", "3"]),
])
def test_transcode_worker_integration(tmp_path, db_session, monkeypatch, fixture_name, expected_variants):
    log.info(f"\n[TEST] Starting test_transcode_worker_integration for {fixture_name}")
    
    ingest = tmp_path / "input"
    ingest.mkdir()
    
    staging = tmp_path / "staging"
    staging.mkdir()
    
    output = tmp_path / "output"
    output.mkdir()
    
    processed = tmp_path / "processed"
    processed.mkdir()
    
    monkeypatch.setattr(settings, "INGEST_ROOT", str(ingest))
    monkeypatch.setattr(settings, "STAGING_ROOT", str(staging))
    monkeypatch.setattr(settings, "OUTPUT_ROOT", str(output))
    monkeypatch.setattr(settings, "PROCESSED_ROOT", str(processed))
    
    # Record existing file state in real storage/processed to ensure we don't modify it
    real_processed_file = Path("storage/processed") / fixture_name
    real_processed_mtime_before = real_processed_file.stat().st_mtime if real_processed_file.exists() else None
    
    # Copy the fixture to the temporary ingest root so it isn't destroyed by the archive step
    fixture_src = FIXTURES_DIR / fixture_name
    fixture_dst = ingest / fixture_name
    shutil.copy2(fixture_src, fixture_dst)
    
    asset_uuid = str(uuid.uuid4())
    enlace_id = f"test_{fixture_name.split('.')[0]}"
    
    db = db_session
    
    asset = Asset(
        vod_uuid=asset_uuid,
        enlace_id=enlace_id,
        source_uri=fixture_name,
        status=VideoStatus.CREATED
    )
    db.add(asset)
    db.commit()
    
    probe_job = Job(asset_id=asset.id, type=JobType.PROBE, status=JobStatus.PENDING)
    db.add(probe_job)
    db.commit()
    
    redis_conn = Redis.from_url(settings.REDIS_URL)
    q = Queue(name=settings.RQ_QUEUE_NAME, connection=redis_conn)
    
    rq_job_probe = q.enqueue("src.worker.tasks.probe_and_prepare_job", args=(probe_job.id,), job_id=str(probe_job.id))
    probe_job.rq_job_id = rq_job_probe.id
    db.commit()
    
    probe_job_id = probe_job.id
    asset_id = asset.id
    
    db_session.close()
    
    # Run PROBE
    with mock.patch("src.worker.tasks.SessionLocal", side_effect=TestingSessionLocal):
        from rq import SimpleWorker
        worker = SimpleWorker([q], connection=redis_conn)
        worker.work(burst=True)
        
    db_session = TestingSessionLocal()
    probe_job = db_session.query(Job).filter_by(id=probe_job_id).first()
    asset = db_session.query(Asset).filter_by(id=asset_id).first()
    
    assert probe_job.status == JobStatus.COMPLETED
    assert asset.source_width > 0
    assert asset.source_height > 0
    assert asset.source_fps > 0
    
    # Check that TRANSCODE job completed
    transcode_job = db_session.query(Job).filter_by(asset_id=asset.id, type=JobType.TRANSCODE).first()
        
    assert transcode_job is not None
    assert transcode_job.status == JobStatus.COMPLETED
    assert asset.status == VideoStatus.READY
    
    renditions = db_session.query(Rendition).filter_by(asset_id=asset.id).all()
    rendition_names = [r.name for r in renditions]
    assert set(rendition_names) == set(expected_variants)
    
    # Check paths
    uuid_upper = str(asset_uuid).upper()
    expected_path = f"EnlacePlus/_definst_/amlst:{uuid_upper}/{enlace_id}/manifest.m3u8"
    
    assert asset.manifest_path == expected_path
    assert asset.manifest_url.endswith(expected_path)
    
    # Verify file system
    final_output_dir = output / "EnlacePlus" / "_definst_" / f"amlst:{uuid_upper}" / enlace_id
    assert final_output_dir.exists()
    assert (final_output_dir / "manifest.m3u8").exists()
    
    for variant in expected_variants:
        var_dir = final_output_dir / variant
        assert var_dir.exists()
        assert (var_dir / "playlist.m3u8").exists()
        
    # Verify renditions
    renditions = db_session.query(Rendition).filter_by(asset_id=asset.id).all()
    assert len(renditions) == len(expected_variants)
    
    for r in renditions:
        assert r.name in expected_variants
        assert r.width > 0
        assert r.height > 0
        
    # Confirm archiving behavior
    assert fixture_src.exists()
    assert not fixture_dst.exists()
    assert (processed / fixture_name).exists()
    
    if real_processed_mtime_before is None:
        assert not real_processed_file.exists()
    else:
        assert real_processed_file.stat().st_mtime == real_processed_mtime_before
        
    db_session.close()
    redis_conn.close()
    engine.dispose()
