import os
import shutil
import pytest
from pathlib import Path
from datetime import datetime, timezone
import hashlib

from redis import Redis
from rq import Queue, Worker, SimpleWorker

from src.core.database import SessionLocal, Base
from src.core.config import settings
from src.models.job import Job
from src.models.asset import Asset
from src.models.enums import JobStatus, VideoStatus, JobType
from src.scripts.reconcile_jobs import reconcile_jobs
from unittest import mock

import src

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Setup test database
SQLALCHEMY_DATABASE_URL = settings.DATABASE_URL.replace("5433/vod", "5433/vod_test")
from sqlalchemy.pool import NullPool
engine = create_engine(SQLALCHEMY_DATABASE_URL, poolclass=NullPool)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)



def test_real_worker_integration(tmp_path):
    # Setup test file in ingest
    ingest = tmp_path / "input"
    ingest.mkdir()
    settings.INGEST_ROOT = str(ingest)
    
    staging = tmp_path / "staging"
    staging.mkdir()
    settings.STAGING_ROOT = str(staging)
    
    output = tmp_path / "output"
    output.mkdir()
    settings.OUTPUT_ROOT = str(output)
    
    test_file = ingest / "source.mp4"
    shutil.copy2(FIXTURES_DIR / "valid.mp4", test_file)
    
    db = TestingSessionLocal()
    
    asset = Asset(vod_uuid="44444444-4444-4444-4444-444444444444", enlace_id="test_int_1", source_uri="source.mp4", status=VideoStatus.CREATED)
    db.add(asset)
    db.commit()
    
    job = Job(asset_id=asset.id, type=JobType.PROBE, status=JobStatus.PENDING)
    db.add(job)
    db.commit()
    
    redis_conn = Redis.from_url(settings.REDIS_URL)
    q = Queue(name=settings.RQ_QUEUE_NAME, connection=redis_conn)
    rq_job = q.enqueue("src.worker.tasks.probe_and_prepare_job", args=(job.id,), job_id=str(job.id))
    
    job.rq_job_id = rq_job.id
    db.commit()
    
    job_id = job.id
    asset_id = asset.id
    
    with mock.patch("src.scripts.reconcile_jobs.SessionLocal", side_effect=TestingSessionLocal), \
         mock.patch("src.worker.tasks.SessionLocal", side_effect=TestingSessionLocal):
        
        # Run the worker in burst mode
        from rq import Worker
        worker = Worker([q], connection=redis_conn)
        worker.work(burst=True)
    
    # Reload from DB
    db = TestingSessionLocal()
    job = db.query(Job).filter_by(id=job_id).first()
    asset = db.query(Asset).filter_by(id=asset_id).first()
    
    # Check outcomes
    if asset.status != VideoStatus.QUEUED:
        print(f"!!! ASSET FAILED !!! Error Code: {asset.error_code} Message: {asset.error_message}")
    
    assert job.status == JobStatus.COMPLETED
    assert job.worker_id is not None
    assert job.started_at is not None
    assert job.heartbeat is not None
    assert job.finished_at is not None

    # Wait, the burst worker will process ALL jobs in the queue.
    # Since PROBE enqueues TRANSCODE, it will process TRANSCODE as well!
    # Let's check the transcode job and final asset status.
    assert asset.status == VideoStatus.READY
    
    transcode_job = db.query(Job).filter_by(asset_id=asset.id, type=JobType.TRANSCODE).first()
    assert transcode_job is not None
    assert transcode_job.status == JobStatus.COMPLETED
    assert asset.source_sha256 is not None
    assert asset.size > 0
    assert asset.staged_source_path is not None
    
    # Check file
    stable_file = staging / asset.staged_source_path
    assert stable_file.exists()
    assert stable_file.is_file()
    
    # Recalculate hash
    hasher = hashlib.sha256()
    with open(stable_file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    
    assert hasher.hexdigest() == asset.source_sha256
    
    # Check relative path
    assert asset.staged_source_path == f"assets/{asset.vod_uuid}/source.bin"
    
    db.close()
    redis_conn.close()
    engine.dispose()
