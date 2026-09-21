import pytest
import os
import time
from pathlib import Path
from unittest import mock
from src.scripts.run_worker import run
from src.scripts.reconcile_jobs import reconcile_staging
from src.core.config import settings
from src.models.job import Job
from src.models.asset import Asset
from src.models.enums import JobStatus, VideoStatus, JobType
import src

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

SQLALCHEMY_DATABASE_URL = settings.DATABASE_URL.rsplit("/", 1)[0] + "/vod_test"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)



def test_run_worker_importable():
    assert run is not None

@mock.patch("src.scripts.run_worker.Worker")
@mock.patch("src.scripts.run_worker.Redis")
def test_run_worker_execution(mock_redis, mock_worker):
    mock_redis_instance = mock.MagicMock()
    mock_redis.from_url.return_value = mock_redis_instance
    mock_worker_instance = mock.MagicMock()
    mock_worker.return_value = mock_worker_instance

    run(burst=True)

    mock_redis.from_url.assert_called_once()
    mock_worker.assert_called_once()
    mock_worker_instance.work.assert_called_once_with(with_scheduler=True, burst=True)

def test_reconcile_staging_clean(tmp_path):
    old_root = settings.STAGING_ROOT
    old_age = settings.STAGING_ORPHAN_AGE_SECONDS
    
    settings.STAGING_ROOT = str(tmp_path)
    settings.STAGING_ORPHAN_AGE_SECONDS = 0  # instantly orphan
    
    jobs_dir = tmp_path / "jobs"
    assets_dir = tmp_path / "assets"
    jobs_dir.mkdir()
    assets_dir.mkdir()
    
    # 1. Job Active
    job1_dir = jobs_dir / "11111111-1111-1111-1111-111111111111"
    job1_dir.mkdir()
    
    db = TestingSessionLocal()
    asset = Asset(vod_uuid="22222222-2222-2222-2222-222222222222", enlace_id="test_stg_1", source_uri="none", status=VideoStatus.CREATED)
    db.add(asset)
    db.commit()
    
    job1 = Job(id="11111111-1111-1111-1111-111111111111", asset_id=asset.id, type=JobType.PROBE, status=JobStatus.PENDING)
    db.add(job1)
    db.commit()
    
    # 2. Job Terminal
    job2_dir = jobs_dir / "33333333-3333-3333-3333-333333333333"
    job2_dir.mkdir()
    job2 = Job(id="33333333-3333-3333-3333-333333333333", asset_id=asset.id, type=JobType.PROBE, status=JobStatus.FAILED)
    db.add(job2)
    db.commit()
    
    # 3. Referenced Asset
    asset1_dir = assets_dir / "22222222-2222-2222-2222-222222222222"
    asset1_dir.mkdir()
    (asset1_dir / "source.bin").write_text("data")
    asset.staged_source_path = "assets/22222222-2222-2222-2222-222222222222/source.bin"
    db.commit()
    
    # 4. Unreferenced Asset
    asset2_dir = assets_dir / "44444444-4444-4444-4444-444444444444"
    asset2_dir.mkdir()
    
    with mock.patch("src.scripts.reconcile_jobs.SessionLocal", side_effect=TestingSessionLocal):
        # Dry run
        reconcile_staging(dry_run=True)
        assert job1_dir.exists()
        assert job2_dir.exists()
        assert asset1_dir.exists()
        assert asset2_dir.exists()
        
        # Clean run
        reconcile_staging(dry_run=False)
    
    # Pending job should survive
    assert job1_dir.exists()
    # Failed job should be deleted
    assert not job2_dir.exists()
    # Referenced asset should survive
    assert asset1_dir.exists()
    # Unreferenced asset should be deleted
    assert not asset2_dir.exists()
    
    settings.STAGING_ROOT = old_root
    settings.STAGING_ORPHAN_AGE_SECONDS = old_age
