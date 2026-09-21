import os
import shutil
import time
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from redis import Redis
import rq

from src.core.database import SessionLocal, Base
from src.core.config import settings
from src.models.job import Job
from src.models.asset import Asset
from src.models.enums import JobStatus, VideoStatus, JobType
from src.scripts.reconcile_jobs import reconcile_jobs, reconcile_staging
from tests.conftest import TestingSessionLocal



def test_reconcile_pending_queued(db_session):
    db = db_session
    asset = Asset(vod_uuid="11111111-1111-1111-1111-111111111111", enlace_id="test_rq_1", source_uri="none", status=VideoStatus.CREATED)
    db.add(asset)
    db.commit()
    
    job = Job(asset_id=asset.id, type=JobType.PROBE, status=JobStatus.PENDING)
    db.add(job)
    db.commit()
    
    redis_conn = Redis.from_url(settings.REDIS_URL)
    q = rq.Queue(name=settings.RQ_QUEUE_NAME, connection=redis_conn)
    rq_job = q.enqueue("dummy", job_id=str(job.id))
    
    with patch('src.scripts.reconcile_jobs.SessionLocal', side_effect=TestingSessionLocal):
        reconcile_jobs()
    
    db.refresh(job)
    assert job.rq_job_id == rq_job.id
    assert job.status == JobStatus.PENDING

def test_reconcile_lost_redis(db_session):
    db = db_session
    asset = Asset(vod_uuid="22222222-2222-2222-2222-222222222222", enlace_id="test_rq_2", source_uri="none", status=VideoStatus.CREATED)
    db.add(asset)
    db.commit()
    
    job = Job(asset_id=asset.id, type=JobType.PROBE, status=JobStatus.PENDING)
    db.add(job)
    db.commit()
    
    with patch('src.scripts.reconcile_jobs.SessionLocal', side_effect=TestingSessionLocal):
        reconcile_jobs()
    
    db.refresh(job)
    assert job.rq_job_id is not None
    assert job.status == JobStatus.PENDING
    
    # Verify in redis
    redis_conn = Redis.from_url(settings.REDIS_URL)
    rq_job = rq.job.Job.fetch(str(job.id), connection=redis_conn)
    assert rq_job.is_queued

def test_reconcile_finished_invalid_asset(db_session):
    # Job finished but DB is out of sync, asset validation should fail
    db = db_session
    asset = Asset(vod_uuid="33333333-3333-3333-3333-333333333333", enlace_id="test_rq_3", source_uri="none", status=VideoStatus.PROBING)
    db.add(asset)
    db.commit()
    
    job = Job(asset_id=asset.id, type=JobType.PROBE, status=JobStatus.PROCESSING)
    db.add(job)
    db.commit()
    
    redis_conn = Redis.from_url(settings.REDIS_URL)
    q = rq.Queue(name=settings.RQ_QUEUE_NAME, connection=redis_conn)
    rq_job = q.enqueue("dummy", job_id=str(job.id))
    
    with patch('rq.job.Job.get_status', return_value='finished'), \
         patch('src.scripts.reconcile_jobs.SessionLocal', side_effect=TestingSessionLocal):
        reconcile_jobs()
        
    db.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.error_code == "E_JOB_RECONCILED"
    
    db.refresh(asset)
    assert asset.status == VideoStatus.FAILED
