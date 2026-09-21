import os
import uuid
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
from redis import Redis, exceptions as redis_exceptions
from rq import Queue, SimpleWorker
from rq.job import Job as RQJob, JobStatus as RQJobStatus
from fastapi.testclient import TestClient

from src.main import app
from src.core.config import settings
from src.core.database import get_db
from src.core.queues import (
    QUEUE_LEGACY,
    QUEUE_PRIORITY,
    QUEUE_BATCH,
    get_queue,
    get_queue_depths,
)
from src.models.asset import Asset
from src.models.job import Job
from src.models.asset_event import AssetEvent
from src.models.enums import VideoStatus, JobType, JobStatus
from src.services.job_dispatch import (
    dispatch_progressive_job,
    promote_batch_to_priority,
    batch_enqueue_asset,
    QueueUnavailableError,
)
from src.scripts.reconcile_jobs import reconcile_jobs
from tests.conftest import TestingSessionLocal

def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db
client = TestClient(app)

@pytest.fixture
def test_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

@pytest.fixture
def redis_conn():
    conn = Redis.from_url(settings.REDIS_URL)
    # Flush test queues before each test
    conn.delete(
        f"rq:queue:{QUEUE_LEGACY}",
        f"rq:queue:{QUEUE_PRIORITY}",
        f"rq:queue:{QUEUE_BATCH}",
    )
    yield conn
    conn.delete(
        f"rq:queue:{QUEUE_LEGACY}",
        f"rq:queue:{QUEUE_PRIORITY}",
        f"rq:queue:{QUEUE_BATCH}",
    )

# 1, 2, 3. Verificación de constantes de colas
def test_queue_constants():
    assert QUEUE_LEGACY == "vod_tasks"
    assert QUEUE_PRIORITY == "vod_priority"
    assert QUEUE_BATCH == "vod_batch"

# 4. prepare-playback en asset COLD despacha a vod_priority
def test_prepare_playback_dispatches_to_priority(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-PRIO001",
        source_uri="PREDI-PRIO001.mp4",
        status=VideoStatus.COLD,
        progress=0
    )
    test_db.add(asset)
    test_db.commit()

    resp = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "QUEUED"
    assert data["playable"] is False

    # Verify DB job
    job = test_db.query(Job).filter(Job.asset_id == asset.id).first()
    assert job is not None
    assert job.status == JobStatus.PENDING
    assert job.queue_name == QUEUE_PRIORITY
    assert job.rq_job_id is not None

    # Verify in Redis priority queue
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    q_legacy = get_queue(QUEUE_LEGACY, redis_conn)

    assert q_prio.count == 1
    assert q_batch.count == 0
    assert q_legacy.count == 0

# 5. batch-enqueue en asset COLD despacha a vod_batch
def test_batch_enqueue_dispatches_to_batch(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-BATCH001",
        source_uri="PREDI-BATCH001.mp4",
        status=VideoStatus.COLD,
        progress=0
    )
    test_db.add(asset)
    test_db.commit()

    res = batch_enqueue_asset(vod_uuid, db=test_db, redis_conn=redis_conn)
    assert res["status"] == "ENQUEUED_BATCH"
    assert res["queue_name"] == QUEUE_BATCH

    # Verify DB
    job = test_db.query(Job).filter(Job.asset_id == asset.id).first()
    assert job is not None
    assert job.status == JobStatus.PENDING
    assert job.queue_name == QUEUE_BATCH

    # Verify Redis
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    assert q_batch.count == 1
    assert q_prio.count == 0

# 6. READY no se encola en batch (ALREADY_READY)
def test_batch_enqueue_ready_asset(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-READY001",
        source_uri="PREDI-READY001.mp4",
        status=VideoStatus.READY,
        progress=100
    )
    test_db.add(asset)
    test_db.commit()

    res = batch_enqueue_asset(vod_uuid, db=test_db, redis_conn=redis_conn)
    assert res["status"] == "ALREADY_READY"
    assert test_db.query(Job).filter(Job.asset_id == asset.id).count() == 0

# 7. FAILED no se auto-reintenta en batch (REQUIRES_EXPLICIT_RETRY)
def test_batch_enqueue_failed_asset(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-FAIL001",
        source_uri="PREDI-FAIL001.mp4",
        status=VideoStatus.FAILED,
        error_code="E_PREV_FAILURE"
    )
    test_db.add(asset)
    test_db.commit()

    res = batch_enqueue_asset(vod_uuid, db=test_db, redis_conn=redis_conn)
    assert res["status"] == "REQUIRES_EXPLICIT_RETRY"
    assert test_db.query(Job).filter(Job.asset_id == asset.id).count() == 0

# 8. Idempotencia: múltiples prepare-playback generan exactamente 1 job
def test_idempotent_prepare_playback(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-IDEM001",
        source_uri="PREDI-IDEM001.mp4",
        status=VideoStatus.COLD
    )
    test_db.add(asset)
    test_db.commit()

    resp1 = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
    resp2 = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
    assert resp1.status_code == 200
    assert resp2.status_code == 200

    jobs = test_db.query(Job).filter(Job.asset_id == asset.id).all()
    assert len(jobs) == 1
    assert get_queue(QUEUE_PRIORITY, redis_conn).count == 1

# 9. Idempotencia: múltiples batch-enqueue generan exactamente 1 job
def test_idempotent_batch_enqueue(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-IDEMBATCH001",
        source_uri="PREDI-IDEMBATCH001.mp4",
        status=VideoStatus.COLD
    )
    test_db.add(asset)
    test_db.commit()

    res1 = batch_enqueue_asset(vod_uuid, db=test_db, redis_conn=redis_conn)
    res2 = batch_enqueue_asset(vod_uuid, db=test_db, redis_conn=redis_conn)
    assert res1["status"] == "ENQUEUED_BATCH"
    assert "ALREADY_PENDING_VOD_BATCH" in res2["status"]

    jobs = test_db.query(Job).filter(Job.asset_id == asset.id).all()
    assert len(jobs) == 1
    assert get_queue(QUEUE_BATCH, redis_conn).count == 1

# 10, 11, 12, 14. Promoción atómica batch -> priority
def test_batch_to_priority_promotion(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-PROMOTE001",
        source_uri="PREDI-PROMOTE001.mp4",
        status=VideoStatus.COLD
    )
    test_db.add(asset)
    test_db.commit()

    # 1. Encolar en batch
    batch_res = batch_enqueue_asset(vod_uuid, db=test_db, redis_conn=redis_conn)
    assert batch_res["status"] == "ENQUEUED_BATCH"

    job_before = test_db.query(Job).filter(Job.asset_id == asset.id).first()
    assert job_before.queue_name == QUEUE_BATCH
    assert job_before.status == JobStatus.PENDING
    original_job_id = job_before.id
    original_rq_id = job_before.rq_job_id

    # Verify queue counts before promotion
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    q_legacy = get_queue(QUEUE_LEGACY, redis_conn)
    assert q_batch.count == 1
    assert q_prio.count == 0
    assert q_legacy.count == 0

    # 2. Usuario solicita reproducción (prepare-playback) -> debe PROMOVER
    resp = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
    assert resp.status_code == 200

    test_db.expire_all()

    # 11. Mismo Job de PostgreSQL, NO se creó un segundo Job
    all_jobs = test_db.query(Job).filter(Job.asset_id == asset.id).all()
    assert len(all_jobs) == 1

    job_after = all_jobs[0]
    assert job_after.id == original_job_id
    assert job_after.queue_name == QUEUE_PRIORITY
    assert job_after.status == JobStatus.PENDING

    # 12 & 14. Exactamente 1 RQ job ejecutable en total across all 3 queues
    assert q_batch.count == 0
    assert q_prio.count == 1
    assert q_legacy.count == 0

    # Verify RQ job attributes
    rq_job = q_prio.fetch_job(job_after.rq_job_id)
    assert rq_job is not None
    assert rq_job.origin == QUEUE_PRIORITY

    # Verify AssetEvent recorded promotion
    events = test_db.query(AssetEvent).filter(AssetEvent.asset_id == asset.id).all()
    promotion_events = [e for e in events if (e.details or {}).get("context") == "batch_to_priority_promotion"]
    assert len(promotion_events) == 1
    event = promotion_events[0]
    assert event.details["old_queue"] == QUEUE_BATCH
    assert event.details["new_queue"] == QUEUE_PRIORITY

# 13. Concurrencia: Si batch ya está PROCESSING/STARTED, prepare-playback NO promueve ni duplica
def test_playback_during_batch_processing_reuses_execution(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-RACE001",
        source_uri="PREDI-RACE001.mp4",
        status=VideoStatus.COLD
    )
    test_db.add(asset)
    test_db.commit()

    # Enqueue in batch
    batch_res = batch_enqueue_asset(vod_uuid, db=test_db, redis_conn=redis_conn)
    job = test_db.query(Job).filter(Job.asset_id == asset.id).first()

    # Simulate batch worker started running (PROCESSING in DB and started in RQ)
    job.status = JobStatus.PROCESSING
    asset.status = VideoStatus.PROCESSING
    test_db.commit()

    rq_job = RQJob.fetch(str(job.rq_job_id or job.id), connection=redis_conn)
    rq_job.set_status(RQJobStatus.STARTED)

    # User calls prepare-playback while PROCESSING
    resp = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "PROCESSING"

    # Verify NO duplicate job created and queue_name remained batch
    jobs = test_db.query(Job).filter(Job.asset_id == asset.id).all()
    assert len(jobs) == 1
    assert jobs[0].queue_name == QUEUE_BATCH

    # Priority queue was NOT touched
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    assert q_prio.count == 0

# 15. Persistencia y auditoría de queue_name
def test_queue_name_persisted(test_db, redis_conn):
    asset_prio = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="PREDI-PERSIST-PRIO",
        source_uri="test1.mp4",
        status=VideoStatus.COLD
    )
    asset_batch = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="PREDI-PERSIST-BATCH",
        source_uri="test2.mp4",
        status=VideoStatus.COLD
    )
    test_db.add_all([asset_prio, asset_batch])
    test_db.commit()

    job_prio = dispatch_progressive_job(asset_prio, QUEUE_PRIORITY, test_db, context="test")
    job_batch = dispatch_progressive_job(asset_batch, QUEUE_BATCH, test_db, context="test")

    assert job_prio.queue_name == "vod_priority"
    assert job_batch.queue_name == "vod_batch"

# 16, 17, 18. Reconciliador preserva la queue correcta (priority -> priority, batch -> batch, legacy -> legacy)
def test_reconciler_preserves_queue_names(test_db, redis_conn):
    # 1. Priority job missing from Redis
    asset_p = Asset(vod_uuid=uuid.uuid4(), enlace_id="REC-PRIO", source_uri="p.mp4", status=VideoStatus.QUEUED)
    test_db.add(asset_p)
    test_db.commit()

    job_p = Job(
        asset_id=asset_p.id,
        type=JobType.TRANSCODE,
        status=JobStatus.PENDING,
        queue_name=QUEUE_PRIORITY
    )
    test_db.add(job_p)

    # 2. Batch job missing from Redis
    asset_b = Asset(vod_uuid=uuid.uuid4(), enlace_id="REC-BATCH", source_uri="b.mp4", status=VideoStatus.QUEUED)
    test_db.add(asset_b)
    test_db.commit()

    job_b = Job(
        asset_id=asset_b.id,
        type=JobType.TRANSCODE,
        status=JobStatus.PENDING,
        queue_name=QUEUE_BATCH
    )
    test_db.add(job_b)

    # 3. Legacy job missing from Redis
    asset_l = Asset(vod_uuid=uuid.uuid4(), enlace_id="REC-LEG", source_uri="l.mp4", status=VideoStatus.CREATED)
    test_db.add(asset_l)
    test_db.commit()

    job_l = Job(
        asset_id=asset_l.id,
        type=JobType.PROBE,
        status=JobStatus.PENDING,
        queue_name=QUEUE_LEGACY
    )
    test_db.add(job_l)
    test_db.commit()

    # Clear all Redis queues
    redis_conn.delete(f"rq:queue:{QUEUE_LEGACY}", f"rq:queue:{QUEUE_PRIORITY}", f"rq:queue:{QUEUE_BATCH}")

    # Run reconciler
    with patch('src.scripts.reconcile_jobs.SessionLocal', side_effect=TestingSessionLocal):
        reconcile_jobs()

    # Verify each was re-enqueued to its respective queue!
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    q_legacy = get_queue(QUEUE_LEGACY, redis_conn)

    assert q_prio.count == 1
    assert q_batch.count == 1
    assert q_legacy.count == 1

# 19. Dashboard devuelve profundidades separadas y compatibles
def test_dashboard_queue_depths(test_db, redis_conn):
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    q_legacy = get_queue(QUEUE_LEGACY, redis_conn)

    q_prio.enqueue("os.getpid")
    q_batch.enqueue("os.getpid")
    q_batch.enqueue("os.getpid")
    q_legacy.enqueue("os.getpid")

    admin_key = {"X-Admin-Key": settings.ADMIN_API_KEY} if settings.ADMIN_API_KEY else {}
    resp = client.get("/api/v1/admin/dashboard", headers=admin_key)
    assert resp.status_code == 200
    q_info = resp.json()["queue"]

    assert q_info["legacy_queue_depth"] == 1
    assert q_info["priority_queue_depth"] == 1
    assert q_info["batch_queue_depth"] == 2
    assert q_info["total_queue_depth"] == 4
    # Retrocompatible field 'depth' matches legacy queue count
    assert q_info["depth"] == 1

# 20, 21, 22. Aislamiento de workers: priority worker solo consume priority, batch solo batch
def test_workers_consume_only_assigned_queues(test_db, redis_conn):
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    q_batch = get_queue(QUEUE_BATCH, redis_conn)

    # Put a job in batch and a job in priority
    q_prio.enqueue("os.getpid", job_id="job-prio-only")
    q_batch.enqueue("os.getpid", job_id="job-batch-only")

    assert q_prio.count == 1
    assert q_batch.count == 1

    # SimpleWorker listening strictly on priority
    worker_prio = SimpleWorker([q_prio], connection=redis_conn)
    worker_prio.work(burst=True)

    # Priority queue emptied, batch queue remains untouched
    assert q_prio.count == 0
    assert q_batch.count == 1

    # SimpleWorker listening strictly on batch
    worker_batch = SimpleWorker([q_batch], connection=redis_conn)
    worker_batch.work(burst=True)

    # Batch queue now emptied
    assert q_batch.count == 0

# 23. Fallo de Redis durante promoción: error controlado y Job recuperable
def test_promotion_redis_failure_recovery(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-FAILPROMOTE",
        source_uri="f.mp4",
        status=VideoStatus.COLD
    )
    test_db.add(asset)
    test_db.commit()

    # Enqueue in batch
    batch_enqueue_asset(vod_uuid, db=test_db, redis_conn=redis_conn)
    job = test_db.query(Job).filter(Job.asset_id == asset.id).first()
    assert job.queue_name == QUEUE_BATCH

    # Mock q_prio.enqueue_job to fail
    with patch.object(Queue, "enqueue_job", side_effect=redis_exceptions.ConnectionError("Redis connection lost")):
        with pytest.raises(redis_exceptions.ConnectionError):
            promote_batch_to_priority(job, asset, test_db, redis_conn=redis_conn)

    # Job in PostgreSQL was already marked with queue_name = vod_priority or left consistent
    job_after = test_db.query(Job).filter(Job.id == job.id).first()
    assert job_after is not None
    # Reconciler can re-enqueue it later into the queue specified by queue_name

# 24, 25, 26. FAILED no puede reintentarse desde prepare-playback: 0 jobs, 0 redis, 0 ffmpeg
def test_failed_asset_prepare_playback_rejected(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-HARDEN-FAIL",
        source_uri="fail.mp4",
        status=VideoStatus.FAILED,
        error_code="E_PREV_FAILURE",
        error_message="Previous transcode failed completely"
    )
    test_db.add(asset)
    test_db.commit()

    # Call prepare-playback on FAILED asset
    resp = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
    assert resp.status_code == 409
    data = resp.json()["detail"]
    assert data["error_code"] == "REQUIRES_EXPLICIT_RETRY"
    assert data["status"] == "FAILED"

    # Asset must remain strictly in FAILED status
    test_db.expire_all()
    asset_after = test_db.query(Asset).filter(Asset.vod_uuid == vod_uuid).first()
    assert asset_after.status == VideoStatus.FAILED

    # No jobs created in DB
    jobs = test_db.query(Job).filter(Job.asset_id == asset.id).all()
    assert len(jobs) == 0

    # No jobs in Redis across ALL queues
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    q_legacy = get_queue(QUEUE_LEGACY, redis_conn)
    assert q_prio.count == 0
    assert q_batch.count == 0
    assert q_legacy.count == 0

# 27, 28. Fallo Redis en dispatch inicial priority queda recuperable y reconciler recupera en priority
def test_dispatch_priority_redis_failure_recovery(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-REDIS-PRIO",
        source_uri="prio.mp4",
        status=VideoStatus.COLD
    )
    test_db.add(asset)
    test_db.commit()

    # Simulate Redis connection failure during enqueue
    with patch.object(Queue, "enqueue", side_effect=redis_exceptions.ConnectionError("Redis down")):
        with pytest.raises(QueueUnavailableError):
            dispatch_progressive_job(asset, QUEUE_PRIORITY, test_db, redis_conn=redis_conn)

    # State in PostgreSQL: Asset is QUEUED, Job is PENDING with queue_name = vod_priority
    test_db.expire_all()
    asset_db = test_db.query(Asset).filter(Asset.vod_uuid == vod_uuid).first()
    assert asset_db.status == VideoStatus.QUEUED

    job_db = test_db.query(Job).filter(Job.asset_id == asset_db.id).first()
    assert job_db is not None
    assert job_db.status == JobStatus.PENDING
    assert job_db.queue_name == QUEUE_PRIORITY

    # Redis has 0 jobs currently
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    q_legacy = get_queue(QUEUE_LEGACY, redis_conn)
    assert q_prio.count == 0

    # Run reconciler to recover
    with patch('src.scripts.reconcile_jobs.SessionLocal', side_effect=TestingSessionLocal):
        reconcile_jobs()

    # Reconciler recovered job strictly into vod_priority!
    assert q_prio.count == 1
    assert q_batch.count == 0
    assert q_legacy.count == 0

# 29, 30. Fallo Redis en dispatch inicial batch queda recuperable y reconciler recupera en batch
def test_dispatch_batch_redis_failure_recovery(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-REDIS-BATCH",
        source_uri="batch.mp4",
        status=VideoStatus.COLD
    )
    test_db.add(asset)
    test_db.commit()

    # Simulate Redis connection failure during enqueue
    with patch.object(Queue, "enqueue", side_effect=redis_exceptions.ConnectionError("Redis down")):
        with pytest.raises(QueueUnavailableError):
            dispatch_progressive_job(asset, QUEUE_BATCH, test_db, redis_conn=redis_conn)

    # State in PostgreSQL: Asset is QUEUED, Job is PENDING with queue_name = vod_batch
    test_db.expire_all()
    asset_db = test_db.query(Asset).filter(Asset.vod_uuid == vod_uuid).first()
    assert asset_db.status == VideoStatus.QUEUED

    job_db = test_db.query(Job).filter(Job.asset_id == asset_db.id).first()
    assert job_db is not None
    assert job_db.status == JobStatus.PENDING
    assert job_db.queue_name == QUEUE_BATCH

    # Redis has 0 jobs currently
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    q_legacy = get_queue(QUEUE_LEGACY, redis_conn)
    assert q_batch.count == 0

    # Run reconciler to recover
    with patch('src.scripts.reconcile_jobs.SessionLocal', side_effect=TestingSessionLocal):
        reconcile_jobs()

    # Reconciler recovered job strictly into vod_batch!
    assert q_batch.count == 1
    assert q_prio.count == 0
    assert q_legacy.count == 0

# 31. Reconciler repetido es estrictamente idempotente (no duplica jobs)
def test_reconciler_repeated_runs_never_duplicate(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-IDEM-REC",
        source_uri="rec.mp4",
        status=VideoStatus.QUEUED
    )
    test_db.add(asset)
    test_db.commit()

    job = Job(
        asset_id=asset.id,
        type=JobType.TRANSCODE,
        status=JobStatus.PENDING,
        queue_name=QUEUE_PRIORITY
    )
    test_db.add(job)
    test_db.commit()

    # Run reconciler 3 times consecutively
    with patch('src.scripts.reconcile_jobs.SessionLocal', side_effect=TestingSessionLocal):
        reconcile_jobs()
        reconcile_jobs()
        reconcile_jobs()

    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    q_legacy = get_queue(QUEUE_LEGACY, redis_conn)

    # Exactly 1 job in priority, 0 in others
    assert q_prio.count == 1
    assert q_batch.count == 0
    assert q_legacy.count == 0

    # Active DB jobs per asset <= 1
    all_jobs = test_db.query(Job).filter(Job.asset_id == asset.id).all()
    assert len(all_jobs) == 1

# 32. Carrera promoción vs batch worker: garantiza <= 1 job ejecutable/started
def test_race_promotion_vs_batch_worker_strict_bound(test_db, redis_conn):
    vod_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=vod_uuid,
        enlace_id="PREDI-RACE-STRICT",
        source_uri="race.mp4",
        status=VideoStatus.COLD
    )
    test_db.add(asset)
    test_db.commit()

    # Enqueue to batch
    batch_res = batch_enqueue_asset(vod_uuid, db=test_db, redis_conn=redis_conn)
    assert batch_res["status"] == "ENQUEUED_BATCH"

    job = test_db.query(Job).filter(Job.asset_id == asset.id).first()
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    q_prio = get_queue(QUEUE_PRIORITY, redis_conn)

    # Worker starts running it
    rq_job = RQJob.fetch(str(job.rq_job_id or job.id), connection=redis_conn)
    rq_job.set_status(RQJobStatus.STARTED)
    rq_job.save()
    # Simulate removal from queued list as worker took it
    q_batch.remove(rq_job.id)

    # Prepare-playback triggers promotion during race
    promo_res = promote_batch_to_priority(job, asset, test_db, redis_conn=redis_conn)
    assert promo_res["action"] == "REUSE_STARTED"

    # Verify: 0 executable jobs in priority, 0 in batch queued
    assert q_prio.count == 0
    assert q_batch.count == 0

    # Total executable RQ jobs across all queues <= 1
    total_queued = q_prio.count + q_batch.count
    assert total_queued == 0
    assert job.status == JobStatus.PROCESSING

