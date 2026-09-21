import os
import uuid
import time
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest
from redis import Redis
from rq import Queue
from rq.job import JobStatus as RQJobStatus
from fastapi.testclient import TestClient

from src.main import app
from src.core.config import settings
from src.core.database import get_db
from src.core.queues import (
    QUEUE_LEGACY,
    QUEUE_PRIORITY,
    QUEUE_BATCH,
    QUEUE_INGEST,
    QUEUE_BACKUP,
    QUEUE_SUBTITLES,
    QUEUE_SYNC,
    ALL_QUEUES,
    TRANSCODE_QUEUES,
    POST_PROCESS_QUEUES,
    get_queue,
)
from src.models.asset import Asset
from src.models.job import Job
from src.models.ingest_item import IngestItem
from src.models.workflow_step import AssetWorkflowStep
from src.models.transcript import AssetTranscript, AssetSubtitleTrack
from src.models.enums import (
    VideoStatus,
    JobType,
    JobStatus,
    IngestStatus,
    WorkflowStepType,
    WorkflowStepStatus,
)
from src.services.new_video_scanner import (
    scan_new_videos,
    compute_source_fingerprint,
    ScanResult,
)
from src.services.metadata.static_provider import StaticMetadataProvider
from src.services.metadata.base import NewVideoMetadata
from src.services.backup.local_provider import LocalBackupProvider, BackupResult
from src.services.subtitles.service import SubtitleService
from src.services.subtitles.manifest_updater import update_master_manifest_with_subtitles
from src.services.sync.static_provider import StaticEnlaceSyncProvider
from src.services.workflow.orchestrator import schedule_post_ready_work
from src.services.workflow.reconciler import reconcile_workflow_steps
from src.scripts.workflow_retry import retry_workflow_step
from src.services.job_dispatch import (
    dispatch_progressive_job,
    promote_pending_job_to_priority,
    promote_batch_to_priority,
)
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
    for q_name in ALL_QUEUES:
        conn.delete(f"rq:queue:{q_name}")
    yield conn
    for q_name in ALL_QUEUES:
        conn.delete(f"rq:queue:{q_name}")


@pytest.fixture
def ingest_folder(tmp_path):
    folder = tmp_path / "new_ingest_root"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


# =========================================================================
# 1. QUEUE ARCHITECTURE & WORKER ISOLATION
# =========================================================================

def test_queue_constants_and_isolation():
    assert QUEUE_INGEST == "vod_ingest"
    assert QUEUE_BACKUP == "vod_backup"
    assert QUEUE_SUBTITLES == "vod_subtitles"
    assert QUEUE_SYNC == "vod_sync"

    assert QUEUE_INGEST in ALL_QUEUES
    assert QUEUE_BACKUP in ALL_QUEUES
    assert QUEUE_SUBTITLES in ALL_QUEUES
    assert QUEUE_SYNC in ALL_QUEUES

    assert QUEUE_INGEST in TRANSCODE_QUEUES
    assert QUEUE_BACKUP in POST_PROCESS_QUEUES
    assert QUEUE_SUBTITLES in POST_PROCESS_QUEUES
    assert QUEUE_SYNC in POST_PROCESS_QUEUES

    # Post process queues must never be in transcode queues
    for q in POST_PROCESS_QUEUES:
        assert q not in TRANSCODE_QUEUES


# =========================================================================
# 2. FILE STABILITY: REAL OBSERVATION WINDOW
# =========================================================================

def test_observation_window_first_scan_creates_waiting_stable(test_db, ingest_folder):
    video = ingest_folder / "prog01.mp4"
    video.write_bytes(b"x" * 1024)

    result = scan_new_videos(
        root_path=str(ingest_folder),
        stable_seconds=5,
        db=test_db,
        redis_conn=None
    )

    assert result.scanned == 1
    assert result.waiting_stable == 1
    assert result.dispatched == 0

    item = test_db.query(IngestItem).filter(IngestItem.filename == "prog01.mp4").first()
    assert item is not None
    assert item.status == IngestStatus.WAITING_STABLE
    assert item.size_bytes == 1024
    assert item.first_observed_at is not None
    assert item.last_observed_at is not None
    assert item.stable_at is None


def test_observation_window_second_scan_before_window_remains_waiting(test_db, ingest_folder):
    video = ingest_folder / "prog01.mp4"
    video.write_bytes(b"x" * 1024)

    scan_new_videos(root_path=str(ingest_folder), stable_seconds=100, db=test_db, redis_conn=None)

    # Immediate second scan (well before 100s)
    result2 = scan_new_videos(root_path=str(ingest_folder), stable_seconds=100, db=test_db, redis_conn=None)
    assert result2.waiting_stable == 1
    assert result2.dispatched == 0

    item = test_db.query(IngestItem).filter(IngestItem.filename == "prog01.mp4").first()
    assert item.status == IngestStatus.WAITING_STABLE


def test_observation_window_size_change_resets_window(test_db, ingest_folder):
    video = ingest_folder / "growing.mp4"
    video.write_bytes(b"initial content")

    scan_new_videos(root_path=str(ingest_folder), stable_seconds=2, db=test_db, redis_conn=None)

    item1 = test_db.query(IngestItem).filter(IngestItem.filename == "growing.mp4").first()
    obs1 = item1.last_observed_at

    # Simulate time passing, but file grew
    time.sleep(0.05)
    video.write_bytes(b"initial content plus appended bytes while downloading")

    scan_new_videos(root_path=str(ingest_folder), stable_seconds=2, db=test_db, redis_conn=None)
    test_db.refresh(item1)
    assert item1.status == IngestStatus.WAITING_STABLE
    assert item1.size_bytes > 15
    assert item1.last_observed_at > obs1


def test_observation_window_stable_observation_advances(test_db, ingest_folder, redis_conn):
    video = ingest_folder / "stable.mp4"
    video.write_bytes(b"completed file content")

    meta_prov = StaticMetadataProvider()
    meta_prov.register("stable.mp4", NewVideoMetadata(enlace_id="STABLE-001", title="Stable Video"))

    scan_new_videos(
        root_path=str(ingest_folder),
        stable_seconds=2,
        metadata_provider=meta_prov,
        db=test_db,
        redis_conn=redis_conn
    )

    item = test_db.query(IngestItem).filter(IngestItem.filename == "stable.mp4").first()
    assert item.status == IngestStatus.WAITING_STABLE

    # Manually age the first_observed_at to simulate 5 seconds having passed
    item.first_observed_at = datetime.now(timezone.utc) - timedelta(seconds=10)
    test_db.commit()

    # Second scan -> file unchanged, window satisfied -> advances to STABLE / DISPATCHED
    result = scan_new_videos(
        root_path=str(ingest_folder),
        stable_seconds=2,
        metadata_provider=meta_prov,
        db=test_db,
        redis_conn=redis_conn
    )
    assert result.dispatched == 1

    test_db.refresh(item)
    assert item.status == IngestStatus.DISPATCHED
    assert item.stable_at is not None
    assert item.asset_id is not None


# =========================================================================
# 3. LIGHTWEIGHT FINGERPRINTING & IDEMPOTENCY
# =========================================================================

def test_lightweight_fingerprint_formula():
    fp1 = compute_source_fingerprint("folder/vid.mp4", 1000, 1600000000.0)
    fp2 = compute_source_fingerprint("folder/vid.mp4", 1000, 1600000000.0)
    assert fp1 == fp2
    assert len(fp1) == 64  # SHA-256 hex string

    # Different size changes fingerprint
    fp3 = compute_source_fingerprint("folder/vid.mp4", 1001, 1600000000.0)
    assert fp1 != fp3


def test_repeat_scans_never_duplicate_ingest_item_or_job(test_db, ingest_folder, redis_conn):
    video = ingest_folder / "unique01.mp4"
    video.write_bytes(b"constant video data")

    meta_prov = StaticMetadataProvider()
    meta_prov.register("unique01.mp4", NewVideoMetadata(enlace_id="UNIQUE-001", title="Unique Test"))

    # First scan: first observation creates WAITING_STABLE
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)
    # Second scan: window satisfied (0 seconds) -> dispatches
    r1 = scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)
    assert r1.dispatched == 1

    # Third scan -> already registered/dispatched, counts as dispatched no-op
    r2 = scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)
    assert r2.dispatched == 1

    # Ensure DB has exactly 1 IngestItem, 1 Asset, 1 Job
    assert test_db.query(IngestItem).count() == 1
    assert test_db.query(Asset).count() == 1
    assert test_db.query(Job).count() == 1

    # Ensure Redis has exactly 1 job in vod_ingest
    q = get_queue(QUEUE_INGEST, connection=redis_conn)
    assert q.count == 1


def test_dispatched_file_changed_on_disk_marks_source_changed(test_db, ingest_folder, redis_conn):
    video = ingest_folder / "tampered.mp4"
    video.write_bytes(b"version 1")

    meta_prov = StaticMetadataProvider()
    meta_prov.register("tampered.mp4", NewVideoMetadata(enlace_id="TAMPER-001", title="Tampered"))

    # First scan -> WAITING_STABLE
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)
    # Second scan -> DISPATCHED
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)

    item = test_db.query(IngestItem).filter(IngestItem.filename == "tampered.mp4").first()
    assert item.status == IngestStatus.DISPATCHED

    # Now modify file on disk
    time.sleep(0.01)
    video.write_bytes(b"version 2 modified")

    r = scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)
    assert r.conflicts == 1

    changed_item = test_db.query(IngestItem).filter(
        IngestItem.filename == "tampered.mp4",
        IngestItem.status == IngestStatus.SOURCE_CHANGED
    ).first()
    assert changed_item is not None


# =========================================================================
# 4. SECURITY & FILE FILTERING
# =========================================================================

def test_scanner_rejects_symlinks(test_db, ingest_folder):
    target = ingest_folder / "real_file.mp4"
    target.write_bytes(b"content")

    symlink = ingest_folder / "symlink.mp4"
    symlink.symlink_to(target)

    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, db=test_db)

    # Only real_file.mp4 should be observed, symlink.mp4 rejected
    items = test_db.query(IngestItem).all()
    filenames = [i.filename for i in items]
    assert "real_file.mp4" in filenames
    assert "symlink.mp4" not in filenames


def test_scanner_ignores_hidden_and_temp_files(test_db, ingest_folder):
    (ingest_folder / ".hidden.mp4").write_bytes(b"hidden")
    (ingest_folder / "upload.mp4.part").write_bytes(b"part")
    (ingest_folder / "upload.mp4.tmp").write_bytes(b"tmp")
    (ingest_folder / "valid.mp4").write_bytes(b"valid")

    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, db=test_db)

    items = test_db.query(IngestItem).all()
    assert len(items) == 1
    assert items[0].filename == "valid.mp4"


# =========================================================================
# 5. METADATA PROVIDER & CONFLICTS
# =========================================================================

def test_missing_metadata_stays_metadata_pending(test_db, ingest_folder):
    video = ingest_folder / "nometadata.mp4"
    video.write_bytes(b"video content")

    mock_meta_provider = MagicMock()
    mock_meta_provider.get_metadata.return_value = None

    # First scan -> WAITING_STABLE
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=mock_meta_provider, db=test_db)
    # Second scan -> stable, but metadata missing -> METADATA_PENDING
    r = scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=mock_meta_provider, db=test_db)
    assert r.metadata_pending == 1

    item = test_db.query(IngestItem).filter(IngestItem.filename == "nometadata.mp4").first()
    assert item.status == IngestStatus.METADATA_PENDING
    assert item.asset_id is None


def test_invalid_enlace_id_marks_failed(test_db, ingest_folder):
    video = ingest_folder / "bad_id.mp4"
    video.write_bytes(b"content")

    mock_meta_provider = MagicMock()
    mock_meta_provider.get_metadata.return_value = NewVideoMetadata(
        enlace_id="invalid/slash/id",
        title="Bad ID Video"
    )

    # First scan -> WAITING_STABLE
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=mock_meta_provider, db=test_db)
    # Second scan -> FAILED due to bad ID
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=mock_meta_provider, db=test_db)

    item = test_db.query(IngestItem).filter(IngestItem.filename == "bad_id.mp4").first()
    assert item.status == IngestStatus.FAILED
    assert "Invalid enlace_id" in item.last_error


def test_duplicate_enlace_id_different_source_marks_conflict(test_db, ingest_folder, redis_conn):
    # Pre-existing asset with enlace_id 'DUPLICATE-ID'
    existing = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="DUPLICATE-ID",
        source_uri="some_other_folder/old.mp4",
        status=VideoStatus.READY
    )
    test_db.add(existing)
    test_db.commit()

    video = ingest_folder / "new_dup.mp4"
    video.write_bytes(b"content")

    mock_meta_provider = MagicMock()
    mock_meta_provider.get_metadata.return_value = NewVideoMetadata(
        enlace_id="DUPLICATE-ID",
        title="Duplicate"
    )

    # First scan -> WAITING_STABLE
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=mock_meta_provider, db=test_db, redis_conn=redis_conn)
    # Second scan -> CONFLICT
    r = scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=mock_meta_provider, db=test_db, redis_conn=redis_conn)
    assert r.conflicts == 1

    item = test_db.query(IngestItem).filter(IngestItem.filename == "new_dup.mp4").first()
    assert item.status == IngestStatus.CONFLICT
    assert "Conflict" in item.last_error


# =========================================================================
# 6. ASSET CREATION & DISPATCH TO vod_ingest
# =========================================================================

def test_asset_created_as_cold_with_canonical_url_and_dispatched_to_vod_ingest(test_db, ingest_folder, redis_conn):
    video = ingest_folder / "brand_new.mp4"
    video.write_bytes(b"content")

    meta_prov = StaticMetadataProvider()
    meta_prov.register("brand_new.mp4", NewVideoMetadata(enlace_id="BRAND-NEW-01", title="Brand New"))

    # Scan 1: WAITING_STABLE
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)
    # Scan 2: DISPATCHED
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)

    item = test_db.query(IngestItem).filter(IngestItem.filename == "brand_new.mp4").first()
    assert item.status == IngestStatus.DISPATCHED
    assert item.asset_id is not None

    asset = test_db.query(Asset).filter(Asset.id == item.asset_id).first()
    assert asset is not None
    assert asset.status == VideoStatus.QUEUED
    assert "EnlacePlus/_definst_/amlst:" in asset.manifest_path
    assert f"/{asset.enlace_id}/manifest.m3u8" in asset.manifest_path
    assert "EnlacePlus/*definst*/amlst:" in asset.manifest_url

    job = test_db.query(Job).filter(Job.asset_id == asset.id).first()
    assert job is not None
    assert job.queue_name == QUEUE_INGEST
    assert job.status == JobStatus.PENDING

    # Redis job in vod_ingest
    q = get_queue(QUEUE_INGEST, connection=redis_conn)
    rq_job = q.fetch_job(job.rq_job_id)
    assert rq_job is not None
    assert rq_job.origin == QUEUE_INGEST


# =========================================================================
# 7. PROMOTION & REUSE: vod_ingest -> vod_priority
# =========================================================================

def test_pending_job_in_vod_ingest_promoted_to_vod_priority(test_db, ingest_folder, redis_conn):
    video = ingest_folder / "promote_me.mp4"
    video.write_bytes(b"content")

    meta_prov = StaticMetadataProvider()
    meta_prov.register("promote_me.mp4", NewVideoMetadata(enlace_id="PROMOTE-001", title="Promote Me"))

    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)

    item = test_db.query(IngestItem).filter(IngestItem.filename == "promote_me.mp4").first()
    asset = test_db.query(Asset).filter(Asset.id == item.asset_id).first()
    job = test_db.query(Job).filter(Job.asset_id == asset.id).first()

    q_ingest = get_queue(QUEUE_INGEST, connection=redis_conn)
    q_prio = get_queue(QUEUE_PRIORITY, connection=redis_conn)
    assert q_ingest.count == 1
    assert q_prio.count == 0

    # User requests playback via prepare-playback endpoint
    resp = client.post(f"/api/v1/assets/{asset.vod_uuid}/prepare-playback")
    assert resp.status_code == 200

    test_db.refresh(job)
    assert job.queue_name == QUEUE_PRIORITY
    assert job.status == JobStatus.PENDING

    assert q_ingest.count == 0
    assert q_prio.count == 1


def test_processing_job_in_vod_ingest_reuses_execution(test_db, ingest_folder, redis_conn):
    video = ingest_folder / "reuse_me.mp4"
    video.write_bytes(b"content")

    meta_prov = StaticMetadataProvider()
    meta_prov.register("reuse_me.mp4", NewVideoMetadata(enlace_id="REUSE-001", title="Reuse Me"))

    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)
    scan_new_videos(root_path=str(ingest_folder), stable_seconds=0, metadata_provider=meta_prov, db=test_db, redis_conn=redis_conn)

    item = test_db.query(IngestItem).filter(IngestItem.filename == "reuse_me.mp4").first()
    asset = test_db.query(Asset).filter(Asset.id == item.asset_id).first()
    job = test_db.query(Job).filter(Job.asset_id == asset.id).first()

    # Simulate worker started processing the job
    job.status = JobStatus.PROCESSING
    asset.status = VideoStatus.PROCESSING
    test_db.commit()

    q_ingest = get_queue(QUEUE_INGEST, connection=redis_conn)
    rq_job = q_ingest.fetch_job(job.rq_job_id)
    rq_job.set_status(RQJobStatus.STARTED)

    # User requests playback -> must reuse ongoing execution without launching new transcode
    resp = client.post(f"/api/v1/assets/{asset.vod_uuid}/prepare-playback")
    assert resp.status_code == 200

    # Ensure no second job was created
    all_jobs = test_db.query(Job).filter(Job.asset_id == asset.id).all()
    assert len(all_jobs) == 1
    assert all_jobs[0].status == JobStatus.PROCESSING


# =========================================================================
# 8. POST-READY WORKFLOW ORCHESTRATION & FAILURE INDEPENDENCE
# =========================================================================

def test_post_ready_workflow_steps_scheduled_only_for_new_pipeline(test_db, redis_conn):
    # 1. Historical asset (no IngestItem)
    historical_asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="HISTORICAL-001",
        source_uri="cold/hist.mp4",
        status=VideoStatus.READY
    )
    test_db.add(historical_asset)
    test_db.commit()

    steps_hist = schedule_post_ready_work(historical_asset, db=test_db, redis_conn=redis_conn)
    assert len(steps_hist) == 0

    # 2. New pipeline asset (has IngestItem)
    new_asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="NEW-001",
        source_uri="new_videos/new.mp4",
        status=VideoStatus.READY
    )
    test_db.add(new_asset)
    test_db.flush()

    ingest_item = IngestItem(
        relative_path="new.mp4",
        filename="new.mp4",
        size_bytes=5000,
        mtime=1600000000.0,
        source_fingerprint="abc123hash",
        status=IngestStatus.DISPATCHED,
        asset_id=new_asset.id
    )
    test_db.add(ingest_item)
    test_db.commit()

    steps_new = schedule_post_ready_work(new_asset, db=test_db, redis_conn=redis_conn)
    assert len(steps_new) == 3

    steps = test_db.query(AssetWorkflowStep).filter(AssetWorkflowStep.asset_id == new_asset.id).all()
    step_types = [s.step_type for s in steps]
    assert WorkflowStepType.AZURE_BACKUP in step_types
    assert WorkflowStepType.SUBTITLES in step_types
    assert WorkflowStepType.ENLACE_SYNC in step_types

    # Verify each queued in its respective dedicated queue
    q_backup = get_queue(QUEUE_BACKUP, connection=redis_conn)
    q_subtitles = get_queue(QUEUE_SUBTITLES, connection=redis_conn)
    q_sync = get_queue(QUEUE_SYNC, connection=redis_conn)
    assert q_backup.count == 1
    assert q_subtitles.count == 1
    assert q_sync.count == 1


def test_workflow_step_failure_does_not_affect_asset_ready_status(test_db):
    asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="FAILURE-TEST",
        source_uri="new_videos/test.mp4",
        status=VideoStatus.READY
    )
    test_db.add(asset)
    test_db.commit()

    step = AssetWorkflowStep(
        asset_id=asset.id,
        step_type=WorkflowStepType.AZURE_BACKUP,
        status=WorkflowStepStatus.PROCESSING,
        queue_name=QUEUE_BACKUP
    )
    test_db.add(step)
    test_db.commit()

    # Step fails
    step.status = WorkflowStepStatus.FAILED
    step.last_error = "Azure connection failed"
    test_db.commit()

    # Asset status must remain READY!
    test_db.refresh(asset)
    assert asset.status == VideoStatus.READY


# =========================================================================
# 9. RECONCILER & CLI RETRY
# =========================================================================

def test_reconciler_recovers_missing_post_ready_steps(test_db, redis_conn):
    asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="RECON-001",
        source_uri="new_videos/recon.mp4",
        status=VideoStatus.READY
    )
    test_db.add(asset)
    test_db.flush()

    ingest_item = IngestItem(
        relative_path="recon.mp4",
        filename="recon.mp4",
        size_bytes=1000,
        mtime=1600000000.0,
        source_fingerprint="fp123",
        status=IngestStatus.DISPATCHED,
        asset_id=asset.id
    )
    test_db.add(ingest_item)
    test_db.commit()

    # System crashed right after READY before scheduling steps
    reconciled_count = reconcile_workflow_steps(db=test_db, redis_conn=redis_conn)
    assert reconciled_count >= 1

    steps = test_db.query(AssetWorkflowStep).filter(AssetWorkflowStep.asset_id == asset.id).all()
    assert len(steps) == 3


def test_workflow_retry_cli_logic_retries_only_failed_step(test_db, redis_conn):
    asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="RETRY-001",
        source_uri="new_videos/retry.mp4",
        status=VideoStatus.READY
    )
    test_db.add(asset)
    test_db.flush()

    step_backup = AssetWorkflowStep(
        asset_id=asset.id,
        step_type=WorkflowStepType.AZURE_BACKUP,
        status=WorkflowStepStatus.COMPLETED,
        queue_name=QUEUE_BACKUP
    )
    step_sub = AssetWorkflowStep(
        asset_id=asset.id,
        step_type=WorkflowStepType.SUBTITLES,
        status=WorkflowStepStatus.FAILED,
        last_error="Temporary timeout",
        queue_name=QUEUE_SUBTITLES
    )
    test_db.add_all([step_backup, step_sub])
    test_db.commit()

    res = retry_workflow_step(
        asset_identifier=str(asset.vod_uuid),
        step_type_str="SUBTITLES",
        db=test_db,
        redis_conn=redis_conn
    )
    assert res["status"] == "RETRIED"

    test_db.refresh(step_backup)
    test_db.refresh(step_sub)
    assert step_backup.status == WorkflowStepStatus.COMPLETED  # Unchanged
    assert step_sub.status == WorkflowStepStatus.QUEUED
    assert step_sub.attempt_count == 1

    q_sub = get_queue(QUEUE_SUBTITLES, connection=redis_conn)
    assert q_sub.count == 1


# =========================================================================
# 10. SUBTITLE SERVICE & MANIFEST UPDATER
# =========================================================================

def test_subtitle_sidecars_and_manifest_atomicity(tmp_path):
    # Create dummy master playlist
    master_content = (
        "#EXTM3U\n"
        "#EXT-X-VERSION:3\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080\n"
        "1080p/index.m3u8\n"
    )
    manifest_file = tmp_path / "index.m3u8"
    manifest_file.write_text(master_content)

    update_master_manifest_with_subtitles(
        master_manifest_path=manifest_file,
        languages=["es", "en"],
        default_lang="es",
    )

    new_content = manifest_file.read_text()
    assert 'TYPE=SUBTITLES,GROUP-ID="subs",NAME="Español",DEFAULT=YES' in new_content
    assert 'TYPE=SUBTITLES,GROUP-ID="subs",NAME="English"' in new_content
    assert 'SUBTITLES="subs"' in new_content


# =========================================================================
# 11. ORIGINAL FILE PRESERVATION & DRY RUN
# =========================================================================

def test_original_file_preserved_never_deleted(test_db, ingest_folder, redis_conn, tmp_path):
    video = ingest_folder / "preserve_me.mp4"
    video.write_bytes(b"valuable original recording")

    backup_dir = tmp_path / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="PRESERVE-001",
        source_uri="preserve_me.mp4",
        status=VideoStatus.READY
    )

    provider = LocalBackupProvider(backup_dir=str(backup_dir))
    backup_result = provider.backup_original(asset=asset, source_path=video)

    # Both original and backup must exist with identical content
    assert video.exists()
    backup_file_path = Path(backup_result.metadata["backup_path"])
    assert backup_file_path.exists()
    assert video.read_bytes() == backup_file_path.read_bytes()


def test_scanner_dry_run_makes_zero_db_or_redis_writes(test_db, ingest_folder, redis_conn):
    (ingest_folder / "dry01.mp4").write_bytes(b"content 1")
    (ingest_folder / "dry02.mp4").write_bytes(b"content 2")

    result = scan_new_videos(
        root_path=str(ingest_folder),
        stable_seconds=0,
        dry_run=True,
        db=test_db,
        redis_conn=redis_conn
    )

    assert result.scanned == 2

    # Verify zero records in DB and Redis
    assert test_db.query(IngestItem).count() == 0
    assert test_db.query(Asset).count() == 0
    assert test_db.query(Job).count() == 0
    for q_name in ALL_QUEUES:
        assert get_queue(q_name, connection=redis_conn).count == 0


# =========================================================================
# 12. WORKER TASK EXECUTION & ISOLATION
# =========================================================================

@patch("src.worker.workflow_tasks.SessionLocal", side_effect=TestingSessionLocal)
def test_execute_workflow_step_backup_task(mock_session, test_db, tmp_path):
    from src.worker.workflow_tasks import execute_workflow_step_job

    video = tmp_path / "vid_backup.mp4"
    video.write_bytes(b"content to backup")

    asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="BACKUP-EXEC-01",
        source_uri=str(video),
        status=VideoStatus.READY
    )
    test_db.add(asset)
    test_db.flush()

    step = AssetWorkflowStep(
        asset_id=asset.id,
        step_type=WorkflowStepType.AZURE_BACKUP,
        status=WorkflowStepStatus.QUEUED,
        queue_name=QUEUE_BACKUP
    )
    test_db.add(step)
    test_db.commit()

    execute_workflow_step_job(step.id)

    test_db.expire_all()
    step_after = test_db.query(AssetWorkflowStep).filter(AssetWorkflowStep.id == step.id).first()
    assert step_after.status == WorkflowStepStatus.COMPLETED
    assert step_after.metadata_json is not None
    assert "backup_uri" in step_after.metadata_json

    # Asset must remain READY
    asset_after = test_db.query(Asset).filter(Asset.id == asset.id).first()
    assert asset_after.status == VideoStatus.READY


@patch("src.worker.workflow_tasks.SessionLocal", side_effect=TestingSessionLocal)
def test_execute_workflow_step_subtitles_task(mock_session, test_db, tmp_path):
    from src.worker.workflow_tasks import execute_workflow_step_job

    asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="SUB-EXEC-01",
        source_uri="dummy.mp4",
        status=VideoStatus.READY
    )
    test_db.add(asset)
    test_db.flush()

    step = AssetWorkflowStep(
        asset_id=asset.id,
        step_type=WorkflowStepType.SUBTITLES,
        status=WorkflowStepStatus.QUEUED,
        queue_name=QUEUE_SUBTITLES
    )
    test_db.add(step)
    test_db.commit()

    execute_workflow_step_job(step.id)

    test_db.expire_all()
    step_after = test_db.query(AssetWorkflowStep).filter(AssetWorkflowStep.id == step.id).first()
    assert step_after.status == WorkflowStepStatus.COMPLETED

    # Check that AssetTranscript and AssetSubtitleTrack records were created
    transcripts = test_db.query(AssetTranscript).filter(AssetTranscript.asset_id == asset.id).all()
    assert len(transcripts) >= 1

    tracks = test_db.query(AssetSubtitleTrack).filter(AssetSubtitleTrack.asset_id == asset.id).all()
    assert len(tracks) >= 1
    languages = [t.language for t in tracks]
    assert "es" in languages

    # Asset must remain READY
    asset_after = test_db.query(Asset).filter(Asset.id == asset.id).first()
    assert asset_after.status == VideoStatus.READY


@patch("src.worker.workflow_tasks.SessionLocal", side_effect=TestingSessionLocal)
def test_execute_workflow_step_sync_task(mock_session, test_db):
    from src.worker.workflow_tasks import execute_workflow_step_job

    asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="SYNC-EXEC-01",
        source_uri="dummy.mp4",
        status=VideoStatus.READY
    )
    test_db.add(asset)
    test_db.flush()

    step = AssetWorkflowStep(
        asset_id=asset.id,
        step_type=WorkflowStepType.ENLACE_SYNC,
        status=WorkflowStepStatus.QUEUED,
        queue_name=QUEUE_SYNC
    )
    test_db.add(step)
    test_db.commit()

    execute_workflow_step_job(step.id)

    test_db.expire_all()
    step_after = test_db.query(AssetWorkflowStep).filter(AssetWorkflowStep.id == step.id).first()
    assert step_after.status == WorkflowStepStatus.COMPLETED
    assert step_after.metadata_json is not None
    assert step_after.metadata_json.get("status") == "SYNCED"

    # Asset must remain READY
    asset_after = test_db.query(Asset).filter(Asset.id == asset.id).first()
    assert asset_after.status == VideoStatus.READY


@patch("src.worker.workflow_tasks.SessionLocal", side_effect=TestingSessionLocal)
def test_execute_workflow_step_failure_records_failed_without_affecting_asset(mock_session, test_db):
    from src.worker.workflow_tasks import execute_workflow_step_job

    asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id="FAIL-EXEC-01",
        source_uri="dummy.mp4",
        status=VideoStatus.READY
    )
    test_db.add(asset)
    test_db.flush()

    step = AssetWorkflowStep(
        asset_id=asset.id,
        step_type=WorkflowStepType.ENLACE_SYNC,
        status=WorkflowStepStatus.QUEUED,
        queue_name=QUEUE_SYNC
    )
    test_db.add(step)
    test_db.commit()

    # Simulate sync error
    with patch("src.worker.workflow_tasks.get_sync_provider") as mock_get_sync:
        mock_provider = MagicMock()
        mock_provider.sync_ready_asset.side_effect = RuntimeError("External API timeout")
        mock_get_sync.return_value = mock_provider

        execute_workflow_step_job(step.id)

    test_db.expire_all()
    step_after = test_db.query(AssetWorkflowStep).filter(AssetWorkflowStep.id == step.id).first()
    assert step_after.status == WorkflowStepStatus.FAILED
    assert "External API timeout" in step_after.last_error

    # Asset must unconditionally remain READY
    asset_after = test_db.query(Asset).filter(Asset.id == asset.id).first()
    assert asset_after.status == VideoStatus.READY

