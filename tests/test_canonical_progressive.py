import os
import uuid
import shutil
import time
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import urllib.request
from fastapi.testclient import TestClient
from redis import Redis
from rq import Queue

from src.main import app
from src.core.config import settings
from src.core.database import get_db
from src.core.canonical import (
    normalize_vod_uuid,
    build_canonical_manifest_path,
    build_canonical_manifest_url,
    get_canonical_output_dir
)
from src.models.asset import Asset
from src.models.job import Job
from src.models.enums import VideoStatus, JobStatus, JobType
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
def mock_ingest(tmp_path):
    old_root = settings.INGEST_ROOT
    settings.INGEST_ROOT = str(tmp_path)
    
    # Copy or create a valid input video
    fixture_valid = Path(__file__).parent / "fixtures" / "valid.mp4"
    if fixture_valid.exists():
        shutil.copy(fixture_valid, tmp_path / "valid.mp4")
        shutil.copy(fixture_valid, tmp_path / "valid2.mp4")
    else:
        (tmp_path / "valid.mp4").write_bytes(b"dummy")
        (tmp_path / "valid2.mp4").write_bytes(b"dummy")
        
    yield tmp_path
    settings.INGEST_ROOT = old_root

# 1. Canonical manifest path format
def test_canonical_manifest_path_format():
    test_uuid = uuid.UUID("217cbed8-667b-4a9b-b000-d3003160b0c5")
    enlace_id = "PREDI-VICTO89"
    path = build_canonical_manifest_path(test_uuid, enlace_id)
    expected = "EnlacePlus/_definst_/amlst:217CBED8-667B-4A9B-B000-D3003160B0C5/PREDI-VICTO89/manifest.m3u8"
    assert path == expected
    assert "_definst_" in path
    assert "*definst*" not in path

    output_dir = get_canonical_output_dir(settings.OUTPUT_ROOT, test_uuid, enlace_id)
    assert "_definst_" in str(output_dir)

# 2. Canonical manifest URL format
def test_canonical_manifest_url_format():
    test_uuid = uuid.UUID("217cbed8-667b-4a9b-b000-d3003160b0c5")
    enlace_id = "PREDI-VICTO89"
    url = build_canonical_manifest_url(test_uuid, enlace_id)
    expected = "/EnlacePlus/*definst*/amlst:217CBED8-667B-4A9B-B000-D3003160B0C5/PREDI-VICTO89/manifest.m3u8"
    assert url == expected
    assert "*definst*" in url
    assert "_definst_" not in url

# 3. UUID case normalization (lowercase -> uppercase)
def test_uuid_case_normalization():
    lower_str = "217cbed8-667b-4a9b-b000-d3003160b0c5"
    normalized = normalize_vod_uuid(lower_str)
    assert normalized == "217CBED8-667B-4A9B-B000-D3003160B0C5"
    
    url = build_canonical_manifest_url(lower_str, "TEST-ID")
    assert "217CBED8-667B-4A9B-B000-D3003160B0C5" in url
    
    path = build_canonical_manifest_path(lower_str, "TEST-ID")
    assert "217CBED8-667B-4A9B-B000-D3003160B0C5" in path

# 4. Asset registration COLD
def test_create_cold_asset(mock_ingest):
    res = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "PREDI-BAYLE539-TEST4", "source_uri": "valid.mp4"}
    )
    assert res.status_code == 201
    data = res.json()
    assert data["enlace_id"] == "PREDI-BAYLE539-TEST4"
    assert data["status"] == "COLD"
    assert data["playable"] is False
    assert data["progress"] == 0
    assert data["available_until_seconds"] is None
    assert "/EnlacePlus/*definst*/amlst:" in data["manifest_url"]
    assert "EnlacePlus/_definst_/amlst:" in data["manifest_path"]

# 5. Asset registration idempotency
def test_cold_asset_idempotent(mock_ingest):
    res1 = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "IDEMP-COLD-01", "source_uri": "valid.mp4"}
    )
    assert res1.status_code == 201
    uuid1 = res1.json()["vod_uuid"]

    res2 = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "IDEMP-COLD-01", "source_uri": "valid.mp4"}
    )
    assert res2.status_code == 200
    data2 = res2.json()
    assert data2["vod_uuid"] == uuid1
    assert data2["was_reused"] is True
    assert data2["status"] == "COLD"

# 6. Asset registration conflict (different source_uri)
def test_cold_asset_conflict(mock_ingest):
    res1 = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "CONFLICT-COLD-01", "source_uri": "valid.mp4"}
    )
    assert res1.status_code == 201

    res2 = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "CONFLICT-COLD-01", "source_uri": "valid2.mp4"}
    )
    assert res2.status_code == 409

# 7. Concurrency guarantee on prepare-playback
def test_prepare_playback_concurrency(mock_ingest):
    res = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "CONCUR-01", "source_uri": "valid.mp4"}
    )
    vod_uuid = res.json()["vod_uuid"]

    results = []
    def call_prepare():
        db_session = TestingSessionLocal()
        try:
            r = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
            results.append(r)
        finally:
            db_session.close()

    # Launch 5 concurrent threads
    threads = [threading.Thread(target=call_prepare) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Verify all returned HTTP 200
    for r in results:
        assert r.status_code == 200
        assert r.json()["status"] == "QUEUED"

    # Verify in DB: exactly 1 transcode job was created
    db = TestingSessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.vod_uuid == uuid.UUID(vod_uuid)).first()
        jobs = db.query(Job).filter(Job.asset_id == asset.id, Job.type == JobType.TRANSCODE).all()
        assert len(jobs) == 1, f"Expected exactly 1 job, got {len(jobs)}"
    finally:
        db.close()

# 8. Prepare-playback from COLD
def test_prepare_playback_from_cold(mock_ingest):
    res = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "PREPARE-COLD-01", "source_uri": "valid.mp4"}
    )
    vod_uuid = res.json()["vod_uuid"]

    prep = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
    assert prep.status_code == 200
    data = prep.json()
    assert data["status"] == "QUEUED"
    assert data["playable"] is False
    assert data["manifest_url"] == res.json()["manifest_url"]

# 9. Prepare-playback from READY does not re-enqueue
def test_prepare_playback_from_ready(mock_ingest):
    res = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "READY-TEST-01", "source_uri": "valid.mp4"}
    )
    vod_uuid = res.json()["vod_uuid"]

    # Manually transition asset to READY
    db = TestingSessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.vod_uuid == uuid.UUID(vod_uuid)).first()
        asset.status = VideoStatus.READY
        asset.progress = 100
        asset.duration_seconds = 150.0
        asset.available_until_seconds = 150.0
        db.commit()
    finally:
        db.close()

    prep = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
    assert prep.status_code == 200
    data = prep.json()
    assert data["status"] == "READY"
    assert data["playable"] is True
    assert data["progress"] == 100
    assert data["available_until_seconds"] == 150.0

    # Verify no new job was created
    db = TestingSessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.vod_uuid == uuid.UUID(vod_uuid)).first()
        jobs = db.query(Job).filter(Job.asset_id == asset.id).all()
        assert len(jobs) == 0
    finally:
        db.close()

# 10. PLAYABLE threshold detection
def test_playable_threshold_detection(tmp_path):
    from src.worker.progressive_manager import parse_variant_playlist
    sample_content = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:7
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:6.000000,
seg_000.ts
#EXTINF:6.000000,
seg_001.ts
#EXTINF:6.000000,
seg_002.ts
#EXTINF:6.000000,
seg_003.ts
#EXTINF:6.000000,
seg_004.ts
"""
    m3u8_file = tmp_path / "playlist.m3u8"
    m3u8_file.write_text(sample_content)
    for i in range(5):
        (tmp_path / f"seg_00{i}.ts").write_bytes(b"dummy data")

    count, duration, segs = parse_variant_playlist(m3u8_file)
    assert count == 5
    assert duration == 30.0
    assert len(segs) == 5

# 11. Duration availability in PLAYABLE
def test_duration_availability(mock_ingest):
    res = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "DUR-TEST-01", "source_uri": "valid.mp4"}
    )
    vod_uuid = res.json()["vod_uuid"]

    db = TestingSessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.vod_uuid == uuid.UUID(vod_uuid)).first()
        asset.status = VideoStatus.PLAYABLE
        asset.available_until_seconds = 30.0
        asset.progress = 25
        db.commit()
    finally:
        db.close()

    get_res = client.get(f"/api/v1/assets/{vod_uuid}")
    assert get_res.status_code == 200
    data = get_res.json()
    assert data["status"] == "PLAYABLE"
    assert data["playable"] is True
    assert data["available_until_seconds"] == 30.0

# 12. URL immutability across all states
def test_url_immutability_across_all_states(mock_ingest):
    res = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "IMMUTABLE-01", "source_uri": "valid.mp4"}
    )
    vod_uuid = res.json()["vod_uuid"]
    initial_url = res.json()["manifest_url"]

    states_to_test = [
        VideoStatus.COLD,
        VideoStatus.QUEUED,
        VideoStatus.PROCESSING,
        VideoStatus.PLAYABLE,
        VideoStatus.VALIDATING,
        VideoStatus.READY
    ]

    db = TestingSessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.vod_uuid == uuid.UUID(vod_uuid)).first()
        for st in states_to_test:
            asset.status = st
            db.commit()

            get_res = client.get(f"/api/v1/assets/{vod_uuid}")
            assert get_res.status_code == 200
            assert get_res.json()["manifest_url"] == initial_url, f"URL changed at state {st}"
    finally:
        db.close()

# 13. Validation failure retains output directory for evidence
def test_validation_failure_retains_output_directory(mock_ingest, tmp_path):
    from src.worker.tasks import progressive_transcode_asset_job
    from src.worker.transcode import TranscodeError

    res = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "FAIL-RETAIN-01", "source_uri": "valid.mp4"}
    )
    vod_uuid = res.json()["vod_uuid"]

    # Trigger prepare-playback
    prep = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
    assert prep.status_code == 200

    db = TestingSessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.vod_uuid == uuid.UUID(vod_uuid)).first()
        job = db.query(Job).filter(Job.asset_id == asset.id).first()
        job_id = job.id
    finally:
        db.close()

    # Create dummy output directory to verify it is NOT deleted on validation failure
    output_dir = (Path(settings.OUTPUT_ROOT).resolve() / asset.manifest_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    evidence_file = output_dir / "evidence.txt"
    evidence_file.write_text("diagnostic evidence")

    with patch("src.worker.tasks.SessionLocal", side_effect=TestingSessionLocal), \
         patch("src.worker.tasks.validate_hls_output", side_effect=TranscodeError("Corrupted manifest", "E_HLS_VALIDATION_FAILED")), \
         patch("src.worker.transcode.subprocess.Popen") as mock_popen:
        
        # Mock ffmpeg process returning immediately
        proc_mock = MagicMock()
        proc_mock.poll.return_value = 0
        proc_mock.returncode = 0
        proc_mock.stderr.readline.return_value = ""
        proc_mock.stdout.readline.return_value = ""
        mock_popen.return_value = proc_mock

        # Run worker task
        try:
            progressive_transcode_asset_job(job_id)
        except Exception:
            pass

    # Verify asset is FAILED
    db = TestingSessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.vod_uuid == uuid.UUID(vod_uuid)).first()
        assert asset.status == VideoStatus.FAILED
    finally:
        db.close()

    # CRITICAL: Verify output dir and evidence were NOT wiped
    assert output_dir.exists(), "Output directory must NOT be deleted on failure!"
    assert evidence_file.exists(), "Evidence file must be preserved for diagnosis!"

# 14. Full progressive lifecycle test
def test_progressive_full_lifecycle(mock_ingest):
    from src.worker.tasks import progressive_transcode_asset_job
    
    res = client.post(
        "/api/v1/experimental/cold-assets",
        json={"enlace_id": "LIFECYCLE-01", "source_uri": "valid.mp4"}
    )
    assert res.status_code == 201
    vod_uuid = res.json()["vod_uuid"]
    canonical_url = res.json()["manifest_url"]

    # 1. Prepare playback -> QUEUED
    prep = client.post(f"/api/v1/assets/{vod_uuid}/prepare-playback")
    assert prep.status_code == 200
    assert prep.json()["status"] == "QUEUED"

    db = TestingSessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.vod_uuid == uuid.UUID(vod_uuid)).first()
        job = db.query(Job).filter(Job.asset_id == asset.id).first()
        job_id = job.id
    finally:
        db.close()

    # 2. Run progressive worker job using test db
    with patch("src.worker.tasks.SessionLocal", side_effect=TestingSessionLocal):
        progressive_transcode_asset_job(job_id)

    # 3. Check final state in DB
    db = TestingSessionLocal()
    try:
        asset = db.query(Asset).filter(Asset.vod_uuid == uuid.UUID(vod_uuid)).first()
        assert asset.status == VideoStatus.READY
        assert asset.playable is True
        assert asset.progress == 100
        assert asset.manifest_url == canonical_url
        full_manifest_path = Path(settings.OUTPUT_ROOT).resolve() / asset.manifest_path
        assert full_manifest_path.exists(), f"Manifest file {full_manifest_path} must exist"
    finally:
        db.close()

# 15. Nginx translation rule (*definst* -> _definst_)
def test_nginx_translation_rule():
    test_uuid = "217CBED8-667B-4A9B-B000-D3003160B0C5"
    enlace_id = "NGINX-TEST-01"
    
    # Create the physical directory and dummy manifest in storage/output
    manifest_rel = build_canonical_manifest_path(test_uuid, enlace_id)
    manifest_path = Path(settings.OUTPUT_ROOT).resolve() / manifest_rel
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n")

    try:
        # Request through Nginx port 8085 using the canonical URL with *definst*
        url = f"http://localhost:8085/EnlacePlus/*definst*/amlst:{test_uuid}/{enlace_id}/manifest.m3u8"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200, f"Nginx returned {resp.status} for {url}"
            content = resp.read().decode('utf-8')
            assert "#EXTM3U" in content
    finally:
        # Clean up test files
        if manifest_path.parent.exists():
            shutil.rmtree(manifest_path.parent)
