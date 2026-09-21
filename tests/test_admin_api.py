from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.core.config import settings
from src.core.database import get_db
from src.main import app
from tests.conftest import TestingSessionLocal


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db
client = TestClient(app)


def test_admin_dashboard_page_is_served():
    response = client.get("/admin")
    assert response.status_code == 200
    assert "VOD Control Room" in response.text


@pytest.fixture
def admin_key():
    previous = settings.ADMIN_API_KEY
    settings.ADMIN_API_KEY = "test-admin-secret"
    yield {"X-Admin-Key": "test-admin-secret"}
    settings.ADMIN_API_KEY = previous


def test_admin_fails_closed_when_not_configured():
    previous = settings.ADMIN_API_KEY
    settings.ADMIN_API_KEY = ""
    try:
        response = client.get("/api/v1/admin/dashboard")
    finally:
        settings.ADMIN_API_KEY = previous
    assert response.status_code == 503


def test_admin_rejects_invalid_key(admin_key):
    response = client.get("/api/v1/admin/assets", headers={"X-Admin-Key": "wrong"})
    assert response.status_code == 401


def test_admin_asset_and_job_lists(admin_key):
    assert client.get("/api/v1/admin/assets", headers=admin_key).status_code == 200
    assert client.get("/api/v1/admin/jobs", headers=admin_key).status_code == 200


@patch("src.api.admin_routes.Worker.all", return_value=[])
@patch("src.api.admin_routes.Queue")
@patch("src.api.admin_routes.Redis.from_url")
def test_admin_dashboard(mock_redis, mock_queue, mock_workers, admin_key):
    mock_queue.return_value.count = 3
    response = client.get("/api/v1/admin/dashboard", headers=admin_key)
    assert response.status_code == 200
    q_data = response.json()["queue"]
    assert q_data["name"] == settings.RQ_QUEUE_NAME
    assert q_data["depth"] == 3
    assert q_data["workers"] == 0
    assert q_data["stale_jobs"] == 0
    assert q_data["legacy_queue_depth"] == 3
    assert q_data["priority_queue_depth"] == 3
    assert q_data["batch_queue_depth"] == 3
    assert q_data["total_queue_depth"] == 9

def test_admin_assets_expanded_fields_and_playback_url(admin_key, db_session):
    from src.models.asset import Asset
    from src.models.rendition import Rendition
    from src.models.enums import VideoStatus
    import uuid
    from datetime import datetime, timezone

    asset_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=asset_uuid,
        enlace_id="TEST-EXPANDED",
        source_uri="test.mp4",
        status=VideoStatus.READY,
        progress=100,
        manifest_path="test/manifest.m3u8",
        manifest_url="https://cdn.example.com/manifest.m3u8",
        duration_seconds=120.5,
        source_width=1920,
        source_height=1080,
        video_codec="h264",
        audio_codec="aac",
        published_at=datetime.now(timezone.utc)
    )
    rendition = Rendition(
        asset_id=asset.id,
        name="1080p",
        width=1920,
        height=1080,
        video_bitrate=5000000,
        audio_bitrate=192000,
        playlist_path="test/1080p.m3u8"
    )
    asset.renditions.append(rendition)
    db_session.add(asset)
    db_session.commit()

    response = client.get("/api/v1/admin/assets", headers=admin_key)
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 1
    
    item = next(x for x in data["items"] if x["vod_uuid"] == str(asset_uuid))
    assert item["duration_seconds"] == 120.5
    assert item["source_width"] == 1920
    assert item["source_height"] == 1080
    assert item["manifest_path"] == "test/manifest.m3u8"
    assert item["manifest_url"] == "https://cdn.example.com/manifest.m3u8"
    assert item["playback_url"] == f"{settings.HLS_PLAYBACK_BASE_URL.rstrip('/')}/test/manifest.m3u8"
    assert item["video_codec"] == "h264"
    assert item["audio_codec"] == "aac"
    assert "published_at" in item
    assert isinstance(item["variants"], list)
    assert len(item["variants"]) == 1
    assert item["variants"][0]["name"] == "1080p"
    assert item["variants"][0]["width"] == 1920
    assert item["variants"][0]["height"] == 1080
    assert item["variants"][0]["video_bitrate"] == 5000000
    assert item["variants"][0]["audio_bitrate"] == 192000


def test_admin_assets_metrics_calculation(admin_key, db_session):
    from src.models.asset import Asset
    from src.models.job import Job
    from src.models.enums import VideoStatus, JobType, JobStatus
    import uuid
    from datetime import datetime, timezone, timedelta

    asset_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=asset_uuid,
        enlace_id="TEST-METRICS-1",
        source_uri="test.mp4",
        status=VideoStatus.READY,
        published_at=datetime.now(timezone.utc) + timedelta(minutes=10)
    )
    db_session.add(asset)
    db_session.commit()
    
    t0 = datetime.now(timezone.utc)
    t1 = t0 + timedelta(seconds=10)
    t2 = t1 + timedelta(seconds=20)
    
    # Failed probe attempt 1
    job1 = Job(asset_id=asset.id, type=JobType.PROBE, status=JobStatus.FAILED, attempt=1, created_at=t0, started_at=t1, finished_at=t2)
    db_session.add(job1)
    
    # Successful probe attempt 2
    t3 = t2 + timedelta(seconds=5)
    t4 = t3 + timedelta(seconds=15)
    t5 = t4 + timedelta(seconds=30)
    job2 = Job(asset_id=asset.id, type=JobType.PROBE, status=JobStatus.COMPLETED, attempt=2, created_at=t3, started_at=t4, finished_at=t5)
    db_session.add(job2)
    
    # Successful transcode attempt 1
    t6 = t5 + timedelta(seconds=5)
    t7 = t6 + timedelta(seconds=25)
    t8 = t7 + timedelta(minutes=5)
    job3 = Job(asset_id=asset.id, type=JobType.TRANSCODE, status=JobStatus.COMPLETED, attempt=1, created_at=t6, started_at=t7, finished_at=t8)
    db_session.add(job3)
    db_session.commit()
    
    response = client.get("/api/v1/admin/assets", headers=admin_key)
    assert response.status_code == 200
    data = response.json()
    
    item = next(x for x in data["items"] if x["vod_uuid"] == str(asset_uuid))
    
    assert item["queue_wait_seconds"] == 15 + 25  # Probe wait + Transcode wait
    assert item["probe_processing_seconds"] == 30
    assert item["transcode_processing_seconds"] == 300
    assert item["total_processing_seconds"] == 330
    # published_at is set to t0 + 10m
    expected_elapsed = (asset.published_at - t0).total_seconds()
    assert abs(item["elapsed_wall_seconds"] - expected_elapsed) < 1.0


def test_admin_assets_metrics_incomplete(admin_key, db_session):
    from src.models.asset import Asset
    from src.models.job import Job
    from src.models.enums import VideoStatus, JobType, JobStatus
    import uuid
    from datetime import datetime, timezone

    asset_uuid = uuid.uuid4()
    asset = Asset(
        vod_uuid=asset_uuid,
        enlace_id="TEST-METRICS-2",
        source_uri="test.mp4",
        status=VideoStatus.PROCESSING
    )
    db_session.add(asset)
    db_session.commit()
    
    # Only created, not started
    job = Job(asset_id=asset.id, type=JobType.PROBE, status=JobStatus.PENDING, attempt=1, created_at=datetime.now(timezone.utc))
    db_session.add(job)
    db_session.commit()
    
    response = client.get("/api/v1/admin/assets", headers=admin_key)
    item = next(x for x in response.json()["items"] if x["vod_uuid"] == str(asset_uuid))
    
    assert item["queue_wait_seconds"] is None
    assert item["probe_processing_seconds"] is None
    assert item["transcode_processing_seconds"] is None
    assert item["total_processing_seconds"] is None
    assert item["elapsed_wall_seconds"] is None
