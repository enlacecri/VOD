import os
import uuid
import pytest
import threading
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
import rq

from sqlalchemy.pool import NullPool
from src.main import app
from src.core.database import get_db
from tests.conftest import TestingSessionLocal
from src.core.config import settings

def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db
client = TestClient(app)

@pytest.fixture
def mock_ingest(tmp_path):
    old_root = settings.INGEST_ROOT
    settings.INGEST_ROOT = str(tmp_path)
    
    (tmp_path / "valid.mp4").write_text("dummy")
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "valid2.mp4").write_text("dummy")
    
    evil_dir = tmp_path.parent / "evil"
    evil_dir.mkdir(exist_ok=True)
    (evil_dir / "evil.mp4").write_text("evil")
    os.symlink(str(evil_dir / "evil.mp4"), str(tmp_path / "escape.mp4"))
    
    yield tmp_path
    settings.INGEST_ROOT = old_root

def test_health_live():
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}

def test_health_ready():
    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.json()["checks"]["database"] == "ok"
    assert response.json()["checks"]["redis"] == "ok"
    assert response.json()["checks"]["storage"] == "ok"

@patch('redis.Redis.from_url')
def test_health_ready_redis_down(mock_redis):
    mock_redis.side_effect = Exception("Connection Refused")
    response = client.get("/health/ready")
    assert response.status_code == 503

@patch('src.main.shutil.disk_usage')
def test_health_ready_low_disk(mock_disk_usage):
    mock_disk_usage.return_value.free = 0
    response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["detail"]["storage"] == "unavailable"

def test_startup_different_filesystem():
    from src.core.startup import validate_environment
    from unittest.mock import MagicMock
    import os
    real_stat = os.stat
    
    def mock_stat_func(path, *args, **kwargs):
        path_str = str(path)
        if "staging" in path_str:
            return MagicMock(st_dev=1, st_mode=0o40755)
        elif "output" in path_str:
            return MagicMock(st_dev=2, st_mode=0o40755)
        return real_stat(path, *args, **kwargs)
        
    with patch('os.stat', side_effect=mock_stat_func):
        with pytest.raises(RuntimeError, match="Atomic rename is impossible"):
            validate_environment()

def test_create_real_asset(mock_ingest):
    response = client.post(
        "/api/v1/assets",
        json={
            "enlace_id": "test-enlace-1",
            "source_uri": "valid.mp4"
        }
    )
    assert response.status_code == 201
    data = response.json()
    assert data["enlace_id"] == "test-enlace-1"
    assert "vod_uuid" in data
    
    response_get = client.get(f"/api/v1/assets/{data['vod_uuid']}")
    assert response_get.status_code == 200
    assert response_get.json()["enlace_id"] == "test-enlace-1"
    
    response_get2 = client.get("/api/v1/assets/by-enlace/test-enlace-1")
    assert response_get2.status_code == 200
    assert response_get2.json()["vod_uuid"] == data["vod_uuid"]

def test_idempotent_creation(mock_ingest):
    r1 = client.post("/api/v1/assets", json={"enlace_id": "idemp-1", "source_uri": "valid.mp4"})
    assert r1.status_code == 201
    uuid1 = r1.json()["vod_uuid"]
    
    r2 = client.post("/api/v1/assets", json={"enlace_id": "idemp-1", "source_uri": "valid.mp4"})
    assert r2.status_code == 200
    assert r2.json()["vod_uuid"] == uuid1

def test_different_source_conflict(mock_ingest):
    r1 = client.post("/api/v1/assets", json={"enlace_id": "conflict-1", "source_uri": "valid.mp4"})
    assert r1.status_code == 201
    
    r2 = client.post("/api/v1/assets", json={"enlace_id": "conflict-1", "source_uri": "subdir/valid2.mp4"})
    assert r2.status_code == 409

def test_path_validation_absolute(mock_ingest):
    r = client.post("/api/v1/assets", json={"enlace_id": "test-abs", "source_uri": "/etc/passwd"})
    assert r.status_code == 422 

def test_path_validation_traversal(mock_ingest):
    r = client.post("/api/v1/assets", json={"enlace_id": "test-trav", "source_uri": "../evil.mp4"})
    assert r.status_code == 422 

def test_path_validation_sibling_prefix(mock_ingest):
    sibling_dir = mock_ingest.parent / (mock_ingest.name + "_evil")
    sibling_dir.mkdir(exist_ok=True)
    (sibling_dir / "evil.mp4").write_text("evil")
    
    r = client.post("/api/v1/assets", json={"enlace_id": "test-sibling", "source_uri": f"../{mock_ingest.name}_evil/evil.mp4"})
    assert r.status_code == 422

def test_path_validation_escaping_symlink(mock_ingest):
    r = client.post("/api/v1/assets", json={"enlace_id": "test-sym", "source_uri": "escape.mp4"})
    assert r.status_code == 400
    assert "escapes" in r.json()["detail"] or "Symlink" in r.json()["detail"]

def test_path_validation_non_existent(mock_ingest):
    r = client.post("/api/v1/assets", json={"enlace_id": "test-no", "source_uri": "ghost.mp4"})
    assert r.status_code == 400
    assert "does not exist" in r.json()["detail"]

def test_path_validation_is_directory(mock_ingest):
    (mock_ingest / "subdir.mp4").mkdir(exist_ok=True)
    r = client.post("/api/v1/assets", json={"enlace_id": "test-dir", "source_uri": "subdir.mp4"})
    assert r.status_code == 400
    assert "is a directory" in r.json()["detail"] or "Not a regular file" in r.json()["detail"]

def test_concurrency(mock_ingest):
    results = []
    def create():
        with TestClient(app) as local_client:
            r = local_client.post("/api/v1/assets", json={"enlace_id": "concurrent-1", "source_uri": "valid.mp4"})
            results.append(r)
            
    threads = [threading.Thread(target=create) for _ in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()
    
    status_codes = [r.status_code for r in results]
    assert status_codes.count(201) == 1
    assert status_codes.count(200) == 4
    
    uuids = set(r.json()["vod_uuid"] for r in results)
    assert len(uuids) == 1

def test_enqueue_success_db_fail(mock_ingest, db_session):
    with patch("sqlalchemy.orm.Session.commit") as mock_commit:
        mock_commit.side_effect = Exception("DB Failed")
        with pytest.raises(Exception, match="DB Failed"):
            client.post(
                "/api/v1/assets",
                json={"enlace_id": "fail-test", "source_uri": "valid.mp4"}
            )
