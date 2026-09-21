import logging
import os
import shutil
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from src.core.database import get_db
from src.core.config import settings

from src.core.startup import validate_environment
from src.api.routes import router as api_router
from src.api.admin_routes import router as admin_router
from src.api.progressive_routes import router as progressive_router, progressive_player, asset_player

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicialización en Fase 1
    validate_environment()
    yield
    # Limpieza si aplica

app = FastAPI(
    title="VOD MVP API",
    description="API de ingestión y consulta para el servicio de VOD, hermano del CDN MAC.",
    version="1.0.0",
    lifespan=lifespan
)

from sqlalchemy import text

app.include_router(api_router, prefix="/api/v1")
app.include_router(admin_router, prefix="/api/v1")
app.include_router(progressive_router, prefix="/api/v1/experimental/progressive")
static_dir = Path(__file__).parent / "static"
app.mount("/admin-static", StaticFiles(directory=static_dir), name="admin-static")

@app.get("/experimental/progressive-player", include_in_schema=False)
def progressive_player_page(session_uuid: str = None):
    return progressive_player(session_uuid)

@app.get("/experimental/asset-player", include_in_schema=False)
def asset_player_page(vod_uuid: str = None):
    return asset_player(vod_uuid)


@app.get("/admin", include_in_schema=False)
def admin_dashboard_page():
    return FileResponse(static_dir / "admin.html")

@app.get("/health/live")
def health_live():
    return {"status": "alive"}

@app.get("/health/ready")
def health_ready(db: Session = Depends(get_db)):
    checks = {}
    # Check DB
    try:
        db.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as e:
        logger.error(f"Database readiness check failed: {e}")
        raise HTTPException(status_code=503, detail={"status": "not_ready", "database": "unavailable"})
        
    # Check Redis
    import redis
    try:
        r = redis.Redis.from_url(settings.REDIS_URL)
        r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        logger.error(f"Redis readiness check failed: {e}")
        raise HTTPException(status_code=503, detail={"status": "not_ready", "redis": "unavailable"})

    try:
        staging = Path(settings.STAGING_ROOT).resolve(strict=True)
        output = Path(settings.OUTPUT_ROOT).resolve(strict=True)
        if os.stat(staging).st_dev != os.stat(output).st_dev:
            raise RuntimeError("staging and output are on different filesystems")
        if not os.access(staging, os.W_OK) or not os.access(output, os.W_OK):
            raise RuntimeError("staging or output is not writable")
        free_bytes = min(shutil.disk_usage(staging).free, shutil.disk_usage(output).free)
        if free_bytes < settings.MIN_FREE_DISK_BYTES:
            raise RuntimeError(f"only {free_bytes} bytes available")
        checks["storage"] = "ok"
        checks["free_disk_bytes"] = free_bytes
    except Exception as e:
        logger.error(f"Storage readiness check failed: {e}")
        raise HTTPException(status_code=503, detail={"status": "not_ready", "storage": "unavailable"})

    return {"status": "ready", "checks": checks}
