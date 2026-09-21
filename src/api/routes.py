import os
import uuid
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from src.core.database import get_db
from src.core.config import settings
from src.core.admin_auth import require_admin_api_key
from src.models.asset import Asset
from src.schemas.asset import AssetCreate, AssetResponse

router = APIRouter()

from pathlib import Path

from src.core.security import validate_ingest_path, SecurityError

@router.post("/assets", response_model=AssetResponse, status_code=status.HTTP_201_CREATED)
def create_asset(payload: AssetCreate, db: Session = Depends(get_db)):
    from src.models.job import Job
    from src.models.asset_event import AssetEvent
    from src.models.enums import JobType, JobStatus, VideoStatus, EventType
    from redis import Redis, exceptions as redis_exceptions
    from rq import Queue
    from fastapi.responses import JSONResponse
    from fastapi.encoders import jsonable_encoder
    
    try:
        validate_ingest_path(payload.source_uri)
    except SecurityError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail=f"{e.code}: {e.message}"
        )
    
    # Intento de crear asset idempotente
    new_asset = Asset(
        vod_uuid=uuid.uuid4(),
        enlace_id=payload.enlace_id,
        source_uri=payload.source_uri,
        status=VideoStatus.CREATED
    )
    
    try:
        db.add(new_asset)
        db.commit()
        db.refresh(new_asset)
        asset_id = new_asset.id
        was_reused = False
    except IntegrityError:
        db.rollback()
        existing_asset = db.query(Asset).filter(Asset.enlace_id == payload.enlace_id).first()
        if existing_asset:
            if existing_asset.source_uri != payload.source_uri:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Conflict: enlace_id exists with a different source_uri. Use /replace endpoint."
                )
            
            # Si el asset ya estaba en FAILED o CREATED, lo devolvemos con was_reused=True SIN ENCOLAR
            # El usuario debe llamar a /retry explícitamente para intentar de nuevo si falló.
            resp = jsonable_encoder(AssetResponse.model_validate(existing_asset))
            resp["was_reused"] = True
            return JSONResponse(status_code=status.HTTP_200_OK, content=resp)
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="Database integrity error."
            )
            
    asset = db.query(Asset).filter(Asset.id == asset_id).first()
    
    new_job = Job(
        asset_id=asset.id,
        type=JobType.PROBE,
        status=JobStatus.PENDING,
        attempt=1,
        max_attempts=settings.MAX_PROBE_ATTEMPTS
    )
    db.add(new_job)
    
    event = AssetEvent(
        asset_id=asset.id,
        event_type=EventType.TRANSITION,
        details={"new_status": "CREATED", "job_id": str(new_job.id)}
    )
    db.add(event)
    db.commit()
    db.refresh(new_job)
        
    # Encolar en Redis RQ
    try:
        redis_conn = Redis.from_url(settings.REDIS_URL)
        q = Queue(name=settings.RQ_QUEUE_NAME, connection=redis_conn)
        
        # Consultar si el job ya existe (defensivo)
        existing_rq_job = q.fetch_job(str(new_job.id))
        if existing_rq_job:
            rq_job = existing_rq_job
        else:
            rq_job = q.enqueue(
                "src.worker.tasks.probe_and_prepare_job", 
                args=(new_job.id,),
                job_id=str(new_job.id),
                job_timeout=settings.RQ_JOB_TIMEOUT_SECONDS,
                result_ttl=86400
            )
            
        try:
            new_job.rq_job_id = rq_job.id
            db.commit()
        except Exception as e:
            # Si el commit falla, el job está en Redis pero la BD no tiene rq_job_id.
            # El reconciliador lo arreglará. No fallamos la petición porque el procesamiento ocurrirá.
            db.rollback()
            import logging
            logging.getLogger(__name__).error(f"Failed to commit rq_job_id for job {new_job.id}: {e}")
        
        resp = jsonable_encoder(AssetResponse.model_validate(asset))
        resp["was_reused"] = was_reused
        return JSONResponse(status_code=status.HTTP_201_CREATED, content=resp)
            
    except Exception as e: # Capturar RedisError y rq.exceptions
        asset.status = VideoStatus.FAILED
        asset.error_code = "E_QUEUE_UNAVAILABLE"
        asset.error_message = "Could not enqueue job to Redis"
        
        new_job.status = JobStatus.FAILED
        new_job.error_code = "E_QUEUE_UNAVAILABLE"
        new_job.error_message = str(e)
        
        db.add(AssetEvent(
            asset_id=asset.id,
            event_type=EventType.ERROR,
            details={"error_code": "E_QUEUE_UNAVAILABLE", "error_message": str(e)}
        ))
        db.commit()
        
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error_code": "E_QUEUE_UNAVAILABLE", "message": "Failed to enqueue job"}
        )

@router.post("/assets/{vod_uuid}/retry", response_model=AssetResponse)
def retry_asset(vod_uuid: uuid.UUID, db: Session = Depends(get_db), _admin: None = Depends(require_admin_api_key)):
    from src.models.job import Job
    from src.models.asset_event import AssetEvent
    from src.models.enums import JobType, JobStatus, VideoStatus, EventType
    from redis import Redis, exceptions as redis_exceptions
    from rq import Queue
    from sqlalchemy import func
    
    # 1. Lock the asset to prevent concurrent retries
    asset = db.query(Asset).filter(Asset.vod_uuid == vod_uuid).with_for_update().first()
    if not asset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Asset not found."
        )
        
    if asset.status not in [VideoStatus.FAILED, VideoStatus.CREATED]:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot retry asset in status {asset.status.value}"
        )
        
    # Check for active jobs
    active_job = db.query(Job).filter(
        Job.asset_id == asset.id,
        Job.status.in_([JobStatus.PENDING, JobStatus.PROCESSING])
    ).first()
    
    if active_job:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Asset already has an active job."
        )
        
    # Calcular nuevo intento
    max_prev_attempt = db.query(func.max(Job.attempt)).filter(Job.asset_id == asset.id).scalar() or 0
    new_attempt = max_prev_attempt + 1
    
    if new_attempt > settings.MAX_PROBE_ATTEMPTS:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="E_MAX_ATTEMPTS_EXCEEDED"
        )
        
    # Reactivate
    asset.status = VideoStatus.CREATED
    asset.error_code = None
    asset.error_message = None
    
    new_job = Job(
        asset_id=asset.id,
        type=JobType.PROBE,
        status=JobStatus.PENDING,
        attempt=new_attempt,
        max_attempts=settings.MAX_PROBE_ATTEMPTS
    )
    db.add(new_job)
    
    db.add(AssetEvent(
        asset_id=asset.id,
        event_type=EventType.TRANSITION,
        details={"new_status": "CREATED", "job_id": str(new_job.id), "context": "retry", "attempt": new_attempt}
    ))
    db.commit()
    db.refresh(new_job)
    
    # Encolar
    # Encolar
    try:
        redis_conn = Redis.from_url(settings.REDIS_URL)
        q = Queue(name=settings.RQ_QUEUE_NAME, connection=redis_conn)
        
        existing_rq_job = q.fetch_job(str(new_job.id))
        if existing_rq_job:
            rq_job = existing_rq_job
        else:
            rq_job = q.enqueue(
                "src.worker.tasks.probe_and_prepare_job", 
                args=(new_job.id,),
                job_id=str(new_job.id),
                job_timeout=settings.RQ_JOB_TIMEOUT_SECONDS,
                result_ttl=86400
            )
            
        try:
            new_job.rq_job_id = rq_job.id
            db.commit()
        except Exception as e:
            db.rollback()
            import logging
            logging.getLogger(__name__).error(f"Failed to commit rq_job_id for job {new_job.id}: {e}")
            
        return asset
    except Exception as e:
        asset.status = VideoStatus.FAILED
        asset.error_code = "E_QUEUE_UNAVAILABLE"
        asset.error_message = "Could not enqueue job to Redis"
        
        new_job.status = JobStatus.FAILED
        new_job.error_code = "E_QUEUE_UNAVAILABLE"
        new_job.error_message = str(e)
        
        db.add(AssetEvent(
            asset_id=asset.id,
            event_type=EventType.ERROR,
            details={"error_code": "E_QUEUE_UNAVAILABLE", "error_message": str(e)}
        ))
        db.commit()
        
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error_code": "E_QUEUE_UNAVAILABLE", "message": "Failed to enqueue job"}
        )

@router.get("/assets/by-enlace/{enlace_id}", response_model=AssetResponse)
def get_asset_by_enlace(enlace_id: str, db: Session = Depends(get_db)):
    asset = db.query(Asset).filter(Asset.enlace_id == enlace_id).first()
    if not asset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Asset not found."
        )
    return asset

@router.get("/assets/{vod_uuid}", response_model=AssetResponse)
def get_asset(vod_uuid: uuid.UUID, db: Session = Depends(get_db)):
    asset = db.query(Asset).filter(Asset.vod_uuid == vod_uuid).first()
    if not asset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Asset not found."
        )
    return asset

@router.post("/assets/{vod_uuid}/replace")
def replace_asset(vod_uuid: uuid.UUID, payload: AssetCreate, db: Session = Depends(get_db)):
    """
    CONTRATO PARA REEMPLAZO (FASE POSTERIOR)
    - Validar que el asset exista.
    - Validar que enlace_id coincida.
    - Validar nuevo source_uri.
    - Si el hash coincide con el procesado, ignorar.
    - Si el hash cambia, marcar como INVALIDATED/REPLACING y encolar nuevo trabajo
      hacia storage/staging, validarlo, y finalmente realizar un swap atómico del directorio
      storage/output.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Replacement feature not implemented in Phase 1."
    )

@router.post("/assets/{vod_uuid}/retry-transcode", response_model=AssetResponse)
def retry_transcode(vod_uuid: uuid.UUID, db: Session = Depends(get_db), _admin: None = Depends(require_admin_api_key)):
    from src.models.job import Job
    from src.models.asset_event import AssetEvent
    from src.models.enums import JobType, JobStatus, VideoStatus, EventType
    from redis import Redis
    from rq import Queue
    from sqlalchemy.orm import Session
    from sqlalchemy.exc import NoResultFound

    # Bloquear asset con FOR UPDATE
    try:
        asset = db.query(Asset).filter(Asset.vod_uuid == vod_uuid).with_for_update().one()
    except NoResultFound:
        raise HTTPException(status_code=404, detail="Asset not found")

    if asset.status == VideoStatus.READY:
        raise HTTPException(status_code=400, detail="Asset is already published")
        
    # Verificar si hay trabajos activos
    active_jobs = db.query(Job).filter(
        Job.asset_id == asset.id,
        Job.status.in_([JobStatus.PENDING, JobStatus.PROCESSING])
    ).count()
    
    if active_jobs > 0:
        raise HTTPException(status_code=400, detail="Asset has active jobs")

    # Obtener el último job TRANSCODE
    last_transcode = db.query(Job).filter(
        Job.asset_id == asset.id,
        Job.type == JobType.TRANSCODE
    ).order_by(Job.attempt.desc()).first()

    if not last_transcode or last_transcode.status != JobStatus.FAILED:
        raise HTTPException(status_code=400, detail="Last TRANSCODE job must be FAILED to retry")

    new_attempt = last_transcode.attempt + 1
    if new_attempt > settings.MAX_TRANSCODE_ATTEMPTS:
        raise HTTPException(status_code=400, detail="Max transcode attempts exceeded")

    # Verificar existencia y tamaño de la copia estable para evitar hashing síncrono lento
    if not asset.staged_source_path:
        raise HTTPException(status_code=400, detail="No staged source path")
        
    staging_root = Path(settings.STAGING_ROOT).resolve()
    candidate = (staging_root / asset.staged_source_path).resolve()
    
    if not candidate.is_relative_to(staging_root):
        raise HTTPException(status_code=400, detail="Staged file outside staging root")
        
    if not candidate.is_file():
        raise HTTPException(status_code=400, detail="Staged file does not exist")
        
    if candidate.stat().st_size != asset.size:
        raise HTTPException(status_code=400, detail="Staged file size mismatch")

    asset.status = VideoStatus.QUEUED
    asset.error_code = None
    asset.error_message = None

    new_job = Job(
        asset_id=asset.id,
        type=JobType.TRANSCODE,
        status=JobStatus.PENDING,
        attempt=new_attempt,
        max_attempts=settings.MAX_TRANSCODE_ATTEMPTS
    )
    db.add(new_job)
    
    db.add(AssetEvent(
        asset_id=asset.id,
        event_type=EventType.TRANSITION,
        details={"new_status": "QUEUED", "job_id": str(new_job.id), "context": "retry-transcode", "attempt": new_attempt}
    ))
    db.commit()
    db.refresh(new_job)

    # Encolar con ID determinista
    try:
        redis_conn = Redis.from_url(settings.REDIS_URL)
        q = Queue(name=settings.RQ_QUEUE_NAME, connection=redis_conn)
        
        existing_rq_job = q.fetch_job(str(new_job.id))
        if existing_rq_job:
            rq_job = existing_rq_job
        else:
            rq_job = q.enqueue(
                "src.worker.tasks.transcode_asset_job", 
                args=(new_job.id,),
                job_id=str(new_job.id),
                job_timeout=settings.FFMPEG_TIMEOUT_SECONDS + 300,
                result_ttl=86400
            )
            
        try:
            new_job.rq_job_id = rq_job.id
            db.commit()
        except Exception as e:
            db.rollback()
            import logging
            logging.getLogger(__name__).error(f"Failed to commit rq_job_id for job {new_job.id}: {e}")
            
        return asset
    except Exception as e:
        asset.status = VideoStatus.FAILED
        asset.error_code = "E_QUEUE_UNAVAILABLE"
        asset.error_message = "Could not enqueue job to Redis"
        
        new_job.status = JobStatus.FAILED
        new_job.error_code = "E_QUEUE_UNAVAILABLE"
        new_job.error_message = str(e)
        
        db.add(AssetEvent(
            asset_id=asset.id,
            event_type=EventType.ERROR,
            details={"error_code": "E_QUEUE_UNAVAILABLE", "error_message": str(e)}
        ))
        db.commit()
        
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error_code": "E_QUEUE_UNAVAILABLE", "message": "Failed to enqueue job"}
        )
