import os
import shutil
from pathlib import Path
from redis import Redis
import time
import subprocess
import queue
import threading
from uuid import UUID
from datetime import datetime, timezone
import logging
import rq

from src.core.database import SessionLocal
from src.core.config import settings
from src.core.state import transition_asset, fail_asset
from src.core.security import secure_copy_and_hash, validate_ingest_path, SecurityError
from src.worker.probe import analyze_media, ProbeError
from src.worker.archive import archive_source_file
from src.models.asset import Asset
from src.models.job import Job
from src.models.rendition import Rendition
from src.models.ingest_item import IngestItem
from src.models.enums import VideoStatus, JobStatus, JobType
from src.worker.transcode import execute_transcode, TranscodeError, get_selected_variants, LADDER
from src.worker.validation import validate_hls_output
from src.services.workflow.orchestrator import schedule_post_ready_work

logger = logging.getLogger(__name__)

def truncate_error(msg: str) -> str:
    limit = settings.ERROR_MESSAGE_MAX_LENGTH
    return msg if len(msg) <= limit else msg[:limit - 3] + "..."

def probe_and_prepare_job(job_id: UUID):
    """
    Worker task to probe the asset, calculate hash securely by streaming, and prepare it for transcoding.
    """
    db = SessionLocal()
    current_job = rq.get_current_job()
    worker_id = current_job.worker_name if current_job else "local"
    
    staging_root = Path(settings.STAGING_ROOT).resolve(strict=True)
    
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job or job.type != JobType.PROBE:
            logger.error(f"Job not found or invalid type: {job_id}")
            return
            
        job.status = JobStatus.PROCESSING
        job.worker_id = worker_id
        job.started_at = datetime.now(timezone.utc)
        job.heartbeat = datetime.now(timezone.utc)
        db.commit()
        
        asset = db.query(Asset).filter(Asset.id == job.asset_id).first()
        if not asset or asset.status != VideoStatus.CREATED:
            job.status = JobStatus.FAILED
            job.error_message = "Asset not found or not in CREATED state"
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return

        # Transition to PROBING
        transition_asset(db, asset, VideoStatus.PROBING)
        db.commit()

        job_staging_dir = staging_root / "jobs" / str(job_id)
        asset_staging_dir = staging_root / "assets" / str(asset.vod_uuid)
        
        staging_part_file = job_staging_dir / "source.part"
        stable_source_file = asset_staging_dir / "source.bin"
        
        try:
            def heartbeat_callback():
                # Re-fetch job to avoid stale state in long operations if needed,
                # but simple commit on existing session might be enough if no concurrent modifications.
                # To be safe, just update heartbeat.
                job.heartbeat = datetime.now(timezone.utc)
                db.commit()

            # 1. Validate Source Path
            candidate = validate_ingest_path(asset.source_uri)
            
            job.heartbeat = datetime.now(timezone.utc)
            db.commit()

            # 2. Create job exclusive directory
            job_staging_dir.mkdir(parents=True, exist_ok=True)
            
            # 3. Securely copy to temp part and calculate hash during streaming
            sha256, size = secure_copy_and_hash(candidate, staging_part_file, heartbeat_callback=heartbeat_callback)
            
            job.heartbeat = datetime.now(timezone.utc)
            db.commit()
            
            # 4. Analyze media strictly on the stable copy (staging_part_file)
            media_info = analyze_media(staging_part_file, heartbeat_callback=heartbeat_callback)
            
            job.heartbeat = datetime.now(timezone.utc)
            db.commit()
            
            # 5. Promote to stable asset directory
            asset_staging_dir.mkdir(parents=True, exist_ok=True)
            os.replace(str(staging_part_file), str(stable_source_file))
            
            # Try to fsync parent directory of stable file
            try:
                dir_fd = os.open(str(asset_staging_dir), os.O_RDONLY)
                os.fsync(dir_fd)
                os.close(dir_fd)
            except OSError:
                pass
            
            # 6. Update asset
            asset.source_sha256 = sha256
            asset.size = size
            asset.duration_seconds = media_info["duration_seconds"]
            asset.source_width = media_info["source_width"]
            asset.source_height = media_info["source_height"]
            asset.video_codec = media_info["video_codec"]
            asset.audio_codec = media_info["audio_codec"]
            asset.has_audio = media_info["has_audio"]
            asset.source_fps = media_info.get("fps")
            asset.probe_metadata = media_info["probe_metadata"]
            
            # Relative to STAGING_ROOT
            staged_rel = stable_source_file.relative_to(staging_root)
            asset.staged_source_path = str(staged_rel)
            
            # Transition to QUEUED (Ready for transcoding phase 3)
            transition_asset(db, asset, VideoStatus.QUEUED)
            job.status = JobStatus.COMPLETED
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            
            # Cascade: Automatically start TRANSCODE
            try:
                transcode_job = Job(
                    asset_id=asset.id, 
                    type=JobType.TRANSCODE, 
                    status=JobStatus.PENDING,
                    attempt=1,
                    max_attempts=settings.MAX_TRANSCODE_ATTEMPTS
                )
                db.add(transcode_job)
                db.commit()
                
                if current_job:
                    redis_conn = current_job.connection
                else:
                    redis_conn = Redis.from_url(settings.REDIS_URL)

                q = rq.Queue(name=settings.RQ_QUEUE_NAME, connection=redis_conn)
                rq_job = q.enqueue(
                    "src.worker.tasks.transcode_asset_job", 
                    args=(transcode_job.id,), 
                    job_id=str(transcode_job.id),
                    job_timeout=settings.FFMPEG_TIMEOUT_SECONDS + 300
                )
                transcode_job.rq_job_id = rq_job.id
                db.commit()
            except Exception as e:
                logger.error(f"Failed to cascade TRANSCODE job for asset {asset.id}: {e}")
                # We don't rollback or fail the PROBE job since the PROBE successfully completed
                # The asset remains in QUEUED status and can be retried or reconciled
            
        except SecurityError as e:
            db.rollback()
            err_msg = truncate_error(e.message)
            fail_asset(db, asset, e.code, err_msg)
            job.status = JobStatus.FAILED
            job.error_code = e.code
            job.error_message = err_msg
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
        except ProbeError as e:
            db.rollback()
            err_msg = truncate_error(str(e))
            fail_asset(db, asset, e.code, err_msg)
            job.status = JobStatus.FAILED
            job.error_code = e.code
            job.error_message = err_msg
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
        except Exception as e:
            db.rollback()
            logger.exception("Unexpected error in worker")
            err_msg = truncate_error(str(e))
            fail_asset(db, asset, "E_INTERNAL_ERROR", "Internal worker error")
            job.status = JobStatus.FAILED
            job.error_code = "E_INTERNAL_ERROR"
            job.error_message = err_msg
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
        finally:
            # Clean up the job staging directory (removes source.part if it failed)
            if staging_part_file.exists():
                try:
                    staging_part_file.unlink()
                except OSError:
                    pass
            try:
                job_staging_dir.rmdir()
            except OSError:
                pass

    finally:
        db.close()

def transcode_asset_job(job_id: UUID):
    """
    Worker task to transcode an asset to HLS.
    """
    db = SessionLocal()
    current_job = rq.get_current_job()
    worker_id = current_job.worker_name if current_job else "local"
    
    staging_root = Path(settings.STAGING_ROOT).resolve(strict=True)
    output_root = Path(settings.OUTPUT_ROOT).resolve(strict=True)
    
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job or job.type != JobType.TRANSCODE:
            logger.error(f"Job not found or invalid type: {job_id}")
            return
            
        job.status = JobStatus.PROCESSING
        job.worker_id = worker_id
        job.started_at = datetime.now(timezone.utc)
        job.heartbeat = datetime.now(timezone.utc)
        
        # Set log path
        log_root = Path(settings.LOG_ROOT)
        log_root.mkdir(parents=True, exist_ok=True)
        log_root = log_root.resolve(strict=True)
        log_path = log_root / "jobs" / str(job_id) / "ffmpeg.log"
        job.log_path = str(log_path.relative_to(log_root))
        
        db.commit()
        
        asset = db.query(Asset).filter(Asset.id == job.asset_id).first()
        if not asset or asset.status != VideoStatus.QUEUED:
            job.status = JobStatus.FAILED
            job.error_message = "Asset not found or not in QUEUED state"
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return

        # Transition to PROCESSING
        transition_asset(db, asset, VideoStatus.PROCESSING)
        db.commit()

        job_staging_dir = staging_root / "jobs" / str(job_id)
        job_hls_dir = job_staging_dir / "hls"
        
        # Validar ruta de origen desde STAGING
        try:
            if not asset.staged_source_path:
                raise SecurityError("E_SECURITY_PATH_NON_EXISTENT", "staged_source_path is null")
            
            # Use strict path validation logic inside staging root
            from src.core.security import secure_resolve
            # Relativa a STAGING_ROOT
            candidate = secure_resolve(staging_root, asset.staged_source_path)
            
            if not candidate.is_file():
                raise SecurityError("E_SECURITY_PATH_IS_DIRECTORY", "Source is not a file")
                
            if asset.size is not None and candidate.stat().st_size != asset.size:
                raise SecurityError("E_SOURCE_SIZE_MISMATCH", "Source file size mismatch")
                
            # Calcular SHA-256 por streaming
            import hashlib
            hasher = hashlib.sha256()
            with open(candidate, "rb") as f:
                last_heartbeat = time.monotonic()
                for chunk in iter(lambda: f.read(settings.HASH_CHUNK_SIZE), b""):
                    hasher.update(chunk)
                    now = time.monotonic()
                    if now - last_heartbeat > 5.0:
                        job.heartbeat = datetime.now(timezone.utc)
                        db.commit()
                        last_heartbeat = now
            
            computed_hash = hasher.hexdigest()
            if computed_hash != asset.source_sha256:
                raise SecurityError("E_SOURCE_HASH_MISMATCH", "Source hash does not match asset")
                
        except SecurityError as e:
            db.rollback()
            err_msg = truncate_error(e.message)
            fail_asset(db, asset, e.code, err_msg)
            job.status = JobStatus.FAILED
            job.error_code = e.code
            job.error_message = err_msg
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return
        
        # Public route constraint: OUTPUT_ROOT/EnlacePlus/_definst_/amlst:{VOD_UUID}/{ENLACE_ID}/manifest.m3u8
        # Enforce exact path format
        if not asset.vod_uuid or not asset.enlace_id:
            raise TranscodeError("E_INVALID_METADATA", "VOD UUID or Enlace ID is null")
            
        uuid_upper = str(asset.vod_uuid).upper()
        asset_out_dir = output_root / "EnlacePlus" / "_definst_" / f"amlst:{uuid_upper}" / str(asset.enlace_id)
        
        try:
            def heartbeat_callback():
                job.heartbeat = datetime.now(timezone.utc)
                db.commit()

            def progress_callback(pct: int):
                asset.progress = pct
                job.heartbeat = datetime.now(timezone.utc)
                db.commit()

            # 1. Ensure clean job dir
            # job_hls_dir is created inside execute_transcode with exist_ok=False
            
            job.heartbeat = datetime.now(timezone.utc)
            db.commit()

            # 2. Transcode
            execute_transcode(
                source_path=candidate,
                output_dir=job_hls_dir,
                log_path=log_path,
                source_width=asset.source_width,
                source_height=asset.source_height,
                source_fps=asset.source_fps,
                has_audio=asset.has_audio,
                duration_sec=asset.duration_seconds,
                heartbeat_callback=heartbeat_callback,
                progress_callback=progress_callback
            )
            
            job.heartbeat = datetime.now(timezone.utc)
            db.commit()
            
            # Transition to VALIDATING
            transition_asset(db, asset, VideoStatus.VALIDATING)
            db.commit()
            
            # 3. Validate Output
            # Pass LADDER to validate_hls_output or use selected_variants
            # Because get_selected_variants is the source of truth, we recalculate it here for validation
            expected_variants = get_selected_variants(asset.source_width, asset.source_height)
            validated_renditions = validate_hls_output(job_hls_dir, expected_variants, asset.duration_seconds, asset.has_audio, asset.source_width, asset.source_height)
            
            # 4. Atomic Promotion
            if asset_out_dir.exists():
                raise TranscodeError("E_OUTPUT_EXISTS", f"Output directory already exists: {asset_out_dir}")
                
            asset_out_dir.parent.mkdir(parents=True, exist_ok=True)
            
            # Check devices
            if job_hls_dir.stat().st_dev != asset_out_dir.parent.stat().st_dev:
                raise TranscodeError("E_CROSS_DEVICE_LINK", "Staging and Output are on different devices")
                
            try:
                os.replace(str(job_hls_dir), str(asset_out_dir))
                # Sync parent dir
                try:
                    dir_fd = os.open(str(asset_out_dir.parent), os.O_RDONLY)
                    os.fsync(dir_fd)
                    os.close(dir_fd)
                except OSError:
                    pass
            except OSError as e:
                raise TranscodeError("E_ATOMIC_PROMOTION_FAILED", f"Atomic rename failed: {str(e)}")
            
            # 5. Save Renditions & Manifest
            manifest_relative = f"EnlacePlus/_definst_/amlst:{uuid_upper}/{asset.enlace_id}/manifest.m3u8"
            asset.manifest_path = manifest_relative
            cdn_base = settings.normalized_cdn_url
            asset.manifest_url = f"{cdn_base}/{manifest_relative}"
            asset.progress = 100
            
            # Parse variants from output to create Rendition rows
            for v_name, v_meta in validated_renditions.items():
                rendition = Rendition(
                    asset_id=asset.id,
                    name=v_name,
                    width=v_meta["width"],
                    height=v_meta["height"],
                    video_bitrate=v_meta["bandwidth"],
                    audio_bitrate=128000 if asset.has_audio else 0,
                    playlist_path=f"{v_name}/playlist.m3u8",
                    duration_seconds=v_meta["duration"],
                    segment_count=v_meta["segment_count"]
                )
                db.add(rendition)
            
            transition_asset(db, asset, VideoStatus.READY)
            asset.published_at = datetime.now(timezone.utc)
            
            job.status = JobStatus.COMPLETED
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            
            # Step 5: Archive the source file, independent commit
            try:
                archive_source_file(db, asset)
                db.commit()
            except Exception as e:
                db.rollback()
                logger.exception("Failed to archive source file, but asset is already READY")
            
        except TranscodeError as e:
            db.rollback()
            err_msg = truncate_error(e.message)
            fail_asset(db, asset, e.code, err_msg)
            job.status = JobStatus.FAILED
            job.error_code = e.code
            job.error_message = err_msg
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
        except Exception as e:
            db.rollback()
            logger.exception("Unexpected error in transcode worker")
            err_msg = truncate_error(str(e))
            fail_asset(db, asset, "E_INTERNAL_ERROR", "Internal worker error")
            job.status = JobStatus.FAILED
            job.error_code = "E_INTERNAL_ERROR"
            job.error_message = err_msg
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
        finally:
            try:
                shutil.rmtree(job_staging_dir, ignore_errors=True)
            except OSError:
                pass

    finally:
        db.close()


def progressive_transcode_asset_job(job_id: UUID):
    """
    Worker task to progressively transcode an asset directly into its permanent canonical output path.
    Transitions: COLD/QUEUED -> PROCESSING -> PLAYABLE (at >= 5 segments) -> VALIDATING -> READY
    Maintains constant canonical manifest_url throughout all states.
    """
    db = SessionLocal()
    current_job = rq.get_current_job()
    worker_id = current_job.worker_name if current_job else "local"
    output_root = Path(settings.OUTPUT_ROOT).resolve(strict=True)

    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if not job or job.type != JobType.TRANSCODE:
            logger.error(f"Job not found or invalid type: {job_id}")
            return

        job.status = JobStatus.PROCESSING
        job.worker_id = worker_id
        job.started_at = datetime.now(timezone.utc)
        job.heartbeat = datetime.now(timezone.utc)

        log_root = Path(settings.LOG_ROOT)
        log_root.mkdir(parents=True, exist_ok=True)
        log_path = log_root.resolve(strict=True) / "jobs" / str(job_id) / "ffmpeg.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        job.log_path = str(log_path.relative_to(log_root.resolve(strict=True)))
        db.commit()

        asset = db.query(Asset).filter(Asset.id == job.asset_id).first()
        if not asset or asset.status not in [VideoStatus.QUEUED, VideoStatus.COLD, VideoStatus.PROCESSING]:
            job.status = JobStatus.FAILED
            job.error_message = f"Asset not found or invalid state: {asset.status if asset else 'None'}"
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return

        from src.core.canonical import get_canonical_output_dir, build_canonical_manifest_path, build_canonical_manifest_url
        canonical_dir = get_canonical_output_dir(output_root, asset.vod_uuid, asset.enlace_id)

        # Ensure canonical manifest_path and manifest_url are assigned if not already
        if not asset.manifest_path:
            asset.manifest_path = build_canonical_manifest_path(asset.vod_uuid, asset.enlace_id)
        if not asset.manifest_url:
            asset.manifest_url = build_canonical_manifest_url(asset.vod_uuid, asset.enlace_id)

        # Check existing directory rule:
        # - If already READY: do not overwrite
        if asset.status == VideoStatus.READY:
            logger.info(f"Asset {asset.vod_uuid} is already READY. Skipping transcode.")
            job.status = JobStatus.COMPLETED
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return

        # - If directory exists and contains manifest.m3u8 but asset is not PROCESSING/PLAYABLE:
        if canonical_dir.exists() and any(canonical_dir.iterdir()):
            if asset.status not in [VideoStatus.PROCESSING, VideoStatus.PLAYABLE]:
                err = f"Canonical output directory already exists with content: {canonical_dir}"
                fail_asset(db, asset, "E_OUTPUT_EXISTS", err)
                job.status = JobStatus.FAILED
                job.error_code = "E_OUTPUT_EXISTS"
                job.error_message = err
                job.finished_at = datetime.now(timezone.utc)
                db.commit()
                return

        # Transition to PROCESSING
        transition_asset(db, asset, VideoStatus.PROCESSING)
        db.commit()

        # Probe source if needed
        try:
            source_path = validate_ingest_path(asset.source_uri)
            if not source_path.is_file():
                raise SecurityError("E_SECURITY_PATH_IS_DIRECTORY", "Source is not a file")

            if not asset.duration_seconds or not asset.source_width or not asset.source_height:
                media_info = analyze_media(source_path)
                asset.duration_seconds = media_info["duration_seconds"]
                asset.source_width = media_info["source_width"]
                asset.source_height = media_info["source_height"]
                asset.source_fps = media_info.get("fps") or 30.0
                asset.video_codec = media_info["video_codec"]
                asset.audio_codec = media_info["audio_codec"]
                asset.has_audio = media_info["has_audio"]
                asset.probe_metadata = media_info["probe_metadata"]
                asset.size = source_path.stat().st_size
                db.commit()

        except (SecurityError, ProbeError) as e:
            db.rollback()
            err_msg = truncate_error(str(e))
            fail_asset(db, asset, getattr(e, "code", "E_PROBE_FAILED"), err_msg)
            job.status = JobStatus.FAILED
            job.error_code = getattr(e, "code", "E_PROBE_FAILED")
            job.error_message = err_msg
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return
        except Exception as e:
            db.rollback()
            err_msg = truncate_error(str(e))
            fail_asset(db, asset, "E_INTERNAL_ERROR", err_msg)
            job.status = JobStatus.FAILED
            job.error_code = "E_INTERNAL_ERROR"
            job.error_message = err_msg
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return

        # Determine ladder variants and encoder
        try:
            from src.worker.transcode import get_best_encoder, build_transcode_args, parse_ffmpeg_progress
            from src.worker.progressive_manager import parse_variant_playlist

            encoder = get_best_encoder()
            selected_variants = get_selected_variants(asset.source_width, asset.source_height)
        except Exception as e:
            db.rollback()
            err_msg = truncate_error(str(e))
            fail_asset(db, asset, getattr(e, "code", "E_ENCODER_UNAVAILABLE"), err_msg)
            job.status = JobStatus.FAILED
            job.error_code = getattr(e, "code", "E_ENCODER_UNAVAILABLE")
            job.error_message = err_msg
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return

        canonical_dir.mkdir(parents=True, exist_ok=True)
        for k in selected_variants.keys():
            (canonical_dir / k).mkdir(parents=True, exist_ok=True)

        args = build_transcode_args(
            source_path=source_path,
            output_dir=canonical_dir,
            source_width=asset.source_width,
            source_height=asset.source_height,
            source_fps=asset.source_fps or 30.0,
            has_audio=asset.has_audio,
            encoder=encoder,
            selected_variants=selected_variants,
            hls_playlist_type="event",
            hls_flags="independent_segments+temp_file",
            hls_list_size=0
        )

        with open(log_path, "w") as log_file:
            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=log_file,
                text=True,
                shell=False,
                start_new_session=True,
            )

            out_queue = queue.Queue()
            stop_event = threading.Event()

            def reader_thread(pipe, q, stop_ev):
                try:
                    for line in iter(pipe.readline, ''):
                        if stop_ev.is_set():
                            break
                        q.put(line)
                except Exception:
                    pass
                finally:
                    try:
                        pipe.close()
                    except Exception:
                        pass
                    q.put(None)

            t_reader = threading.Thread(target=reader_thread, args=(process.stdout, out_queue, stop_event), daemon=True)
            t_reader.start()

            total_us = int((asset.duration_seconds or 1.0) * 1_000_000)
            last_heartbeat = time.monotonic()
            last_db_persist = time.monotonic()
            last_pct = 0
            is_playable = False

            while True:
                now = time.monotonic()

                # Heartbeat every 5s
                if now - last_heartbeat > settings.HEARTBEAT_INTERVAL_SECONDS:
                    job.heartbeat = datetime.now(timezone.utc)
                    db.commit()
                    last_heartbeat = now

                # Drain stdout lines for progress
                while True:
                    try:
                        line = out_queue.get_nowait()
                        if line is None:
                            break
                        us_done = parse_ffmpeg_progress(line)
                        if us_done is not None and total_us > 0:
                            pct = min(max(int((us_done / total_us) * 100), 0), 99)
                            if pct > last_pct:
                                last_pct = pct
                    except queue.Empty:
                        break

                # Count segments per variant
                counts: dict[str, int] = {}
                durs: dict[str, float] = {}
                for k in selected_variants.keys():
                    v_pl = canonical_dir / k / "playlist.m3u8"
                    c, d, _ = parse_variant_playlist(v_pl)
                    counts[k] = c
                    durs[k] = d

                if all(counts.get(k, 0) > 0 for k in selected_variants.keys()):
                    min_dur = min(durs.values())
                else:
                    min_dur = 0.0

                min_segments = settings.PROGRESSIVE_MIN_SEGMENTS
                if not is_playable and all(counts.get(k, 0) >= min_segments for k in selected_variants.keys()):
                    is_playable = True
                    asset.progress = last_pct
                    asset.available_until_seconds = min_dur
                    transition_asset(db, asset, VideoStatus.PLAYABLE, details={"available_until_seconds": min_dur, "segments": counts})
                    db.commit()
                    logger.info(f"Asset {asset.vod_uuid} is now PLAYABLE ({min_segments} segments, {min_dur:.1f}s)")
                elif now - last_db_persist > settings.PROGRESS_PERSIST_INTERVAL_SECONDS:
                    asset.progress = last_pct
                    asset.available_until_seconds = min_dur
                    db.commit()
                    last_db_persist = now

                if process.poll() is not None:
                    break

                time.sleep(0.5)

            stop_event.set()
            t_reader.join(timeout=2.0)
            returncode = process.wait()

            if returncode != 0:
                with open(log_path, "r") as lf:
                    lines = lf.readlines()
                    stderr_tail = "".join(lines[-20:]) if lines else "No stderr"
                err_msg = truncate_error(f"FFmpeg exit code {returncode}: {stderr_tail}")
                fail_asset(db, asset, "E_TRANSCODE_FAILED", err_msg)
                job.status = JobStatus.FAILED
                job.error_code = "E_TRANSCODE_FAILED"
                job.error_message = err_msg
                job.finished_at = datetime.now(timezone.utc)
                db.commit()
                return

            # Transition to VALIDATING
            transition_asset(db, asset, VideoStatus.VALIDATING)
            db.commit()

            try:
                validated_renditions = validate_hls_output(
                    canonical_dir,
                    selected_variants,
                    asset.duration_seconds,
                    asset.has_audio,
                    asset.source_width,
                    asset.source_height
                )

                for v_name, v_meta in validated_renditions.items():
                    existing_rendition = db.query(Rendition).filter_by(asset_id=asset.id, name=v_name).first()
                    if not existing_rendition:
                        rendition = Rendition(
                            asset_id=asset.id,
                            name=v_name,
                            width=v_meta["width"],
                            height=v_meta["height"],
                            video_bitrate=v_meta["bandwidth"],
                            audio_bitrate=128000 if asset.has_audio else 0,
                            playlist_path=f"{v_name}/playlist.m3u8",
                            duration_seconds=v_meta["duration"],
                            segment_count=v_meta["segment_count"]
                        )
                        db.add(rendition)
                    else:
                        existing_rendition.segment_count = v_meta["segment_count"]
                        existing_rendition.duration_seconds = v_meta["duration"]

                asset.progress = 100
                asset.available_until_seconds = asset.duration_seconds
                asset.published_at = datetime.now(timezone.utc)
                transition_asset(db, asset, VideoStatus.READY)
                job.status = JobStatus.COMPLETED
                job.finished_at = datetime.now(timezone.utc)
                db.commit()

                is_new_video_pipeline = db.query(IngestItem).filter(IngestItem.asset_id == asset.id).first() is not None
                if is_new_video_pipeline:
                    try:
                        schedule_post_ready_work(asset, db)
                    except Exception as e:
                        logger.exception(f"Failed to schedule post-ready work for asset {asset.vod_uuid}: {e}")
                else:
                    try:
                        archive_source_file(db, asset)
                        db.commit()
                    except Exception as e:
                        db.rollback()
                        logger.exception("Failed to archive source file, but asset is already READY")

            except TranscodeError as e:
                db.rollback()
                err_msg = truncate_error(e.message)
                logger.error(f"HLS validation failed for asset {asset.vod_uuid}: {err_msg}. Keeping output for diagnosis.")
                fail_asset(db, asset, e.code, err_msg)
                job.status = JobStatus.FAILED
                job.error_code = e.code
                job.error_message = err_msg
                job.finished_at = datetime.now(timezone.utc)
                db.commit()
            except Exception as e:
                db.rollback()
                logger.exception("Unexpected error validating progressive HLS")
                err_msg = truncate_error(str(e))
                fail_asset(db, asset, "E_HLS_VALIDATION_FAILED", err_msg)
                job.status = JobStatus.FAILED
                job.error_code = "E_HLS_VALIDATION_FAILED"
                job.error_message = err_msg
                job.finished_at = datetime.now(timezone.utc)
                db.commit()

    except Exception as e:
        logger.exception(f"Unexpected error in progressive worker for job {job_id}")
        db.rollback()
    finally:
        db.close()
