import os
import signal
import subprocess
import time
import uuid
import re
import queue
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

from src.core.config import settings
from src.core.security import validate_ingest_path, SecurityError
from src.worker.probe import analyze_media, ProbeError
from src.worker.transcode import (
    build_transcode_args,
    get_best_encoder,
    get_selected_variants,
    parse_ffmpeg_progress,
    TranscodeError
)

logger = logging.getLogger(__name__)

# Regular expressions for playlist parsing
EXTINF_RE = re.compile(r'#EXTINF:([\d\.]+),?')

class ProgressiveSession:
    def __init__(self, session_uuid: str, source_uri: str, source_path: Path):
        self.session_uuid = session_uuid
        self.source_uri = source_uri
        self.source_path = source_path
        self.session_dir = Path(settings.PROGRESSIVE_ROOT).resolve() / session_uuid
        
        self.status = "STARTING"
        self.progress = 0.0
        self.available_until_seconds = 0.0
        self.duration_seconds = 0.0
        self.manifest_url = f"http://localhost:8080/progressive/{session_uuid}/manifest.m3u8"
        self.error_message: Optional[str] = None
        
        self.selected_variants: Dict[str, Dict] = {}
        self.variant_segments: Dict[str, int] = {}
        self.variant_durations: Dict[str, float] = {}
        
        self.created_at = datetime.now(timezone.utc)
        self.started_at: Optional[datetime] = None
        self.playable_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        
        self.process: Optional[Any] = None
        self.log_path = self.session_dir / "ffmpeg.log"
        self._lock = threading.RLock()

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "session_uuid": self.session_uuid,
                "status": self.status,
                "progress": round(self.progress, 1),
                "available_until_seconds": round(self.available_until_seconds, 1),
                "duration_seconds": round(self.duration_seconds, 1),
                "manifest_url": self.manifest_url,
                "error_message": self.error_message,
                "active_variants": list(self.selected_variants.keys()),
                "segments_per_variant": dict(self.variant_segments),
                "created_at": self.created_at.isoformat(),
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "playable_at": self.playable_at.isoformat() if self.playable_at else None,
                "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            }

    def save_auxiliary_state(self):
        try:
            import json
            state_file = self.session_dir / "session.json"
            state_data = self.to_dict()
            with open(state_file, "w") as f:
                json.dump(state_data, f, indent=2)
        except Exception:
            pass


class ProgressiveSessionManager:
    def __init__(self):
        self._sessions: Dict[str, ProgressiveSession] = {}
        self._lock = threading.RLock()

    def get_session(self, session_uuid: str) -> Optional[ProgressiveSession]:
        with self._lock:
            return self._sessions.get(session_uuid)

    def start_session(self, source_uri: str) -> ProgressiveSession:
        # 1. Path safety validation
        source_path = validate_ingest_path(source_uri)
        if not source_path.is_file():
            raise SecurityError("E_SECURITY_PATH_IS_DIRECTORY", "Source path is not a file")

        session_uuid = str(uuid.uuid4())
        session = ProgressiveSession(session_uuid, source_uri, source_path)
        
        with self._lock:
            self._sessions[session_uuid] = session

        logger.info(f"[PROGRESSIVE] session created: {session_uuid} for source: {source_uri}")

        # Launch background transcode and monitor thread
        t = threading.Thread(
            target=self._run_transcode_session,
            args=(session,),
            name=f"progressive-{session_uuid}",
            daemon=True
        )
        t.start()

        return session

    def _parse_variant_playlist(self, playlist_path: Path) -> (int, float, List[str]):
        """
        Parses an HLS variant playlist to extract:
        - segment count
        - total accumulated duration
        - list of segment file names
        Verifies that each segment file exists and is not empty.
        """
        if not playlist_path.exists():
            return 0, 0.0, []

        try:
            content = playlist_path.read_text()
        except Exception:
            return 0, 0.0, []

        lines = content.splitlines()
        segments = []
        durations = []
        current_duration = 0.0

        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith("#EXTINF:"):
                match = EXTINF_RE.match(line)
                if match:
                    try:
                        current_duration = float(match.group(1))
                    except ValueError:
                        current_duration = 0.0
            elif not line.startswith("#"):
                seg_name = line
                seg_path = playlist_path.parent / seg_name
                # Only count segment if it physically exists on disk and is non-empty
                if seg_path.exists() and seg_path.is_file() and seg_path.stat().st_size > 0:
                    segments.append(seg_name)
                    durations.append(current_duration)
                current_duration = 0.0

        return len(segments), sum(durations), segments

    def _run_transcode_session(self, session: ProgressiveSession):
        session_dir = session.session_dir
        try:
            session_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            with session._lock:
                session.status = "FAILED"
                session.error_message = f"Failed to create progressive directory: {str(e)}"
            logger.error(f"[PROGRESSIVE] failed to create directory: {e}")
            return

        # 2. Probe media
        try:
            media_info = analyze_media(session.source_path)
            duration_sec = media_info["duration_seconds"]
            source_width = media_info["source_width"]
            source_height = media_info["source_height"]
            source_fps = media_info.get("fps") or 30.0
            has_audio = media_info["has_audio"]
            
            with session._lock:
                session.duration_seconds = duration_sec
        except Exception as e:
            with session._lock:
                session.status = "FAILED"
                session.error_message = f"Media probe failed: {str(e)}"
            logger.error(f"[PROGRESSIVE] failed probing source for session {session.session_uuid}: {e}")
            session.save_auxiliary_state()
            return

        # 3. Determine encoder and ladder variants
        try:
            encoder = get_best_encoder()
            selected_variants = get_selected_variants(source_width, source_height)
            with session._lock:
                session.selected_variants = selected_variants
                for k in selected_variants.keys():
                    session.variant_segments[k] = 0
                    session.variant_durations[k] = 0.0
        except Exception as e:
            with session._lock:
                session.status = "FAILED"
                session.error_message = f"Variant/Encoder resolution failed: {str(e)}"
            logger.error(f"[PROGRESSIVE] encoder error: {e}")
            session.save_auxiliary_state()
            return

        # Create subdirectories for each variant
        for k in selected_variants.keys():
            (session_dir / k).mkdir(exist_ok=True)

        # 4. Build FFmpeg command
        args = build_transcode_args(
            source_path=session.source_path,
            output_dir=session_dir,
            source_width=source_width,
            source_height=source_height,
            source_fps=source_fps,
            has_audio=has_audio,
            encoder=encoder,
            selected_variants=selected_variants,
            hls_playlist_type="event",
            hls_flags="independent_segments+temp_file",
            hls_list_size=0
        )

        with open(session.log_path, "w") as log_file:
            try:
                process = subprocess.Popen(
                    args,
                    stdout=subprocess.PIPE,
                    stderr=log_file,
                    text=True,
                    shell=False,
                    start_new_session=True,
                )
                with session._lock:
                    session.process = process
                    session.started_at = datetime.now(timezone.utc)
                    session.status = "PROCESSING"
                logger.info(f"[PROGRESSIVE] ffmpeg started (pid={process.pid}) for session {session.session_uuid}")
                session.save_auxiliary_state()
            except Exception as e:
                with session._lock:
                    session.status = "FAILED"
                    session.error_message = f"Failed to start FFmpeg: {str(e)}"
                logger.error(f"[PROGRESSIVE] FFmpeg launch failed: {e}")
                session.save_auxiliary_state()
                return

            # Read stdout asynchronously for progress
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

            total_us = int(duration_sec * 1_000_000)
            last_progress = 0.0
            master_pl_logged = False
            variant_pl_logged = set()
            known_segments: Dict[str, set] = {k: set() for k in selected_variants.keys()}

            # Monitoring loop
            while True:
                # 1. Drain progress lines from stdout
                while True:
                    try:
                        line = out_queue.get_nowait()
                        if line is None:
                            break
                        us_done = parse_ffmpeg_progress(line)
                        if us_done is not None and total_us > 0:
                            pct = min(max((us_done / total_us) * 100.0, 0.0), 99.0)
                            if pct >= last_progress:
                                with session._lock:
                                    session.progress = pct
                                last_progress = pct
                    except queue.Empty:
                        break

                # 2. Check master playlist appearance
                master_pl = session_dir / "manifest.m3u8"
                if not master_pl_logged and master_pl.exists():
                    master_pl_logged = True
                    logger.info(f"[PROGRESSIVE] master playlist created for session {session.session_uuid}")

                # 3. Check variant playlists & count segments
                counts: Dict[str, int] = {}
                durs: Dict[str, float] = {}

                for k in selected_variants.keys():
                    v_pl = session_dir / k / "playlist.m3u8"
                    if k not in variant_pl_logged and v_pl.exists():
                        variant_pl_logged.add(k)
                        logger.info(f"[PROGRESSIVE] rendition playlist created for variant {k}")

                    count, dur, seg_list = self._parse_variant_playlist(v_pl)
                    counts[k] = count
                    durs[k] = dur

                    # Check for new segments to log
                    for seg in seg_list:
                        if seg not in known_segments[k]:
                            known_segments[k].add(seg)
                            logger.info(f"[PROGRESSIVE] segment produced: {k}/{seg}")

                # Update session metrics
                with session._lock:
                    session.variant_segments = counts
                    session.variant_durations = durs

                    # available_until_seconds is the minimum across ALL active variants
                    if all(counts.get(k, 0) > 0 for k in selected_variants.keys()):
                        min_dur = min(durs.values())
                    else:
                        min_dur = 0.0
                    session.available_until_seconds = min_dur

                    # PLAYABLE condition:
                    # Every active variant must have at least PROGRESSIVE_MIN_SEGMENTS (5)
                    min_segments = settings.PROGRESSIVE_MIN_SEGMENTS
                    if session.status in ["STARTING", "PROCESSING"]:
                        if all(counts.get(k, 0) >= min_segments for k in selected_variants.keys()):
                            session.status = "PLAYABLE"
                            session.playable_at = datetime.now(timezone.utc)
                            logger.info(
                                f"[PROGRESSIVE] playable threshold reached for session {session.session_uuid} "
                                f"({min_segments} segments, available: {min_dur:.1f}s)"
                            )
                            session.save_auxiliary_state()
                        elif master_pl_logged:
                            session.status = "PROCESSING"

                # Check if FFmpeg has exited
                ret = process.poll()
                if ret is not None:
                    break

                time.sleep(0.5)

            # Wait for reader thread to finish
            stop_event.set()
            t_reader.join(timeout=2.0)

            # Final evaluation
            returncode = process.wait()
            # Final re-parse of playlists to ensure complete capture
            counts = {}
            durs = {}
            for k in selected_variants.keys():
                v_pl = session_dir / k / "playlist.m3u8"
                count, dur, _ = self._parse_variant_playlist(v_pl)
                counts[k] = count
                durs[k] = dur

            with session._lock:
                session.variant_segments = counts
                session.variant_durations = durs
                if durs:
                    session.available_until_seconds = min(durs.values())

                if returncode == 0:
                    session.status = "COMPLETED"
                    session.progress = 100.0
                    session.completed_at = datetime.now(timezone.utc)
                    logger.info(
                        f"[PROGRESSIVE] ffmpeg completed for session {session.session_uuid} "
                        f"(available: {session.available_until_seconds:.1f}s)"
                    )
                else:
                    session.status = "FAILED"
                    # Tail log file for error details
                    try:
                        with open(session.log_path, "r") as lf:
                            err_lines = lf.readlines()
                            session.error_message = "".join(err_lines[-10:]) if err_lines else f"FFmpeg exited with code {returncode}"
                    except Exception:
                        session.error_message = f"FFmpeg exited with code {returncode}"
                    logger.error(f"[PROGRESSIVE] failed for session {session.session_uuid}: {session.error_message}")

                session.save_auxiliary_state()


# Global singleton manager for progressive sessions
progressive_manager = ProgressiveSessionManager()
