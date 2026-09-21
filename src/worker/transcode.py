import os
import signal
import subprocess
import time
import logging
from pathlib import Path
from typing import Dict, List, Callable, Optional, Any

from src.core.config import settings
from src.models.enums import (
    E_FFMPEG_NOT_FOUND,
    E_FFMPEG_TIMEOUT,
    E_ENCODER_UNAVAILABLE,
    E_TRANSCODE_FAILED,
    E_PROGRESS_INVALID
)

logger = logging.getLogger(__name__)

class TranscodeError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(self.message)

LADDER = {
    "0": {"w": 1920, "h": 1080, "vbr": "5000k", "maxrate": "5350k", "bufsize": "10000k"},
    "1": {"w": 1280, "h": 720,  "vbr": "2640k", "maxrate": "2800k", "bufsize": "5280k"},
    "2": {"w": 854,  "h": 480,  "vbr": "1024k", "maxrate": "1100k", "bufsize": "2048k"},
    "3": {"w": 640,  "h": 360,  "vbr": "512k",  "maxrate": "560k",  "bufsize": "1024k"}
}

def check_encoder_support(encoder: str) -> bool:
    process = None
    try:
        process = subprocess.Popen(
            [settings.FFMPEG_PATH, "-hide_banner", "-encoders"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        stdout, _ = process.communicate(
            timeout=settings.FFMPEG_ENCODER_CHECK_TIMEOUT_SECONDS
        )
        return process.returncode == 0 and encoder in stdout
    except FileNotFoundError:
        raise TranscodeError(E_FFMPEG_NOT_FOUND, "FFmpeg binary not found in PATH or configured path")
    except subprocess.TimeoutExpired as exc:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        raise TranscodeError(
            E_FFMPEG_TIMEOUT,
            "Timed out while checking available FFmpeg encoders",
        ) from exc

def get_best_encoder() -> str:
    # Intentionally check configured encoder
    encoder = settings.VIDEO_ENCODER
    if check_encoder_support(encoder):
        return encoder
    fallback = settings.VIDEO_ENCODER_FALLBACK
    if check_encoder_support(fallback):
        return fallback
    raise TranscodeError(E_ENCODER_UNAVAILABLE, f"Neither {encoder} nor {fallback} are supported by FFmpeg")

def get_selected_variants(source_width: int, source_height: int) -> Dict[str, Dict]:
    source_max = max(source_width, source_height)
    
    selected_variants = {}
    for k, v in LADDER.items():
        variant_max = max(v["w"], v["h"])
        # Only include variant if it doesn't cause upscale
        if source_max >= variant_max:
            selected_variants[k] = v
            
    if not selected_variants:
        # Source is smaller than the lowest ladder rung (360p)
        # Create a custom "source" variant preventing upscaling
        even_w = (source_width // 2) * 2
        even_h = (source_height // 2) * 2
        
        # Calculate bitrate proportionally based on 360p (640x360 -> 512k)
        area = even_w * even_h
        ref_area = 640 * 360
        calc_bitrate = int(512 * (area / ref_area))
        # Limit between 250k and 512k
        calc_bitrate = max(250, min(512, calc_bitrate))
        
        selected_variants["source"] = {
            "w": even_w,
            "h": even_h,
            "vbr": f"{calc_bitrate}k",
            "maxrate": f"{int(calc_bitrate * 1.1)}k",
            "bufsize": f"{calc_bitrate * 2}k"
        }
        
    return selected_variants

def build_transcode_args(
    source_path: Path,
    output_dir: Path,
    source_width: int,
    source_height: int,
    source_fps: float,
    has_audio: bool,
    encoder: str,
    selected_variants: Dict[str, Dict] = None,
    hls_playlist_type: str = "vod",
    hls_flags: str = "independent_segments",
    hls_list_size: Optional[int] = None
) -> List[str]:
    if selected_variants is None:
        selected_variants = get_selected_variants(source_width, source_height)

    args = [
        settings.FFMPEG_PATH,
        "-y",
        "-nostdin",
        "-hide_banner",
        "-nostats",
        "-progress", "pipe:1",
        "-i", str(source_path)
    ]
    
    # Map and build filter complex
    filter_complex = ""
    # Create the base video split
    filter_complex += f"[0:v]split={len(selected_variants)}"
    for k in selected_variants.keys():
        filter_complex += f"[v{k}]"
    filter_complex += ";"
    
    # Add scaling for each variant (ensure even dimensions)
    for k, v in selected_variants.items():
        filter_complex += f"[v{k}]scale=w={v['w']}:h={v['h']}:force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2[vout{k}];"
        
    args.extend(["-filter_complex", filter_complex.rstrip(";")])
    
    # We map video and audio for each stream
    # Each variant corresponds to a stream output
    stream_idx = 0
    variant_map = []
    
    for k, v in selected_variants.items():
        # Map video
        args.extend(["-map", f"[vout{k}]"])
        args.extend([f"-c:v:{stream_idx}", encoder])
        args.extend([f"-b:v:{stream_idx}", v["vbr"]])
        args.extend([f"-maxrate:v:{stream_idx}", v["maxrate"]])
        args.extend([f"-bufsize:v:{stream_idx}", v["bufsize"]])
        
        # Disable scene cut for keyframe alignment
        args.extend([f"-sc_threshold:v:{stream_idx}", "0"])
        
        # Calculate GOP based on FPS
        if source_fps and source_fps > 0:
            gop = int(round(source_fps * 2))
        else:
            gop = 60 # fallback 30fps
            
        args.extend([f"-g:v:{stream_idx}", str(gop)])
        args.extend([f"-keyint_min:v:{stream_idx}", str(gop)])
        
        if encoder == "h264_videotoolbox":
            args.extend([f"-profile:v:{stream_idx}", "main"])
        elif encoder == "libx264":
            args.extend([f"-preset:v:{stream_idx}", "veryfast"])
            args.extend([f"-profile:v:{stream_idx}", "main"])
            
        # Map audio if present
        if has_audio:
            args.extend(["-map", "0:a:0"])
            args.extend([f"-c:a:{stream_idx}", "aac"])
            args.extend([f"-b:a:{stream_idx}", "128k"])
            args.extend([f"-ac:a:{stream_idx}", "2"])
            args.extend([f"-ar:a:{stream_idx}", "44100"])
            
        variant_map.append(k)
        stream_idx += 1
        
    # HLS packaging settings
    hls_opts = [
        "-f", "hls",
        "-hls_time", str(settings.HLS_TIME),
    ]
    if hls_list_size is not None:
        hls_opts.extend(["-hls_list_size", str(hls_list_size)])
    hls_opts.extend([
        "-hls_playlist_type", hls_playlist_type,
        "-hls_flags", hls_flags,
        "-hls_segment_type", "mpegts",
        "-master_pl_name", "manifest.m3u8",
        "-hls_segment_filename", str(output_dir / "%v" / "seg_%04d.ts")
    ])
    args.extend(hls_opts)
    
    # Stream map for HLS
    args.extend(["-force_key_frames", f"expr:gte(t,n_forced*{settings.HLS_GOP_SECONDS})"])
    
    # e.g. -var_stream_map "v:0,a:0,name:0 v:1,a:1,name:1"
    stream_map = []
    for idx, k in enumerate(variant_map):
        if has_audio:
            stream_map.append(f"v:{idx},a:{idx},name:{k}")
        else:
            stream_map.append(f"v:{idx},name:{k}")
            
    args.extend(["-var_stream_map", " ".join(stream_map)])
    
    # Finally, output location for the variants
    args.append(str(output_dir / "%v" / "playlist.m3u8"))
    
    return args

def parse_ffmpeg_progress(line: str) -> Optional[int]:
    """
    Parses 'out_time_us=1234567' and returns microseconds.
    """
    if line.startswith("out_time_us="):
        val = line.split("=", 1)[1].strip()
        if val.lstrip('-').isdigit():
            return int(val)
    return None

import threading
import queue

def execute_transcode(
    source_path: Path,
    output_dir: Path,
    log_path: Path,
    source_width: int,
    source_height: int,
    source_fps: float,
    has_audio: bool,
    duration_sec: float,
    heartbeat_callback: Callable[[], None],
    progress_callback: Callable[[int], None]
):
    encoder = get_best_encoder()
    
    selected_variants = get_selected_variants(source_width, source_height)
    
    # Create subdirectories for variants
    try:
        output_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        raise TranscodeError("E_OUTPUT_EXISTS", f"Output directory already exists: {output_dir}")
        
    for k in selected_variants.keys():
        (output_dir / k).mkdir(exist_ok=False)
        
    args = build_transcode_args(source_path, output_dir, source_width, source_height, source_fps, has_audio, encoder, selected_variants)
    logger.info(f"FFmpeg command: {' '.join(args)}")
    
    # Ensure log directory exists
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(log_path, "w") as log_file:
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=log_file,
            text=True,
            shell=False,
            start_new_session=True,
        )
        
        total_us = int(duration_sec * 1_000_000)
        
        stop_event = threading.Event()
        
        # Non-blocking read via thread
        out_queue = queue.Queue()
        def reader_thread(pipe, q, stop_event):
            # We use iter(pipe.readline, '') but need to check stop_event occasionally.
            # However, readline might block, so we set a timeout if possible, or just let it close
            # pipe.close() from the main thread will break the readline in python 3.
            try:
                for line in iter(pipe.readline, ''):
                    if stop_event.is_set():
                        break
                    q.put(line)
            except ValueError:
                # Pipe closed
                pass
            finally:
                try:
                    pipe.close()
                except Exception:
                    pass
                q.put(None) # Sentinel
            
        t = threading.Thread(target=reader_thread, args=(process.stdout, out_queue, stop_event))
        t.daemon = True
        t.start()
        
        start_time = time.monotonic()
        last_heartbeat = start_time
        last_pct = 0
        last_progress_time = start_time
        
        try:
            while True:
                now = time.monotonic()
                if now - start_time > settings.FFMPEG_TIMEOUT_SECONDS:
                    raise subprocess.TimeoutExpired(process.args, settings.FFMPEG_TIMEOUT_SECONDS)
                    
                if now - last_heartbeat > settings.HEARTBEAT_INTERVAL_SECONDS:
                    heartbeat_callback()
                    last_heartbeat = now
                
                try:
                    line = out_queue.get(timeout=1.0)
                except queue.Empty:
                    if process.poll() is not None:
                        break
                    continue
                
                if line is None:
                    # Sentinel reached
                    if process.poll() is not None:
                        break
                    continue
                
                if line:
                    us_done = parse_ffmpeg_progress(line)
                    if us_done is not None and total_us > 0:
                        pct = int((us_done / total_us) * 100)
                        # Ensure progress doesn't go backwards
                        if pct < last_pct:
                            raise TranscodeError("E_PROGRESS_INVALID", "Progress decreased or is invalid")
                            
                        # Ensure progress does not go above 100
                        if pct > 100:
                            raise TranscodeError("E_PROGRESS_INVALID", f"Progress exceeded 100%: {pct}%")
                            
                        pct = min(max(pct, 0), 99)  # Cap at 99 until validated
                        
                        # Throttle updates
                        if pct > last_pct and (now - last_progress_time) > settings.PROGRESS_PERSIST_INTERVAL_SECONDS:
                            progress_callback(pct)
                            last_pct = pct
                            last_progress_time = now
                            
            # FFmpeg finished naturally
            returncode = process.wait(timeout=5)
            if returncode != 0:
                with open(log_path, "r") as f:
                    lines = f.readlines()
                    stderr_tail = "".join(lines[-20:]) if lines else "No stderr output"
                raise TranscodeError("E_TRANSCODE_FAILED", f"FFmpeg failed with code {returncode}: {stderr_tail}")
                
        except subprocess.TimeoutExpired as exc:
            raise TranscodeError(
                E_FFMPEG_TIMEOUT,
                f"FFmpeg exceeded the {settings.FFMPEG_TIMEOUT_SECONDS}s timeout",
            ) from exc
        finally:
            stop_event.set()

            # Stop the complete FFmpeg process group before closing stdout. A
            # child process may inherit the pipe and otherwise keep the reader
            # thread (and close()) blocked indefinitely.
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=settings.FFMPEG_GRACEFUL_STOP_SECONDS)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)

            if process.stdout:
                try:
                    process.stdout.close()
                except Exception:
                    pass

            t.join(timeout=2.0)
            if t.is_alive():
                logger.warning("Reader thread did not terminate in time")
            if process.poll() is None:
                raise TranscodeError("E_PROCESS_STUCK", "FFmpeg process is stuck and could not be killed")
