import json
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional
from src.core.config import settings

class ProbeError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)

def parse_rational(r_str: str) -> Optional[float]:
    if not r_str or r_str == "N/A" or r_str == "0/0":
        return None
    try:
        parts = r_str.split("/")
        if len(parts) == 2:
            num, den = float(parts[0]), float(parts[1])
            if den == 0:
                return None
            return num / den
        return float(r_str)
    except (ValueError, TypeError):
        return None

def parse_float_safe(f_str: str) -> Optional[float]:
    if not f_str or f_str == "N/A":
        return None
    try:
        return float(f_str)
    except (ValueError, TypeError):
        return None

def run_ffprobe(file_path: Path, heartbeat_callback=None) -> Dict[str, Any]:
    import time
    args = [
        settings.FFPROBE_PATH,
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        str(file_path)
    ]
    try:
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        start_time = time.time()
        last_heartbeat = start_time
        
        while process.poll() is None:
            time.sleep(0.1)
            now = time.time()
            if now - start_time > settings.FFPROBE_TIMEOUT_SECONDS:
                process.kill()
                raise ProbeError("E_PROBE_TIMEOUT", f"FFprobe exceeded {settings.FFPROBE_TIMEOUT_SECONDS}s timeout")
                
            if heartbeat_callback and (now - last_heartbeat > settings.RQ_JOB_TIMEOUT_SECONDS * 0.25):
                heartbeat_callback()
                last_heartbeat = now
                
        stdout, _ = process.communicate()
        
    except FileNotFoundError:
        raise ProbeError("E_PROBE_NOT_FOUND", "FFprobe executable not found")

    if process.returncode != 0:
        raise ProbeError("E_PROBE_FAILED", f"FFprobe failed with return code {process.returncode}")

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError:
        raise ProbeError("E_INVALID_PROBE_OUTPUT", "FFprobe returned invalid JSON")

    return data

def sanitize_probe_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Removes sensitive absolute paths and truncates massive tags."""
    if "format" in data:
        if "filename" in data["format"]:
            del data["format"]["filename"]
        if "tags" in data["format"]:
            for key, val in list(data["format"]["tags"].items()):
                if isinstance(val, str) and len(val) > 256:
                    data["format"]["tags"][key] = val[:256] + "...[TRUNCATED]"

    for stream in data.get("streams", []):
        if "tags" in stream:
            for key, val in list(stream["tags"].items()):
                if isinstance(val, str) and len(val) > 256:
                    stream["tags"][key] = val[:256] + "...[TRUNCATED]"

    json_str = json.dumps(data)
    if len(json_str) > settings.PROBE_JSON_LIMIT_BYTES:
        # Instead of generic {"error": ...}, aggressively strip to preserve structure
        if "format" in data and "tags" in data["format"]:
            del data["format"]["tags"]
        for stream in data.get("streams", []):
            if "tags" in stream:
                del stream["tags"]
        
        # Check again
        json_str = json.dumps(data)
        if len(json_str) > settings.PROBE_JSON_LIMIT_BYTES:
            # If it STILL exceeds, keep only the strictly necessary basic fields
            safe_data = {"format": {"duration": data.get("format", {}).get("duration")}, "streams": []}
            for s in data.get("streams", []):
                safe_data["streams"].append({
                    "codec_type": s.get("codec_type"),
                    "codec_name": s.get("codec_name"),
                    "width": s.get("width"),
                    "height": s.get("height")
                })
            data = safe_data
            
    return data

def get_video_score(stream: Dict[str, Any]) -> tuple:
    # Tuple sorting: default flag (1 > 0), resolution, then negative index (smaller index is better)
    is_default = stream.get("disposition", {}).get("default", 0)
    width = stream.get("width") or 0
    height = stream.get("height") or 0
    res = width * height
    idx = stream.get("index", 999)
    return (is_default, res, -idx)

def analyze_media(file_path: Path, heartbeat_callback=None) -> Dict[str, Any]:
    raw_data = run_ffprobe(file_path, heartbeat_callback=heartbeat_callback)
    
    video_streams = [s for s in raw_data.get("streams", []) if s.get("codec_type") == "video"]
    audio_streams = [s for s in raw_data.get("streams", []) if s.get("codec_type") == "audio"]
    
    if not video_streams:
        raise ProbeError("E_NO_VIDEO_STREAM", "No video stream found in file")
        
    main_video = max(video_streams, key=get_video_score)
    
    has_audio = len(audio_streams) > 0
    main_audio = audio_streams[0] if has_audio else None
    
    duration = parse_float_safe(main_video.get("duration"))
    if duration is None:
        duration = parse_float_safe(raw_data.get("format", {}).get("duration"))

    fps = parse_rational(main_video.get("avg_frame_rate"))
    if fps is None:
        fps = parse_rational(main_video.get("r_frame_rate"))

    width = main_video.get("width")
    height = main_video.get("height")
    
    if not isinstance(width, int) or width <= 0 or not isinstance(height, int) or height <= 0:
        raise ProbeError("E_INVALID_PROBE_OUTPUT", "Video width or height are invalid or not positive integers")

    sanitized_metadata = sanitize_probe_data(raw_data)

    return {
        "duration_seconds": duration,
        "source_width": width,
        "source_height": height,
        "video_codec": main_video.get("codec_name"),
        "audio_codec": main_audio.get("codec_name") if main_audio else None,
        "has_audio": has_audio,
        "fps": fps,
        "probe_metadata": sanitized_metadata
    }
