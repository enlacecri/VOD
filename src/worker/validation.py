import os
import json
import subprocess
from pathlib import Path
from typing import Dict, Any

from src.core.config import settings
from src.models.enums import E_HLS_VALIDATION_FAILED
from src.worker.transcode import TranscodeError, LADDER

def parse_master_playlist(master_content: str) -> Dict[str, Dict]:
    import re
    variants = {}
    lines = master_content.splitlines()
    current_stream_info = {}
    
    # Regex to match key=value or key="value" pairs
    # It matches word characters for the key, then an equals sign, 
    # then either a quoted string or unquoted non-comma characters.
    attr_re = re.compile(r'([A-Z0-9\-]+)=("([^"]*)"|([^,]*))')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line.startswith("#EXT-X-STREAM-INF:"):
            info = line.replace("#EXT-X-STREAM-INF:", "")
            
            for match in attr_re.finditer(info):
                key = match.group(1)
                # group(3) is the quoted value without quotes, group(4) is unquoted
                val = match.group(3) if match.group(3) is not None else match.group(4)
                current_stream_info[key] = val.strip()
                
        elif not line.startswith("#") and "/" in line:
            variant_name = line.split("/")[0]
            variants[variant_name] = current_stream_info
            current_stream_info = {}
            
    return variants

def validate_hls_output(hls_dir: Path, expected_variants: Dict[str, Dict], source_duration: float, has_audio: bool, source_width: int, source_height: int) -> Dict[str, Dict]:
    master_pl = hls_dir / "manifest.m3u8"
    if not master_pl.exists():
        raise TranscodeError(E_HLS_VALIDATION_FAILED, "Missing master playlist (manifest.m3u8)")

    if master_pl.stat().st_size > settings.PROBE_JSON_LIMIT_BYTES:
        raise TranscodeError(E_HLS_VALIDATION_FAILED, "Master playlist exceeds size limit")

    master_content = master_pl.read_text()
    actual_variants_meta = parse_master_playlist(master_content)
    
    if set(actual_variants_meta.keys()) != set(expected_variants.keys()):
        raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Variants mismatch. Expected: {list(expected_variants.keys())}, Actual: {list(actual_variants_meta.keys())}")
        
    validated_renditions = {}
    last_bandwidth = float('inf')
        
    for v_name in expected_variants.keys():
        variant_dir = hls_dir / v_name
        if not variant_dir.exists() or not variant_dir.is_dir():
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Variant directory '{v_name}' missing")
            
        variant_pl = variant_dir / "playlist.m3u8"
        if not variant_pl.exists():
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Variant playlist missing in '{v_name}'")
            
        if variant_pl.stat().st_size > settings.PROBE_JSON_LIMIT_BYTES * 5:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Variant playlist '{v_name}' exceeds size limit")
            
        variant_content = variant_pl.read_text()
        if "#EXT-X-ENDLIST" not in variant_content:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Missing #EXT-X-ENDLIST in variant '{v_name}'")
            
        segments = []
        for line in variant_content.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                if "/" in line or ".." in line or "http://" in line or "https://" in line:
                    raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Traversal or external URI detected in segment '{line}'")
                segments.append(line)
                
        if not segments:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"No segments found in variant '{v_name}'")
            
        for seg in segments:
            seg_path = variant_dir / seg
            if not seg_path.exists():
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Segment missing: '{seg}' in variant '{v_name}'")
            if not seg_path.is_file() or seg_path.stat().st_size == 0:
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Segment empty or not a regular file: '{seg}' in variant '{v_name}'")
            if seg_path.is_symlink():
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Segment is a symlink: '{seg}' in variant '{v_name}'")
                
        # Full decode check
        try:
            # Run ffmpeg to decode the entire playlist to null
            decode_cmd = [
                settings.FFMPEG_PATH, "-v", "error", 
                "-i", str(variant_pl), 
                "-f", "null", "-"
            ]
            result = subprocess.run(decode_cmd, capture_output=True, text=True, timeout=settings.HLS_VALIDATION_TIMEOUT_SECONDS)
            if result.returncode != 0:
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Full decode check failed for '{v_name}': {result.stderr}")
        except subprocess.TimeoutExpired:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Full decode check timed out for '{v_name}' after {settings.HLS_VALIDATION_TIMEOUT_SECONDS}s")
            
        # Get real resolution and duration via ffprobe
        try:
            probe_cmd = [
                settings.FFPROBE_PATH, "-v", "error", 
                "-show_format", "-show_streams", 
                "-of", "json", str(variant_pl)
            ]
            result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"ffprobe failed on variant '{v_name}': {result.stderr}")
                
            probe_data = json.loads(result.stdout)
            format_duration = float(probe_data.get("format", {}).get("duration", 0))
            
            if abs(format_duration - source_duration) > max(2.0, source_duration * 0.1):
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Variant '{v_name}' duration mismatch: {format_duration} vs {source_duration}")
                
            video_stream = next((s for s in probe_data.get("streams", []) if s.get("codec_type") == "video"), None)
            if not video_stream:
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"No video stream found in variant '{v_name}'")
                
            if video_stream.get("codec_name") != "h264":
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Video codec is not H.264 in variant '{v_name}'")
                
            real_width = video_stream.get("width", 0)
            real_height = video_stream.get("height", 0)
            
            # Check audio
            audio_stream = next((s for s in probe_data.get("streams", []) if s.get("codec_type") == "audio"), None)
            if has_audio and not audio_stream:
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Expected audio stream missing in variant '{v_name}'")
            if has_audio and audio_stream.get("codec_name") != "aac":
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Audio codec is not AAC in variant '{v_name}'")
            if not has_audio and audio_stream:
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Unexpected audio stream found in variant '{v_name}'")
                
        except subprocess.TimeoutExpired:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"ffprobe timeout on variant '{v_name}'")
        except (json.JSONDecodeError, ValueError) as e:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Invalid probe data for variant '{v_name}': {str(e)}")
            
        v_meta = actual_variants_meta.get(v_name, {})
        
        # Validation of CODECS
        codecs = v_meta.get("CODECS", "")
        if "avc1" not in codecs:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Missing avc1 codec in manifest for '{v_name}'")
        if has_audio and "mp4a" not in codecs:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Missing mp4a codec in manifest for '{v_name}'")
            
        # Validation of BANDWIDTH
        bandwidth = int(v_meta.get("BANDWIDTH", -1))
        if bandwidth <= 0:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Invalid bandwidth {bandwidth} for '{v_name}'")
        if bandwidth > last_bandwidth:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Bandwidth {bandwidth} for '{v_name}' is higher than previous {last_bandwidth} (must be descending)")
        last_bandwidth = bandwidth
        
        maxrate_str = expected_variants[v_name]["maxrate"].replace("k", "000")
        target_bandwidth = int(maxrate_str)
        max_allowed_bandwidth = int(target_bandwidth * settings.BANDWIDTH_OVERHEAD_MAX_RATIO) + 200000 # 200kbps static overhead margin for audio/container
        if bandwidth > max_allowed_bandwidth:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Bandwidth {bandwidth} for '{v_name}' exceeds maximum allowed {max_allowed_bandwidth}")
        
        # Validation of RESOLUTION metadata
        res_meta = v_meta.get("RESOLUTION", "")
        if res_meta:
            meta_w, meta_h = map(int, res_meta.split("x"))
            if meta_w != real_width or meta_h != real_height:
                raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Resolution mismatch in manifest vs real stream for '{v_name}'")
        
        # Validation of Source Bounding Box and Aspect Ratio
        if real_width > source_width or real_height > source_height:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Variant '{v_name}' actual resolution ({real_width}x{real_height}) exceeds source ({source_width}x{source_height})")
            
        source_aspect = source_width / source_height
        real_aspect = real_width / real_height
        if abs(source_aspect - real_aspect) > settings.ASPECT_RATIO_TOLERANCE:
            raise TranscodeError(E_HLS_VALIDATION_FAILED, f"Variant '{v_name}' aspect ratio {real_aspect:.3f} deviates from source {source_aspect:.3f}")
            
        validated_renditions[v_name] = {
            "width": real_width,
            "height": real_height,
            "bandwidth": bandwidth,
            "duration": format_duration,
            "segment_count": len(segments)
        }
        
    return validated_renditions
