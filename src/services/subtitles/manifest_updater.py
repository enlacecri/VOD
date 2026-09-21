import re
import uuid
from pathlib import Path
from typing import List, Dict

LANGUAGE_NAMES = {
    "es": "Español",
    "en": "English",
    "pt": "Português",
    "fr": "Français",
}

def format_vtt_timestamp(seconds: float) -> str:
    hrs = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int(round((seconds - int(seconds)) * 1000))
    if millis >= 1000:
        millis = 999
    return f"{hrs:02d}:{mins:02d}:{secs:02d}.{millis:03d}"

def generate_vtt_content(segments: list) -> str:
    lines = ["WEBVTT", ""]
    for idx, seg in enumerate(segments, 1):
        start_ts = format_vtt_timestamp(seg.start)
        end_ts = format_vtt_timestamp(seg.end)
        lines.append(f"{idx}")
        lines.append(f"{start_ts} --> {end_ts}")
        lines.append(seg.text)
        lines.append("")
    return "\n".join(lines)

def generate_subtitle_playlist(vtt_filename: str, duration: float) -> str:
    target_dur = max(1, int(duration + 1))
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{target_dur}",
        "#EXT-X-MEDIA-SEQUENCE:0",
        f"#EXTINF:{duration:.3f},",
        vtt_filename,
        "#EXT-X-ENDLIST",
        "",
    ]
    return "\n".join(lines)

def update_master_manifest_with_subtitles(
    master_manifest_path: Path,
    languages: List[str],
    default_lang: str = "es",
) -> None:
    """
    Atomically updates the HLS master manifest to integrate WebVTT subtitle tracks.
    Ensures:
      - Valid HLS #EXT-X-MEDIA:TYPE=SUBTITLES tags.
      - References SUBTITLES="subs" on variant stream playlists.
      - Atomic write via temporary file replace.
    """
    if not master_manifest_path.exists():
        raise FileNotFoundError(f"Master manifest does not exist: {master_manifest_path}")

    with open(master_manifest_path, "r", encoding="utf-8") as f:
        content = f.read()

    if not content.startswith("#EXTM3U"):
        raise ValueError(f"Invalid master manifest (missing #EXTM3U): {master_manifest_path}")

    # Build subtitle media tags
    sub_media_tags: List[str] = []
    for lang in languages:
        name = LANGUAGE_NAMES.get(lang, lang.upper())
        is_default = "YES" if lang == default_lang else "NO"
        tag = (
            f'#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="{name}",'
            f'DEFAULT={is_default},AUTOSELECT=YES,LANGUAGE="{lang}",'
            f'URI="subtitles/{lang}/index.m3u8"'
        )
        sub_media_tags.append(tag)

    # Filter out any existing SUBTITLES media tags to allow idempotent updates
    lines = content.splitlines()
    filtered_lines: List[str] = []
    inserted_media = False

    for line in lines:
        if line.startswith('#EXT-X-MEDIA:TYPE=SUBTITLES'):
            continue  # Replaced by new set

        # Insert subtitle media tags right before the first variant stream or at top after headers
        if (line.startswith("#EXT-X-STREAM-INF") or line.startswith("#EXTINF")) and not inserted_media:
            for sub_tag in sub_media_tags:
                filtered_lines.append(sub_tag)
            inserted_media = True

        # Ensure variant stream has SUBTITLES="subs" attribute
        if line.startswith("#EXT-X-STREAM-INF"):
            if 'SUBTITLES="subs"' not in line and "SUBTITLES=" not in line:
                line = line + ',SUBTITLES="subs"'
            elif "SUBTITLES=" in line and 'SUBTITLES="subs"' not in line:
                line = re.sub(r'SUBTITLES="[^"]*"', 'SUBTITLES="subs"', line)

        filtered_lines.append(line)

    # If not yet inserted (e.g. manifest has no stream tags yet)
    if not inserted_media:
        for sub_tag in sub_media_tags:
            filtered_lines.append(sub_tag)

    new_content = "\n".join(filtered_lines) + "\n"

    # Validation: must contain #EXTM3U and each sub_media_tag
    if not new_content.startswith("#EXTM3U"):
        raise ValueError("Generated master manifest validation failed")
    for sub_tag in sub_media_tags:
        if sub_tag not in new_content:
            raise ValueError(f"Missing expected subtitle tag: {sub_tag}")

    # Atomic write
    tmp_path = master_manifest_path.parent / f"{master_manifest_path.name}.{uuid.uuid4().hex[:8]}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(new_content)
            f.flush()
        tmp_path.replace(master_manifest_path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
