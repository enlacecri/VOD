import pytest
from pathlib import Path
from src.worker.transcode import build_transcode_args, parse_ffmpeg_progress, LADDER

def test_parse_ffmpeg_progress():
    assert parse_ffmpeg_progress("out_time_us=1234567") == 1234567
    assert parse_ffmpeg_progress("out_time_us=0") == 0
    assert parse_ffmpeg_progress("out_time_us=-1000") == -1000
    assert parse_ffmpeg_progress("frame=100") is None

def test_build_transcode_args_no_upscale():
    # 720p source should skip 1080p variant
    args = build_transcode_args(
        source_path=Path("/src.mp4"),
        output_dir=Path("/out"),
        source_width=1280,
        source_height=720,
        source_fps=30.0,
        has_audio=True,
        encoder="libx264"
    )
    
    args_str = " ".join(args)
    
    # It should split into 3 streams (720p, 480p, 360p)
    assert "split=3" in args_str
    assert "scale=w=1920:h=1080" not in args_str
    assert "scale=w=1280:h=720" in args_str
    assert "scale=w=854:h=480" in args_str
    assert "scale=w=640:h=360" in args_str
    
def test_build_transcode_args_vertical_video():
    # 1080x1920 source should generate 1080p variant (even if width is 1080)
    args = build_transcode_args(
        source_path=Path("/src.mp4"),
        output_dir=Path("/out"),
        source_width=1080,
        source_height=1920,
        source_fps=30.0,
        has_audio=True,
        encoder="libx264"
    )
    
    args_str = " ".join(args)
    # It should split into 4 streams (1080p, 720p, 480p, 360p)
    assert "split=4" in args_str
    assert "scale=w=1920:h=1080" in args_str

def test_build_transcode_args_no_audio():
    args = build_transcode_args(
        source_path=Path("/src.mp4"),
        output_dir=Path("/out"),
        source_width=1920,
        source_height=1080,
        source_fps=30.0,
        has_audio=False,
        encoder="libx264"
    )
    
    args_str = " ".join(args)
    # Ensure no audio mapping is added
    assert "-c:a" not in args_str
    assert "a:0" not in args_str
    assert "v:0,name:0" in args_str  # No audio in stream map

def test_build_transcode_args_videotoolbox():
    args = build_transcode_args(
        source_path=Path("/src.mp4"),
        output_dir=Path("/out"),
        source_width=1920,
        source_height=1080,
        source_fps=30.0,
        has_audio=True,
        encoder="h264_videotoolbox"
    )
    
    args_str = " ".join(args)
    assert "-c:v:0 h264_videotoolbox" in args_str
    # Ensure profile is set
    assert "-profile:v:0 main" in args_str
    # Ensure preset is NOT set (videotoolbox doesn't use it)
    assert "-preset" not in args_str
