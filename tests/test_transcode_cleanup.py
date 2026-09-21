import pytest
import time
import subprocess
import threading
from pathlib import Path

from src.core.config import settings
from src.worker.transcode import execute_transcode, TranscodeError

def test_transcode_timeout_cleanup(tmp_path):
    # We will simulate a fake ffmpeg that sleeps indefinitely
    fake_ffmpeg = tmp_path / "fake_ffmpeg.sh"
    fake_ffmpeg.write_text(
        "#!/bin/bash\n"
        "if [[ \"$*\" == *\"-encoders\"* ]]; then\n"
        "  echo ' V..... h264_videotoolbox'\n"
        "  exit 0\n"
        "fi\n"
        "while true; do sleep 1; done\n"
    )
    fake_ffmpeg.chmod(0o755)
    
    settings.FFMPEG_PATH = str(fake_ffmpeg)
    settings.FFMPEG_TIMEOUT_SECONDS = 2
    settings.FFMPEG_GRACEFUL_STOP_SECONDS = 1
    
    out_dir = tmp_path / "out"
    log_file = tmp_path / "ffmpeg.log"
    log_file.write_text("")
    
    initial_thread_count = threading.active_count()
    
    with pytest.raises(TranscodeError) as exc_info:
        execute_transcode(
            source_path=tmp_path / "dummy.mp4",
            output_dir=out_dir,
            log_path=log_file,
            source_width=1280,
            source_height=720,
            source_fps=30.0,
            has_audio=True,
            duration_sec=30.0,
            heartbeat_callback=lambda: None,
            progress_callback=lambda x: None
        )
        
    assert exc_info.value.code == "E_FFMPEG_TIMEOUT"
    
    # Check that thread is gone
    time.sleep(0.5)
    final_thread_count = threading.active_count()
    assert final_thread_count == initial_thread_count


def test_encoder_detection_timeout_cleans_process_group(tmp_path):
    fake_ffmpeg = tmp_path / "fake_ffmpeg.sh"
    fake_ffmpeg.write_text("#!/bin/bash\nwhile true; do sleep 1; done\n")
    fake_ffmpeg.chmod(0o755)

    settings.FFMPEG_PATH = str(fake_ffmpeg)
    settings.FFMPEG_ENCODER_CHECK_TIMEOUT_SECONDS = 1

    with pytest.raises(TranscodeError) as exc_info:
        execute_transcode(
            source_path=tmp_path / "dummy.mp4",
            output_dir=tmp_path / "out",
            log_path=tmp_path / "ffmpeg.log",
            source_width=1280,
            source_height=720,
            source_fps=30.0,
            has_audio=True,
            duration_sec=30.0,
            heartbeat_callback=lambda: None,
            progress_callback=lambda x: None,
        )

    assert exc_info.value.code == "E_FFMPEG_TIMEOUT"
    
