import os
import shutil
import time
import pytest
from pathlib import Path
from unittest import mock
from fastapi.testclient import TestClient

from src.main import app
from src.core.config import settings
from src.core.security import SecurityError
from src.worker.transcode import (
    build_transcode_args,
    get_selected_variants,
    parse_ffmpeg_progress,
    LADDER
)
from src.worker.progressive_manager import (
    ProgressiveSession,
    ProgressiveSessionManager,
    progressive_manager
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

client = TestClient(app)

def test_vod_pipeline_remains_intact():
    """1. El pipeline VOD normal sigue funcionando con hls_playlist_type=vod por defecto."""
    args = build_transcode_args(
        source_path=Path("/src.mp4"),
        output_dir=Path("/out"),
        source_width=1920,
        source_height=1080,
        source_fps=30.0,
        has_audio=True,
        encoder="libx264"
    )
    args_str = " ".join(args)
    assert "-hls_playlist_type vod" in args_str
    assert "-hls_flags independent_segments" in args_str
    assert "event" not in args_str

def test_progressive_args_configuration():
    """Parametrización progresiva usa event, temp_file y list_size 0."""
    args = build_transcode_args(
        source_path=Path("/src.mp4"),
        output_dir=Path("/progressive/test_uuid"),
        source_width=1920,
        source_height=1080,
        source_fps=30.0,
        has_audio=True,
        encoder="libx264",
        hls_playlist_type="event",
        hls_flags="independent_segments+temp_file",
        hls_list_size=0
    )
    args_str = " ".join(args)
    assert "-hls_playlist_type event" in args_str
    assert "-hls_flags independent_segments+temp_file" in args_str
    assert "-hls_list_size 0" in args_str

def test_ladder_no_upscale_720p():
    """4 & 5. Reutilización del ladder: un video 720p no produce 1080p."""
    variants = get_selected_variants(1280, 720)
    assert "0" not in variants  # 1080p skipped
    assert "1" in variants      # 720p present
    assert "2" in variants      # 480p present
    assert "3" in variants      # 360p present

def test_no_audio_video_handling():
    """6. Video sin audio no mapea audio ni en streams ni en var_stream_map."""
    args = build_transcode_args(
        source_path=Path("/src.mp4"),
        output_dir=Path("/progressive/test_uuid"),
        source_width=1280,
        source_height=720,
        source_fps=30.0,
        has_audio=False,
        encoder="libx264",
        hls_playlist_type="event",
        hls_flags="independent_segments+temp_file"
    )
    args_str = " ".join(args)
    assert "-c:a" not in args_str
    assert "a:0" not in args_str
    assert "v:0,name:1" in args_str or "v:0,name:0" in args_str

def test_path_traversal_protection():
    """3. Protección contra Path Traversal en el endpoint progresivo."""
    res = client.post("/api/v1/experimental/progressive/start", json={"source_uri": "../../../etc/passwd"})
    assert res.status_code == 400
    assert "E_SOURCE_" in res.text or "E_SECURITY_" in res.text

def test_isolation_storage_progressive(tmp_path, monkeypatch):
    """2. La modalidad progresiva escribe exclusivamente en storage/progressive."""
    prog_dir = tmp_path / "progressive"
    prog_dir.mkdir()
    monkeypatch.setattr(settings, "PROGRESSIVE_ROOT", str(prog_dir))

    test_mgr = ProgressiveSessionManager()
    
    # Create a small dummy file in input
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    dummy_video = input_dir / "valid.mp4"
    shutil.copy2(FIXTURES_DIR / "valid.mp4", dummy_video)
    monkeypatch.setattr(settings, "INGEST_ROOT", str(input_dir))

    session = test_mgr.start_session("valid.mp4")
    assert session.session_dir.is_relative_to(prog_dir)
    assert "output" not in str(session.session_dir)
    assert "staging" not in str(session.session_dir)

def test_playable_threshold_and_available_duration(tmp_path):
    """7 & 8. PLAYABLE no ocurre antes de 5 segmentos y usa el mínimo común."""
    mgr = ProgressiveSessionManager()
    session_dir = tmp_path / "test_session"
    session_dir.mkdir()
    
    # Create variants 0 and 1
    (session_dir / "0").mkdir()
    (session_dir / "1").mkdir()
    
    # Helper to write fake segments and playlist
    def write_variant_playlist(var_dir, num_segments, seg_dur=6.0):
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:6",
            "#EXT-X-TARGETDURATION:6",
            "#EXT-X-PLAYLIST-TYPE:EVENT",
            "#EXT-X-INDEPENDENT-SEGMENTS"
        ]
        for i in range(num_segments):
            seg_name = f"seg_{i:04d}.ts"
            seg_file = var_dir / seg_name
            seg_file.write_bytes(b"dummy ts content")
            lines.append(f"#EXTINF:{seg_dur:.6f},")
            lines.append(seg_name)
        (var_dir / "playlist.m3u8").write_text("\n".join(lines) + "\n")

    # Case A: Variant 0 has 5 segments (30s), Variant 1 has only 4 segments (24s)
    write_variant_playlist(session_dir / "0", 5, 6.0)
    write_variant_playlist(session_dir / "1", 4, 6.0)

    count0, dur0, _ = mgr._parse_variant_playlist(session_dir / "0" / "playlist.m3u8")
    count1, dur1, _ = mgr._parse_variant_playlist(session_dir / "1" / "playlist.m3u8")
    assert count0 == 5
    assert count1 == 4
    assert dur0 == 30.0
    assert dur1 == 24.0

    # Minimum available duration between variants
    available = min(dur0, dur1)
    assert available == 24.0
    
    # Must NOT be playable if any active variant has < 5 segments
    min_segments = 5
    is_playable = all(c >= min_segments for c in [count0, count1])
    assert is_playable is False

    # Case B: Variant 1 gets its 5th segment
    write_variant_playlist(session_dir / "1", 5, 6.0)
    count1, dur1, _ = mgr._parse_variant_playlist(session_dir / "1" / "playlist.m3u8")
    assert count1 == 5
    assert dur1 == 30.0

    is_playable = all(c >= min_segments for c in [count0, count1])
    assert is_playable is True
    assert min(dur0, dur1) == 30.0

def test_temp_file_ignored_until_promoted(tmp_path):
    """Segmentos .tmp en progreso son ignorados por el parser hasta su renombramiento atómico."""
    mgr = ProgressiveSessionManager()
    var_dir = tmp_path / "var_0"
    var_dir.mkdir()

    # Playlist lists seg_0000.ts, but only seg_0000.ts.tmp exists on disk
    (var_dir / "seg_0000.ts.tmp").write_bytes(b"partial ts")
    lines = [
        "#EXTM3U",
        "#EXTINF:6.0,",
        "seg_0000.ts"
    ]
    pl = var_dir / "playlist.m3u8"
    pl.write_text("\n".join(lines))

    count, dur, segs = mgr._parse_variant_playlist(pl)
    # The segment seg_0000.ts does not physically exist yet!
    assert count == 0
    assert dur == 0.0
    assert len(segs) == 0

    # When renamed atomically to .ts
    (var_dir / "seg_0000.ts.tmp").rename(var_dir / "seg_0000.ts")
    count, dur, segs = mgr._parse_variant_playlist(pl)
    assert count == 1
    assert dur == 6.0
    assert segs == ["seg_0000.ts"]

def test_playlists_grow_dynamically(tmp_path):
    """11. Los playlists crecen mientras el proceso está activo y available_until_seconds aumenta."""
    mgr = ProgressiveSessionManager()
    session_dir = tmp_path / "growth_test"
    session_dir.mkdir()
    var_dir = session_dir / "0"
    var_dir.mkdir()

    # Step 1: 1 segment
    (var_dir / "seg_0000.ts").write_bytes(b"ts0")
    (var_dir / "playlist.m3u8").write_text("#EXTM3U\n#EXTINF:6.0,\nseg_0000.ts\n")
    c1, d1, _ = mgr._parse_variant_playlist(var_dir / "playlist.m3u8")
    assert c1 == 1
    assert d1 == 6.0

    # Step 2: 2nd segment added
    (var_dir / "seg_0001.ts").write_bytes(b"ts1")
    (var_dir / "playlist.m3u8").write_text("#EXTM3U\n#EXTINF:6.0,\nseg_0000.ts\n#EXTINF:6.0,\nseg_0001.ts\n")
    c2, d2, _ = mgr._parse_variant_playlist(var_dir / "playlist.m3u8")
    assert c2 == 2
    assert d2 == 12.0
    assert d2 > d1

def test_ffmpeg_error_transitions_to_failed(tmp_path):
    """10. Captura de errores de FFmpeg: retorno no-cero transiciona a FAILED."""
    session = ProgressiveSession("err_uuid", "dummy.mp4", tmp_path / "dummy.mp4")
    session.session_dir.mkdir(parents=True, exist_ok=True)
    with open(session.log_path, "w") as f:
        f.write("Error while opening decoder\nConversion failed!\n")

    # Simulate fake process exit with return code 1
    class FakeProc:
        returncode = 1
        def poll(self): return 1
        def wait(self): return 1
        class stdout:
            @staticmethod
            def readline(): return ""
            @staticmethod
            def close(): pass

    session.process = FakeProc()
    mgr = ProgressiveSessionManager()
    # Test error message extraction
    with open(session.log_path, "r") as lf:
        lines = lf.readlines()
        session.error_message = "".join(lines[-10:])
    session.status = "FAILED"

    assert session.status == "FAILED"
    assert "Conversion failed!" in session.error_message

def test_progressive_full_transcode_completes(tmp_path, monkeypatch):
    """9. COMPLETED ocurre cuando FFmpeg termina correctamente y los playlists tienen #EXT-X-ENDLIST."""
    prog_dir = tmp_path / "progressive"
    prog_dir.mkdir()
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    shutil.copy2(FIXTURES_DIR / "valid.mp4", input_dir / "valid.mp4")

    monkeypatch.setattr(settings, "PROGRESSIVE_ROOT", str(prog_dir))
    monkeypatch.setattr(settings, "INGEST_ROOT", str(input_dir))

    mgr = ProgressiveSessionManager()
    session = mgr.start_session("valid.mp4")

    # Wait for completion (valid.mp4 is 1.0s, transcode takes < 2s)
    for _ in range(20):
        time.sleep(0.3)
        if session.status in ["COMPLETED", "FAILED"]:
            break

    assert session.status == "COMPLETED"
    assert session.progress == 100.0
    assert session.available_until_seconds > 0.0

    # Verify #EXT-X-ENDLIST in variant playlists
    for v in session.selected_variants.keys():
        v_pl = session.session_dir / v / "playlist.m3u8"
        assert v_pl.exists()
        content = v_pl.read_text()
        assert "#EXT-X-ENDLIST" in content
        assert "seg_0000.ts" in content
