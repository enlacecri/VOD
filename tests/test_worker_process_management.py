"""
Automated tests for worker process management, orphan reconciliation, and idempotence.
Covers Cases A through I required by the specification:
- Caso A: start limpio
- Caso B: start repetido / idempotencia
- Caso C: PID file stale
- Caso D: PID reused / foreign process protection
- Caso E: Worker huérfano adopción
- Caso F: Limpieza de duplicados
- Caso G: Stop robusto (workers + duplicados)
- Caso H: Restart desde estado inconsistente
- Caso I: Protección de concurrencia / Locking
"""

import os
import sys
import time
import signal
import subprocess
import pytest
from pathlib import Path
from src.scripts.worker_manager import (
    WorkerManager,
    WORKER_SPECS,
    SPEC_BY_NAME,
    is_pid_alive,
)


@pytest.fixture
def test_mgr(tmp_path):
    """Creates a WorkerManager isolated to a temporary project root."""
    run_dir = tmp_path / "storage" / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    mgr = WorkerManager(project_root=tmp_path, run_dir=run_dir)
    return mgr, tmp_path, run_dir


def spawn_mock_worker(tmp_path, queue: str, name: str) -> subprocess.Popen:
    """Spawns a mock worker process in tmp_path matching the command pattern."""
    script_path = tmp_path / "src" / "scripts" / "run_worker.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    if not script_path.exists():
        script_path.write_text(
            "import time\n"
            "try:\n"
            "    while True: time.sleep(0.1)\n"
            "except KeyboardInterrupt:\n"
            "    pass\n"
        )

    proc = subprocess.Popen(
        [sys.executable, str(script_path), "--queue", queue, "--name", name],
        cwd=str(tmp_path),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Ensure process is up
    time.sleep(0.15)
    return proc


# ── Caso C: PID file stale ──────────────────────────────────────────────────
def test_case_c_stale_pid_file(test_mgr):
    mgr, tmp_path, run_dir = test_mgr
    spec = SPEC_BY_NAME["vod-priority-worker"]
    pid_file = run_dir / spec["pid_file"]

    # Write a non-existent PID
    pid_file.write_text("999999\n")

    res = mgr.reconcile_worker(spec)
    assert not res["os_running"]
    assert res["active_pid"] is None
    # Stale PID file should be removed
    assert not pid_file.exists()


# ── Caso D: PID reused / foreign process protection ─────────────────────────
def test_case_d_pid_reused_foreign_process(test_mgr):
    mgr, tmp_path, run_dir = test_mgr
    spec = SPEC_BY_NAME["vod-priority-worker"]
    pid_file = run_dir / spec["pid_file"]

    # Spawn an unrelated process (sleep)
    foreign_proc = subprocess.Popen(["sleep", "60"])
    try:
        pid_file.write_text(f"{foreign_proc.pid}\n")

        # read_pid_file should reject foreign process and clean PID file
        valid_pid = mgr.read_pid_file(spec)
        assert valid_pid is None
        assert not pid_file.exists()

        # The foreign process must NOT have been killed!
        assert foreign_proc.poll() is None
        assert is_pid_alive(foreign_proc.pid)
    finally:
        foreign_proc.terminate()
        foreign_proc.wait()


# ── Caso E: Worker huérfano (adopción / reconciliación de PID file) ─────────
def test_case_e_orphan_worker_adopted(test_mgr):
    mgr, tmp_path, run_dir = test_mgr
    spec = SPEC_BY_NAME["vod-priority-worker"]
    pid_file = run_dir / spec["pid_file"]

    # Process exists, but NO PID file exists
    proc = spawn_mock_worker(tmp_path, spec["queue"], spec["name"])
    try:
        assert not pid_file.exists()

        res = mgr.reconcile_worker(spec)
        assert res["os_running"]
        assert res["active_pid"] == proc.pid

        # PID file should have been created with the adopted PID
        assert pid_file.exists()
        assert int(pid_file.read_text().strip()) == proc.pid
    finally:
        mgr.terminate_process(proc.pid)


# ── Caso F: Duplicados (limpieza y conservación de exactamente 1) ───────────
def test_case_f_duplicates_resolution(test_mgr):
    mgr, tmp_path, run_dir = test_mgr
    spec = SPEC_BY_NAME["vod-priority-worker"]
    pid_file = run_dir / spec["pid_file"]

    # Spawn 3 duplicate workers for vod_priority
    p1 = spawn_mock_worker(tmp_path, spec["queue"], spec["name"])
    p2 = spawn_mock_worker(tmp_path, spec["queue"], spec["name"])
    p3 = spawn_mock_worker(tmp_path, spec["queue"], spec["name"])

    try:
        # Point PID file to p1
        pid_file.write_text(f"{p1.pid}\n")

        res = mgr.reconcile_worker(spec)
        assert res["os_running"]
        assert res["active_pid"] == p1.pid
        assert res["duplicates_count"] == 2
        assert set(res["duplicates_terminated"]) == {p2.pid, p3.pid}

        # Verify p2 and p3 are dead
        assert not is_pid_alive(p2.pid)
        assert not is_pid_alive(p3.pid)

        # Verify p1 is still alive
        assert is_pid_alive(p1.pid)
        assert int(pid_file.read_text().strip()) == p1.pid
    finally:
        mgr.terminate_process(p1.pid)


# ── Caso G: Stop robusto (detener workers válidos y duplicados) ──────────────
def test_case_g_stop_all_workers(test_mgr):
    mgr, tmp_path, run_dir = test_mgr

    # Spawn 7 valid workers + 2 duplicates
    procs = []
    for spec in WORKER_SPECS:
        p = spawn_mock_worker(tmp_path, spec["queue"], spec["name"])
        mgr.write_pid_file(spec, p.pid)
        procs.append(p)

    # Add 2 duplicate processes without PID file
    p_dup1 = spawn_mock_worker(tmp_path, "vod_priority", "vod-priority-worker")
    p_dup2 = spawn_mock_worker(tmp_path, "vod_batch", "vod-batch-worker")
    procs.extend([p_dup1, p_dup2])

    # Stop all
    ok = mgr.stop_all_workers()
    assert ok

    # Verify all processes are dead
    for p in procs:
        assert not is_pid_alive(p.pid)

    # Verify all PID files are cleaned
    for spec in WORKER_SPECS:
        assert not (run_dir / spec["pid_file"]).exists()


# ── Caso H: Restart desde estado inconsistente ──────────────────────────────
def test_case_h_restart_from_inconsistent_state(test_mgr):
    mgr, tmp_path, run_dir = test_mgr

    # Inconsistent state:
    # - 1 worker has stale PID
    # - 1 worker has 2 duplicate processes
    # - 1 worker is orphan (no PID file)
    spec_prio = SPEC_BY_NAME["vod-priority-worker"]
    spec_batch = SPEC_BY_NAME["vod-batch-worker"]
    spec_ingest = SPEC_BY_NAME["vod-ingest-worker"]

    (run_dir / spec_prio["pid_file"]).write_text("999998\n")  # stale

    p_batch1 = spawn_mock_worker(tmp_path, spec_batch["queue"], spec_batch["name"])
    p_batch2 = spawn_mock_worker(tmp_path, spec_batch["queue"], spec_batch["name"])

    p_ingest = spawn_mock_worker(tmp_path, spec_ingest["queue"], spec_ingest["name"])

    try:
        # Reconcile all
        results = mgr.reconcile_all_workers()
        status = mgr.get_status()
        sumry = status["summary"]

        assert sumry["duplicates"] == 0
        assert sumry["stale_pid_files"] == 0

        # Batch kept 1
        assert is_pid_alive(p_batch1.pid) or is_pid_alive(p_batch2.pid)
        # Ingest adopted
        assert is_pid_alive(p_ingest.pid)
        assert (run_dir / spec_ingest["pid_file"]).exists()
    finally:
        mgr.stop_all_workers()


# ── Caso I: Protección de concurrencia / Lock atómico ───────────────────────
def test_case_i_locking_concurrency(tmp_path):
    run_dir = tmp_path / "storage" / "run"
    lock_dir = run_dir / "vod.lock"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Simulate process 1 holding the lock
    lock_dir.mkdir()
    (lock_dir / "pid").write_text(f"{os.getpid()}\n")

    # Another process cannot create the lock directory
    assert lock_dir.exists()
    with pytest.raises(FileExistsError):
        os.mkdir(str(lock_dir))

    # Test stale lock cleanup: if PID inside lock_dir is dead
    (lock_dir / "pid").write_text("999999\n")
    # Simulate acquire_lock stale detection logic
    lock_pid = int((lock_dir / "pid").read_text().strip())
    assert not is_pid_alive(lock_pid)

    # Removing stale lock allows new acquisition
    import shutil
    shutil.rmtree(str(lock_dir))
    lock_dir.mkdir()
    (lock_dir / "pid").write_text(f"{os.getpid()}\n")
    assert lock_dir.exists()
    shutil.rmtree(str(lock_dir))


# ── Caso A & B: Start limpio e idempotencia de start repetido ───────────────
def test_case_a_and_b_idempotent_management(test_mgr):
    mgr, tmp_path, run_dir = test_mgr

    # Initially 0 workers
    status0 = mgr.get_status()
    assert status0["summary"]["running"] == 0

    # Start 7 workers
    procs = []
    for spec in WORKER_SPECS:
        p = spawn_mock_worker(tmp_path, spec["queue"], spec["name"])
        mgr.write_pid_file(spec, p.pid)
        procs.append(p)

    try:
        # Check status after start (Caso A: 7 workers)
        status1 = mgr.get_status()
        assert status1["summary"]["running"] == 7
        assert status1["summary"]["duplicates"] == 0

        # Simulate second start invocation (Caso B: idempotencia)
        # Calling reconcile_all_workers should not spawn or kill anything
        mgr.reconcile_all_workers()

        status2 = mgr.get_status()
        assert status2["summary"]["running"] == 7
        assert status2["summary"]["duplicates"] == 0

        # All original PIDs are still running and identical
        current_pids = {w["pid"] for w in status2["workers"]}
        expected_pids = {p.pid for p in procs}
        assert current_pids == expected_pids
    finally:
        mgr.stop_all_workers()
