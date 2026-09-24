#!/usr/bin/env python3
"""
Centralized Worker Process and State Manager for VOD.

Provides idempotent, verifiable, and secure worker lifecycle management:
- Strict worker identification by (queue, worker_name, project_root).
- Detection and graceful elimination of duplicate processes.
- Process group / child process cleanup (FFmpeg, python children).
- Robust PID file validation (handling PID reuse, stale files, orphans).
- RQ / Redis synchronization (cleaning stale worker registries).
- Comprehensive status and transactional healthchecks.
"""

import os
import sys
import time
import signal
import subprocess
import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

try:
    from redis import Redis
    from rq import Worker, worker_registration
    from src.core.config import settings
    from src.core.queues import (
        QUEUE_LEGACY,
        QUEUE_PRIORITY,
        QUEUE_INGEST,
        QUEUE_BATCH,
        QUEUE_BACKUP,
        QUEUE_SUBTITLES,
        QUEUE_SYNC,
    )
except ImportError:
    # Allow partial imports if executed standalone without venv for basic inspection
    settings = None
    QUEUE_LEGACY = "vod_tasks"
    QUEUE_PRIORITY = "vod_priority"
    QUEUE_INGEST = "vod_ingest"
    QUEUE_BATCH = "vod_batch"
    QUEUE_BACKUP = "vod_backup"
    QUEUE_SUBTITLES = "vod_subtitles"
    QUEUE_SYNC = "vod_sync"

# Specification of the 7 managed workers
WORKER_SPECS: List[Dict[str, str]] = [
    {
        "name": "vod-legacy-worker",
        "queue": QUEUE_LEGACY,
        "pid_file": "worker.pid",
        "log_file": "worker.log",
        "label": "Legacy Worker",
        "category": "TRANSCODE",
    },
    {
        "name": "vod-priority-worker",
        "queue": QUEUE_PRIORITY,
        "pid_file": "worker_priority.pid",
        "log_file": "worker_priority.log",
        "label": "Priority Worker",
        "category": "TRANSCODE",
    },
    {
        "name": "vod-ingest-worker",
        "queue": QUEUE_INGEST,
        "pid_file": "worker_ingest.pid",
        "log_file": "worker_ingest.log",
        "label": "Ingest Worker",
        "category": "TRANSCODE",
    },
    {
        "name": "vod-batch-worker",
        "queue": QUEUE_BATCH,
        "pid_file": "worker_batch.pid",
        "log_file": "worker_batch.log",
        "label": "Batch Worker",
        "category": "TRANSCODE",
    },
    {
        "name": "vod-backup-worker",
        "queue": QUEUE_BACKUP,
        "pid_file": "worker_backup.pid",
        "log_file": "worker_backup.log",
        "label": "Backup Worker",
        "category": "POST-PROCESS",
    },
    {
        "name": "vod-subtitles-worker",
        "queue": QUEUE_SUBTITLES,
        "pid_file": "worker_subtitles.pid",
        "log_file": "worker_subtitles.log",
        "label": "Subtitles Worker",
        "category": "POST-PROCESS",
    },
    {
        "name": "vod-sync-worker",
        "queue": QUEUE_SYNC,
        "pid_file": "worker_sync.pid",
        "log_file": "worker_sync.log",
        "label": "Sync Worker",
        "category": "POST-PROCESS",
    },
]

SPEC_BY_NAME = {spec["name"]: spec for spec in WORKER_SPECS}


def get_default_project_root() -> Path:
    """Returns the repository root directory (absolute path)."""
    return Path(__file__).resolve().parent.parent.parent


def get_process_cwd(pid: int) -> Optional[Path]:
    """
    Returns the working directory of a process.
    Supports Linux (/proc) and macOS/BSD (lsof).
    """
    proc_cwd = Path(f"/proc/{pid}/cwd")
    if proc_cwd.is_symlink():
        try:
            return proc_cwd.resolve()
        except (OSError, RuntimeError):
            pass

    try:
        out = subprocess.check_output(
            ["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2.0,
        )
        for line in out.splitlines():
            if line.startswith("n"):
                return Path(line[1:].strip()).resolve()
    except (subprocess.SubprocessError, OSError):
        pass
    return None


def get_process_children(pid: int) -> List[int]:
    """Finds all direct child process IDs of a given PID."""
    children: List[int] = []
    try:
        out = subprocess.check_output(
            ["pgrep", "-P", str(pid)],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2.0,
        )
        for line in out.splitlines():
            line = line.strip()
            if line.isdigit():
                children.append(int(line))
    except (subprocess.SubprocessError, OSError):
        pass
    return children


def get_all_descendant_pids(pid: int) -> List[int]:
    """Recursively collects all descendant process IDs (children, grandchildren, etc.)."""
    descendants: List[int] = []
    queue = [pid]
    seen = {pid}
    while queue:
        curr = queue.pop(0)
        children = get_process_children(curr)
        for child in children:
            if child not in seen:
                seen.add(child)
                descendants.append(child)
                queue.append(child)
    return descendants


def is_pid_alive(pid: int) -> bool:
    """
    Checks if a process exists, can receive signals, and is not a zombie.
    """
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True

    # Validate process is not in zombie/defunct state
    try:
        out = subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "state="],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=1.0,
        ).strip()
        if not out or "Z" in out:
            return False
        return True
    except (subprocess.SubprocessError, OSError):
        return False



def get_process_cmdline(pid: int) -> str:
    """Gets the full command line of a process using ps."""
    try:
        out = subprocess.check_output(
            ["ps", "-ww", "-p", str(pid), "-o", "command="],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2.0,
        )
        return out.strip()
    except (subprocess.SubprocessError, OSError):
        return ""


def list_system_processes() -> List[Tuple[int, int, str]]:
    """
    Returns a list of (pid, ppid, command) for all running processes.
    Uses portable `ps -ww -eo pid,ppid,args`.
    """
    processes = []
    try:
        out = subprocess.check_output(
            ["ps", "-ww", "-eo", "pid,ppid,args"],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5.0,
        )
        lines = out.splitlines()
        if not lines:
            return []
        # Skip header
        for line in lines[1:]:
            parts = line.strip().split(None, 2)
            if len(parts) >= 3 and parts[0].isdigit() and parts[1].isdigit():
                pid = int(parts[0])
                ppid = int(parts[1])
                cmd = parts[2]
                processes.append((pid, ppid, cmd))
    except (subprocess.SubprocessError, OSError):
        pass
    return processes


class WorkerManager:
    def __init__(
        self,
        project_root: Optional[Path] = None,
        run_dir: Optional[Path] = None,
        redis_url: Optional[str] = None,
    ):
        self.project_root = (project_root or get_default_project_root()).resolve()
        self.run_dir = (run_dir or (self.project_root / "storage" / "run")).resolve()
        self.redis_url = redis_url or (settings.REDIS_URL if settings else "redis://localhost:6383/0")
        self._redis_conn: Optional[Redis] = None

    def get_redis(self) -> Optional[Redis]:
        """Lazy Redis connection."""
        if self._redis_conn is None:
            try:
                self._redis_conn = Redis.from_url(self.redis_url, socket_timeout=2.0)
                self._redis_conn.ping()
            except Exception:
                self._redis_conn = None
        return self._redis_conn

    def matches_worker(self, cmd: str, queue: str, name: str, pid: int) -> bool:
        """
        Determines whether a command belongs unequivocally to this worker spec
        AND to this project directory.
        """
        if "run_worker.py" not in cmd and "src/scripts/run_worker.py" not in cmd:
            return False

        # Verify queue
        has_queue = (f"--queue {queue}" in cmd) or (f"--queue={queue}" in cmd)
        if not has_queue:
            return False

        # Verify worker name
        has_name = (f"--name {name}" in cmd) or (f"--name={name}" in cmd)
        if not has_name:
            return False

        # Verify project isolation (CWD or absolute script path)
        project_root_str = str(self.project_root)
        if project_root_str in cmd:
            return True

        # Check process working directory
        proc_cwd = get_process_cwd(pid)
        if proc_cwd and proc_cwd == self.project_root:
            return True

        return False

    def find_worker_pids(self, spec: Dict[str, str]) -> List[int]:
        """Finds all OS processes matching the given worker specification."""
        matching_pids: List[int] = []
        all_procs = list_system_processes()
        for pid, ppid, cmd in all_procs:
            if self.matches_worker(cmd, spec["queue"], spec["name"], pid):
                matching_pids.append(pid)
        return matching_pids

    def read_pid_file(self, spec: Dict[str, str]) -> Optional[int]:
        """
        Reads the PID file for a worker.
        Validates PID existence, command pattern, and project isolation.
        Cleans stale PID files (process dead or command mismatch).
        """
        pid_file_path = self.run_dir / spec["pid_file"]
        if not pid_file_path.exists():
            return None

        try:
            content = pid_file_path.read_text().strip()
            if not content.isdigit():
                pid_file_path.unlink(missing_ok=True)
                return None
            pid = int(content)
        except Exception:
            pid_file_path.unlink(missing_ok=True)
            return None

        if not is_pid_alive(pid):
            # Process is dead - PID file is stale
            pid_file_path.unlink(missing_ok=True)
            return None

        # Process is alive: verify command matches this worker and project
        cmd = get_process_cmdline(pid)
        if not self.matches_worker(cmd, spec["queue"], spec["name"], pid):
            # PID reused by an unrelated process!
            # Do NOT kill the process; unlink stale PID file.
            pid_file_path.unlink(missing_ok=True)
            return None

        return pid

    def write_pid_file(self, spec: Dict[str, str], pid: int) -> None:
        """Writes the PID file for a worker."""
        self.run_dir.mkdir(parents=True, exist_ok=True)
        pid_file_path = self.run_dir / spec["pid_file"]
        pid_file_path.write_text(f"{pid}\n")

    def terminate_process(self, pid: int, timeout: float = 6.0) -> bool:
        """
        Gracefully terminates a process (SIGTERM) and its child descendants (e.g. FFmpeg).
        Falls back to SIGKILL if still alive after timeout.
        """
        if not is_pid_alive(pid):
            return True

        descendants = get_all_descendant_pids(pid)
        pids_to_kill = descendants + [pid]

        # Send SIGTERM to descendants first, then parent
        for p in pids_to_kill:
            try:
                os.kill(p, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass

        # Wait up to timeout seconds
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not is_pid_alive(pid):
                # Clean up any leftover descendants
                for p in descendants:
                    if is_pid_alive(p):
                        try:
                            os.kill(p, signal.SIGTERM)
                        except (ProcessLookupError, PermissionError):
                            pass
                return True
            time.sleep(0.2)

        # Still alive: SIGKILL
        current_descendants = get_all_descendant_pids(pid)
        for p in current_descendants + [pid]:
            try:
                os.kill(p, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

        time.sleep(0.3)
        return not is_pid_alive(pid)

    def reconcile_worker(self, spec: Dict[str, str]) -> Dict[str, Any]:
        """
        Reconciles a worker:
        - Detects running OS processes
        - Cleans up duplicates (keeps exactly one, terminates the rest)
        - Updates/recreates PID file with the kept PID
        - Removes stale PID file if 0 processes are running
        """
        pids = self.find_worker_pids(spec)
        pid_from_file = self.read_pid_file(spec)

        active_pid: Optional[int] = None
        duplicates_terminated: List[int] = []

        if len(pids) == 0:
            # 0 processes running
            pid_file_path = self.run_dir / spec["pid_file"]
            if pid_file_path.exists():
                pid_file_path.unlink(missing_ok=True)
            active_pid = None

        elif len(pids) == 1:
            # Exactly 1 process running
            active_pid = pids[0]
            if pid_from_file != active_pid:
                self.write_pid_file(spec, active_pid)

        else:
            # Multiple duplicate processes detected!
            # Prefer keeping the PID already in the valid PID file if present
            if pid_from_file in pids:
                keep_pid = pid_from_file
            else:
                keep_pid = min(pids)  # Keep the oldest process

            print(f"[WARN] Duplicate worker detected: {spec['name']} ({len(pids)} processes running)", file=sys.stderr)
            print(f"[INFO] Keeping PID {keep_pid}", file=sys.stderr)

            for dup_pid in pids:
                if dup_pid != keep_pid:
                    print(f"[INFO] Terminating duplicate PID {dup_pid}...", file=sys.stderr)
                    self.terminate_process(dup_pid)
                    duplicates_terminated.append(dup_pid)

            active_pid = keep_pid
            self.write_pid_file(spec, active_pid)

        return {
            "name": spec["name"],
            "queue": spec["queue"],
            "active_pid": active_pid,
            "os_running": active_pid is not None,
            "duplicates_count": len(duplicates_terminated),
            "duplicates_terminated": duplicates_terminated,
        }

    def reconcile_all_workers(self) -> List[Dict[str, Any]]:
        """Reconciles all configured workers."""
        results = []
        for spec in WORKER_SPECS:
            res = self.reconcile_worker(spec)
            results.append(res)
        self.clean_stale_redis_registrations([r["name"] for r in results if r["os_running"]])
        return results

    def stop_worker(self, spec: Dict[str, str], timeout: float = 6.0) -> bool:
        """Stops all processes (active and orphans) for a given worker spec."""
        pids = self.find_worker_pids(spec)
        file_pid = self.read_pid_file(spec)
        if file_pid and file_pid not in pids:
            pids.append(file_pid)

        all_stopped = True
        for pid in pids:
            ok = self.terminate_process(pid, timeout=timeout)
            if not ok:
                all_stopped = False

        # Clean PID file
        pid_file_path = self.run_dir / spec["pid_file"]
        pid_file_path.unlink(missing_ok=True)

        return all_stopped

    def stop_all_workers(self, timeout: float = 6.0) -> bool:
        """Stops all workers belonging to the project."""
        all_stopped = True
        for spec in WORKER_SPECS:
            ok = self.stop_worker(spec, timeout=timeout)
            if not ok:
                all_stopped = False

        # Clean Redis registrations for all project workers
        r = self.get_redis()
        if r:
            self.clean_stale_redis_registrations(live_worker_names=[])

        return all_stopped

    def clean_stale_redis_registrations(self, live_worker_names: List[str]) -> int:
        """
        Removes Redis RQ registrations for workers that are dead in the OS.
        Uses RQ official registry mechanisms.
        """
        r = self.get_redis()
        if not r:
            return 0

        cleaned = 0
        live_names_set = set(live_worker_names)

        # Inspect all project worker names
        for spec in WORKER_SPECS:
            w_name = spec["name"]
            w_queue = spec["queue"]
            w_key = f"rq:worker:{w_name}"

            if w_name not in live_names_set:
                # This worker is not alive in OS!
                # Clean up hash and remove from sets
                try:
                    pipe = r.pipeline()
                    pipe.srem("rq:workers", w_key)
                    pipe.srem(f"rq:workers:{w_queue}", w_key)
                    pipe.delete(w_key)
                    pipe.execute()
                    cleaned += 1
                except Exception:
                    pass

        return cleaned

    def get_rq_status(self, spec: Dict[str, str], os_pid: Optional[int]) -> str:
        """
        Determines the RQ registration status in Redis for a worker:
        - Registered: Hash exists and registered in rq:workers / queue set
        - Unregistered: No registration in Redis
        - Stale: Hash exists in Redis but process is dead in OS
        """
        r = self.get_redis()
        if not r:
            return "Unavailable"

        w_name = spec["name"]
        w_queue = spec["queue"]
        w_key = f"rq:worker:{w_name}"

        try:
            in_global = r.sismember("rq:workers", w_key)
            in_queue = r.sismember(f"rq:workers:{w_queue}", w_key)
            key_exists = r.exists(w_key)

            if not os_pid:
                if key_exists or in_global or in_queue:
                    return "Stale"
                return "Not registered"

            # OS process is running
            if in_global or in_queue or key_exists:
                # Ensure it is properly listed in rq:workers and queue set
                if not in_global or not in_queue:
                    pipe = r.pipeline()
                    pipe.sadd("rq:workers", w_key)
                    pipe.sadd(f"rq:workers:{w_queue}", w_key)
                    pipe.execute()
                return "Registered"
            return "Pending"
        except Exception:
            return "Error"

    def get_status(self) -> Dict[str, Any]:
        """Gathers full status of all 7 workers."""
        workers_status = []
        total_configured = len(WORKER_SPECS)
        total_running = 0
        total_duplicates = 0
        stale_pid_files = 0
        rq_stale_workers = 0

        # Check PID files on disk to count stale ones
        for spec in WORKER_SPECS:
            pid_file = self.run_dir / spec["pid_file"]
            if pid_file.exists():
                raw = pid_file.read_text().strip()
                if not raw.isdigit() or not is_pid_alive(int(raw)):
                    stale_pid_files += 1

        for spec in WORKER_SPECS:
            pids = self.find_worker_pids(spec)
            valid_pid = self.read_pid_file(spec)

            if not valid_pid and len(pids) == 1:
                valid_pid = pids[0]

            dups = max(0, len(pids) - 1) if pids else 0
            total_duplicates += dups

            if valid_pid:
                total_running += 1
                os_state = "Running"
            else:
                os_state = "Stopped"

            rq_state = self.get_rq_status(spec, valid_pid)
            if rq_state == "Stale":
                rq_stale_workers += 1

            workers_status.append({
                "label": spec["label"],
                "name": spec["name"],
                "queue": spec["queue"],
                "category": spec["category"],
                "pid": valid_pid,
                "os": os_state,
                "rq": rq_state,
                "duplicates": dups,
            })

        return {
            "workers": workers_status,
            "summary": {
                "configured": total_configured,
                "running": total_running,
                "duplicates": total_duplicates,
                "stale_pid_files": stale_pid_files,
                "rq_stale_workers": rq_stale_workers,
            }
        }

    def print_status_table(self) -> None:
        """Prints a human-readable status table following Section 14."""
        status = self.get_status()

        # Group by category
        categories = {}
        for w in status["workers"]:
            cat = w["category"]
            categories.setdefault(cat, []).append(w)

        for cat, items in categories.items():
            print(f"\n{cat} WORKERS:")
            for w in items:
                print(f"\n{w['label']}")
                print(f"Queue: {w['queue']}")
                print(f"PID: {w['pid'] if w['pid'] else 'None'}")
                print(f"OS: {w['os']}")
                print(f"RQ: {w['rq']}")
                print(f"Duplicates: {w['duplicates']}")

        sumry = status["summary"]
        print("\n" + "=" * 40)
        print(f"Workers configurados: {sumry['configured']}")
        print(f"Workers activos válidos: {sumry['running']}")
        print(f"Duplicados: {sumry['duplicates']}")
        print(f"Stale PID files: {sumry['stale_pid_files']}")
        print(f"RQ stale workers: {sumry['rq_stale_workers']}")
        print("=" * 40 + "\n")

    def healthcheck(self) -> bool:
        """
        Validates that all 7 configured workers are running with 0 duplicates
        and registered in RQ.
        """
        status = self.get_status()
        sumry = status["summary"]
        if sumry["running"] != sumry["configured"]:
            return False
        if sumry["duplicates"] > 0:
            return False

        # Validate that all workers are reported as Registered (or Pending in fast startup)
        for w in status["workers"]:
            if w["os"] != "Running":
                return False
            if w["rq"] not in ("Registered", "Pending"):
                return False

        return True


def main():
    parser = argparse.ArgumentParser(description="VOD Worker Manager")
    parser.add_argument("command", choices=["status", "reconcile", "repair", "stop-all", "stop-worker", "check-worker", "healthcheck"])
    parser.add_argument("--name", default=None, help="Worker name for single-worker commands")
    parser.add_argument("--project-root", default=None, help="Project root directory path")
    parser.add_argument("--run-dir", default=None, help="PID run directory path")
    parser.add_argument("--json", action="store_true", help="Output in JSON format")
    parser.add_argument("--timeout", type=float, default=6.0, help="Graceful stop timeout in seconds")

    args = parser.parse_args()

    project_root = Path(args.project_root) if args.project_root else None
    run_dir = Path(args.run_dir) if args.run_dir else None

    mgr = WorkerManager(project_root=project_root, run_dir=run_dir)

    if args.command == "status":
        if args.json:
            print(json.dumps(mgr.get_status(), indent=2))
        else:
            mgr.print_status_table()

    elif args.command in ("reconcile", "repair"):
        results = mgr.reconcile_all_workers()
        if args.json:
            print(json.dumps(results, indent=2))
        else:
            mgr.print_status_table()

    elif args.command == "stop-all":
        ok = mgr.stop_all_workers(timeout=args.timeout)
        if args.json:
            print(json.dumps({"success": ok}))
        else:
            if ok:
                print("[SUCCESS] All project workers stopped cleanly.")
            else:
                print("[WARNING] Some worker processes required SIGKILL.")

    elif args.command == "stop-worker":
        if not args.name or args.name not in SPEC_BY_NAME:
            print(f"[ERROR] Unknown or missing worker name: {args.name}", file=sys.stderr)
            sys.exit(1)
        ok = mgr.stop_worker(SPEC_BY_NAME[args.name], timeout=args.timeout)
        sys.exit(0 if ok else 1)

    elif args.command == "check-worker":
        if not args.name or args.name not in SPEC_BY_NAME:
            print(f"[ERROR] Unknown or missing worker name: {args.name}", file=sys.stderr)
            sys.exit(1)
        res = mgr.reconcile_worker(SPEC_BY_NAME[args.name])
        if args.json:
            print(json.dumps(res))
        else:
            if res["os_running"]:
                print(f"RUNNING {res['active_pid']}")
            else:
                print("STOPPED")

    elif args.command == "healthcheck":
        ok = mgr.healthcheck()
        if args.json:
            print(json.dumps({"healthy": ok}))
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
