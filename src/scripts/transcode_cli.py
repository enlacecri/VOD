#!/usr/bin/env python3
"""
CLI tool for Transcode Orchestrator inspection and repair.
Invoked via `vod.sh transcodes` and `vod.sh transcodes-repair`.
"""

import sys
import time
import argparse
from datetime import timedelta
from typing import Dict, Any

from src.core.config import settings
from src.core.queues import get_redis_connection, TRANSCODE_QUEUES
from src.services.transcode_orchestrator import (
    transcode_orchestrator,
    Priority,
    HealthStatus,
)


def format_runtime(seconds: float) -> str:
    secs = int(max(0, seconds))
    td = timedelta(seconds=secs)
    # Format as HH:MM:SS
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    secs = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def get_waiting_counts_by_priority() -> Dict[str, int]:
    counts = {
        Priority.CRITICAL.value: 0,
        Priority.HIGH.value: 0,
        Priority.NORMAL.value: 0,
        Priority.LOW.value: 0,
        Priority.BACKGROUND.value: 0,
    }
    if not transcode_orchestrator.slot_manager.is_available():
        return counts

    try:
        from rq import Queue
        r = transcode_orchestrator.slot_manager.redis
        for qname in TRANSCODE_QUEUES:
            q = Queue(name=qname, connection=r)
            q_count = q.count + q.scheduled_job_registry.count
            if "priority" in qname:
                counts[Priority.HIGH.value] += q_count
            elif "batch" in qname:
                counts[Priority.BACKGROUND.value] += q_count
            else:
                counts[Priority.NORMAL.value] += q_count
    except Exception:
        pass
    return counts


def cmd_status():
    metrics = transcode_orchestrator.get_metrics()
    health = transcode_orchestrator.healthcheck()
    res = metrics["resources"]
    active_slots = metrics["active_slots_detail"]
    waiting = get_waiting_counts_by_priority()

    print("TRANSCODE ORCHESTRATOR")
    print("================================================")
    print(f"Node:    {metrics['node_name']}")
    print(f"Backend: {metrics['backend'].upper()}")
    print(f"Status:  {health['status']}")
    if health['status'] != HealthStatus.HEALTHY.value:
        print(f"Note:    {health.get('reason', '')}")
    print()

    print("Capacity")
    print("------------------------------------------------")
    print(f"Maximum concurrent:         {metrics['max_concurrent']}")
    print(f"Reserved priority slots:    {metrics['reserved_priority_slots']}")
    print(f"Active transcodes:          {metrics['active_transcodes']}")
    print(f"Available slots:            {metrics['available_slots']}")
    print()

    print("Resources")
    print("------------------------------------------------")
    print(f"CPU:                       {res['cpu_percent']}%")
    load_1m = res['load_average'][0]
    cores = res['cpu_count']
    print(f"Load:                      {load_1m} / {cores} cores")
    print(f"Memory available:          {res['memory_available_mb'] / 1024:.1f} GB")
    print(f"Disk free:                 {res['disk_free_gb']:.1f} GB")
    print()

    print("Active")
    print("------------------------------------------------")
    if active_slots:
        print(f"{'PID':<8} {'Asset':<14} {'Priority':<12} {'Runtime':<10} {'Profile':<12}")
        now = time.time()
        for s in active_slots:
            pid_str = str(s.get("pid") or "-")
            asset_str = str(s.get("asset_id", ""))[:12]
            prio_str = str(s.get("priority", ""))
            runtime_str = format_runtime(now - s.get("started_at", now))
            profile_str = str(s.get("profile", "default"))
            print(f"{pid_str:<8} {asset_str:<14} {prio_str:<12} {runtime_str:<10} {profile_str:<12}")
    else:
        print("No active transcoding slots.")
    print()

    print("Waiting")
    print("------------------------------------------------")
    for p in [Priority.CRITICAL, Priority.HIGH, Priority.NORMAL, Priority.LOW, Priority.BACKGROUND]:
        print(f"{p.value:<12} {waiting[p.value]}")
    print()

    # Admission preview
    adm_bg = transcode_orchestrator.can_start_transcode(Priority.BACKGROUND)
    adm_hi = transcode_orchestrator.can_start_transcode(Priority.HIGH)
    print("Admission:")
    bg_stat = "ALLOWED" if adm_bg.allowed else "DENIED"
    print(f"NEW BACKGROUND -> {bg_stat} (Reason: {adm_bg.reason})")
    hi_stat = "ALLOWED" if adm_hi.allowed else "DENIED"
    print(f"NEW HIGH       -> {hi_stat} (Reason: {adm_hi.reason})")

    if metrics.get("orphan_ffmpeg_count", 0) > 0:
        print()
        print("WARNING:")
        print(f"Detected {metrics['orphan_ffmpeg_count']} unmanaged FFmpeg processes.")
        for o in metrics.get("orphan_ffmpeg_processes", []):
            print(f"  PID {o['pid']}: {o.get('cmdline', '')[:80]}...")

    print("================================================")


def cmd_repair():
    print("Starting Transcode Orchestrator Repair...")
    res = transcode_orchestrator.repair_slots()
    print(f"Repaired stale slots: {res['repaired_slots']}")
    print(f"Pruned dangling keys: {res['pruned_keys']}")
    print(f"Promoted waiting jobs: {res['promoted_jobs']}")
    health = transcode_orchestrator.healthcheck()
    print(f"Post-repair Status:   {health['status']}")


def main():
    parser = argparse.ArgumentParser(description="Transcode Orchestrator CLI")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("status", help="Show orchestrator status and active slots")
    subparsers.add_parser("repair", help="Repair stale slots and prune dangling keys")

    args = parser.parse_args()
    if args.command == "repair":
        cmd_repair()
    else:
        cmd_status()


if __name__ == "__main__":
    main()
