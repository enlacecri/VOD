import os
import signal
import socket
import time
import random
import uuid
import logging
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from src.core.config import settings
from src.services.transcode_orchestrator.models import (
    TranscodeSlot,
    SystemResources,
    AdmissionDecision,
    Priority,
    ExecutionBackend,
    ResourceType,
    HealthStatus,
    get_priority_for_queue,
)
from src.services.transcode_orchestrator.resource_provider import (
    ResourceProvider,
    SystemResourceProvider,
)
from src.services.transcode_orchestrator.slot_manager import RedisSlotManager

try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger("transcode.orchestrator")


class TranscodeOrchestrator:
    """
    Central Smart Transcoding Orchestrator for Enlace VOD.
    Guarantees strict concurrency limits, admission control, atomic slot leases,
    heartbeats, priority reservations, observability, and self-healing.
    """

    def __init__(
        self,
        slot_manager: Optional[RedisSlotManager] = None,
        resource_provider: Optional[ResourceProvider] = None,
        node_name: Optional[str] = None,
        max_concurrent: Optional[int] = None,
        reserved_priority_slots: Optional[int] = None,
    ):
        self.node_name = node_name or settings.resolved_node_name
        self.slot_manager = slot_manager or RedisSlotManager()
        self.resource_provider = resource_provider or SystemResourceProvider()
        self._max_concurrent = max_concurrent
        self._reserved_priority_slots = reserved_priority_slots

    @property
    def max_concurrent(self) -> int:
        if self._max_concurrent is not None:
            return self._max_concurrent
        return settings.TRANSCODE_MAX_CONCURRENT

    @property
    def reserved_priority_slots(self) -> int:
        if self._reserved_priority_slots is not None:
            return self._reserved_priority_slots
        return settings.TRANSCODE_RESERVED_PRIORITY_SLOTS

    @property
    def is_enabled(self) -> bool:
        return settings.TRANSCODE_ORCHESTRATOR_ENABLED

    def get_resources(self) -> SystemResources:
        """Consults current system resources via the configured ResourceProvider."""
        active_count = 0
        if self.slot_manager.is_available():
            try:
                active_count = self.slot_manager.count_active_slots(self.node_name)
            except Exception:
                pass
        return self.resource_provider.get_resources(active_transcodes=active_count)

    def can_start_transcode(
        self,
        priority: Priority | str = Priority.NORMAL,
        estimated_weight: int = 1,
    ) -> AdmissionDecision:
        """
        Evaluates whether a new heavy transcoding job can be admitted right now.
        Evaluates:
        1. Feature flag (bypass if disabled)
        2. Redis availability (FAIL-CLOSED)
        3. Slot concurrency and priority reservation
        4. System resources (CPU, Memory, Disk)
        """
        prio_enum = Priority.from_str(priority)

        # 1. Feature Flag check
        if not self.is_enabled:
            return AdmissionDecision(
                allowed=True,
                reason="legacy_mode_orchestrator_disabled",
                metrics={"orchestrator_enabled": False},
            )

        # 2. Redis Availability check (FAIL CLOSED)
        if not self.slot_manager.is_available():
            logger.error("transcode.orchestrator: Redis unavailable during admission check (fail-closed)")
            return AdmissionDecision(
                allowed=False,
                reason="orchestrator_unavailable",
                metrics={"redis_available": False},
            )

        # 3. Capacity & Priority Reservation Check
        try:
            active_slots = self.slot_manager.list_slots()
            active_count = len(active_slots)
        except Exception as e:
            logger.error(f"transcode.orchestrator: Failed to fetch slots: {e}")
            return AdmissionDecision(
                allowed=False,
                reason="orchestrator_unavailable",
                metrics={"error": str(e)},
            )

        metrics: Dict[str, Any] = {
            "active_slots": active_count,
            "max_concurrent": self.max_concurrent,
            "reserved_priority_slots": self.reserved_priority_slots,
            "priority": prio_enum.value,
        }

        # Check absolute maximum concurrency
        if active_count >= self.max_concurrent:
            logger.info(
                f"transcode.slot.denied: active={active_count}, max={self.max_concurrent}, "
                f"reason=max_concurrency_reached, priority={prio_enum.value}"
            )
            return AdmissionDecision(
                allowed=False,
                reason="max_concurrency_reached",
                metrics=metrics,
            )

        # Check priority reservation for non-priority jobs
        if not prio_enum.is_priority():
            normal_cap = max(0, self.max_concurrent - self.reserved_priority_slots)
            if active_count >= normal_cap:
                logger.info(
                    f"transcode.slot.denied: active={active_count}, normal_cap={normal_cap}, "
                    f"reserved={self.reserved_priority_slots}, reason=reserved_priority_capacity"
                )
                return AdmissionDecision(
                    allowed=False,
                    reason="reserved_priority_capacity",
                    metrics=metrics,
                )

        # 4. Resource Admission Checks
        if settings.TRANSCODE_RESOURCE_CHECK_ENABLED:
            res = self.get_resources()
            metrics.update({
                "cpu_percent": res.cpu_percent,
                "memory_available_mb": res.memory_available_mb,
                "disk_free_gb": res.disk_free_gb,
            })

            # CPU Threshold Check (CRITICAL/HIGH admitted up to HARD_THRESHOLD)
            cpu_limit = (
                settings.TRANSCODE_CPU_HARD_THRESHOLD
                if prio_enum.is_priority()
                else settings.TRANSCODE_CPU_START_THRESHOLD
            )
            if res.cpu_percent >= cpu_limit:
                logger.warning(
                    f"transcode.resource.cpu_high: current={res.cpu_percent}%, "
                    f"threshold={cpu_limit}%, priority={prio_enum.value}"
                )
                return AdmissionDecision(
                    allowed=False,
                    reason="cpu_threshold_exceeded",
                    metrics=metrics,
                )

            # Memory Check
            if res.memory_available_mb < settings.TRANSCODE_MIN_AVAILABLE_MEMORY_MB:
                logger.warning(
                    f"transcode.resource.memory_low: available={res.memory_available_mb}MB, "
                    f"min_required={settings.TRANSCODE_MIN_AVAILABLE_MEMORY_MB}MB"
                )
                return AdmissionDecision(
                    allowed=False,
                    reason="insufficient_memory",
                    metrics=metrics,
                )

            # Disk Check
            if res.disk_free_gb < settings.TRANSCODE_MIN_FREE_DISK_GB:
                logger.warning(
                    f"transcode.resource.disk_low: free={res.disk_free_gb}GB, "
                    f"min_required={settings.TRANSCODE_MIN_FREE_DISK_GB}GB"
                )
                return AdmissionDecision(
                    allowed=False,
                    reason="insufficient_disk",
                    metrics=metrics,
                )

        return AdmissionDecision(
            allowed=True,
            reason="capacity_available",
            metrics=metrics,
        )

    def acquire_slot(
        self,
        job_id: str | Any,
        asset_id: str | Any,
        queue: str,
        worker_name: str,
        priority: Optional[Priority | str] = None,
        profile: str = "default",
        estimated_weight: int = 1,
    ) -> Optional[TranscodeSlot]:
        """
        Attempts to acquire a transcode slot.
        Combines admission control with Redis atomic claim.
        Returns the TranscodeSlot if acquired, or None if denied.
        """
        if not self.is_enabled:
            # Legacy bypass mode: create unmanaged slot representation
            return TranscodeSlot(
                slot_id=f"legacy-{uuid.uuid4()}",
                job_id=str(job_id),
                asset_id=str(asset_id),
                worker_name=worker_name,
                queue=queue,
                node_id=self.node_name,
                priority=Priority.NORMAL.value,
                profile=profile,
                estimated_weight=estimated_weight,
            )

        if priority is None:
            prio_enum = get_priority_for_queue(queue)
        else:
            prio_enum = Priority.from_str(priority)

        # 1. Evaluate admission
        decision = self.can_start_transcode(priority=prio_enum, estimated_weight=estimated_weight)
        if not decision.allowed:
            logger.info(
                f"transcode.slot.denied: job={job_id}, asset={asset_id}, queue={queue}, "
                f"worker={worker_name}, priority={prio_enum.value}, reason={decision.reason}"
            )
            return None

        # 2. Prepare slot object
        slot_id = f"{self.node_name}:{job_id}:{uuid.uuid4().hex[:8]}"
        slot = TranscodeSlot(
            slot_id=slot_id,
            job_id=str(job_id),
            asset_id=str(asset_id),
            worker_name=worker_name,
            queue=queue,
            node_id=self.node_name,
            priority=prio_enum.value,
            profile=profile,
            estimated_weight=estimated_weight,
            started_at=time.time(),
            heartbeat_at=time.time(),
        )

        # 3. Perform atomic reservation in Redis
        success, reason, active_count = self.slot_manager.acquire_slot(
            slot=slot,
            max_concurrent=self.max_concurrent,
            reserved_priority_slots=self.reserved_priority_slots,
            ttl_seconds=settings.TRANSCODE_SLOT_TTL_SECONDS,
        )

        if not success:
            logger.info(
                f"transcode.slot.denied: atomic claim failed: job={job_id}, "
                f"reason={reason}, active={active_count}"
            )
            return None

        logger.info(
            f"transcode.slot.acquire: slot_id={slot.slot_id}, job={job_id}, asset={asset_id}, "
            f"queue={queue}, worker={worker_name}, node={self.node_name}, priority={prio_enum.value}, "
            f"active_now={active_count}"
        )
        return slot

    def heartbeat(self, slot: TranscodeSlot) -> bool:
        """Renews the lease of an acquired transcode slot."""
        if not self.is_enabled or slot.slot_id.startswith("legacy-"):
            return True

        try:
            renewed = self.slot_manager.heartbeat_slot(
                slot=slot,
                ttl_seconds=settings.TRANSCODE_SLOT_TTL_SECONDS,
            )
            if not renewed:
                logger.warning(f"transcode.slot.stale: slot {slot.slot_id} was expired or removed in Redis")
            return renewed
        except Exception as e:
            logger.warning(f"transcode.slot.heartbeat_failed: {slot.slot_id}: {e}")
            return False

    def update_slot_pid(self, slot: TranscodeSlot, pid: int) -> bool:
        """Records the actual FFmpeg subprocess PID in the slot."""
        slot.pid = pid
        if not self.is_enabled or slot.slot_id.startswith("legacy-"):
            return True

        try:
            return self.slot_manager.update_slot(slot)
        except Exception as e:
            logger.warning(f"transcode.slot.update_pid_failed: {slot.slot_id}: {e}")
            return False

    def update_slot_progress(self, slot: TranscodeSlot, progress: int) -> bool:
        """Updates transcode progress percentage and timestamp."""
        slot.progress = progress
        slot.last_progress_at = time.time()
        if not self.is_enabled or slot.slot_id.startswith("legacy-"):
            return True

        try:
            return self.slot_manager.update_slot(slot)
        except Exception as e:
            logger.warning(f"transcode.slot.update_progress_failed: {slot.slot_id}: {e}")
            return False

    def release_slot(self, slot: Optional[TranscodeSlot]) -> bool:
        """Releases the slot and notifies/promotes any waiting scheduled jobs."""
        if slot is None:
            return True

        duration = round(time.time() - slot.started_at, 2)
        if not self.is_enabled or slot.slot_id.startswith("legacy-"):
            logger.info(f"transcode.slot.release: legacy slot {slot.slot_id} released after {duration}s")
            return True

        try:
            released = self.slot_manager.release_slot(slot)
            logger.info(
                f"transcode.slot.release: slot_id={slot.slot_id}, job={slot.job_id}, "
                f"asset={slot.asset_id}, pid={slot.pid}, duration={duration}s"
            )
            # Promote waiting scheduled jobs
            self.promote_scheduled_jobs()
            return released
        except Exception as e:
            logger.error(f"transcode.slot.release_failed: {slot.slot_id}: {e}")
            return False

    def get_retry_delay(self, attempt: int = 0) -> int:
        """Calculates backpressure delay with exponential backoff and jitter."""
        min_sec = settings.TRANSCODE_RETRY_MIN_SECONDS
        max_sec = settings.TRANSCODE_RETRY_MAX_SECONDS
        base = min(max_sec, min_sec * (2 ** min(attempt, 4)))
        jitter = random.uniform(0.8, 1.2)
        return int(min(max_sec, max(min_sec, round(base * jitter))))

    def promote_scheduled_jobs(self) -> int:
        """
        Inspects transcode queues for scheduled jobs whose time has arrived
        and moves them into the active queues immediately.
        """
        if not self.slot_manager.is_available():
            return 0

        from rq import Queue
        from src.core.queues import TRANSCODE_QUEUES

        promoted = 0
        redis_conn = self.slot_manager.redis

        for qname in TRANSCODE_QUEUES:
            try:
                q = Queue(name=qname, connection=redis_conn)
                registry = q.scheduled_job_registry
                ready_job_ids = registry.get_jobs_to_schedule()
                for jid in ready_job_ids:
                    try:
                        from rq.job import Job as RQJob
                        j = RQJob.fetch(jid, connection=redis_conn)
                        q.enqueue_job(j)
                        promoted += 1
                    except Exception:
                        pass
            except Exception:
                pass

        return promoted

    def detect_orphan_ffmpeg_processes(self) -> List[Dict[str, Any]]:
        """
        Detects FFmpeg processes running on this machine that belong to this project
        (e.g. executing files in project storage), but do not have an active slot.
        Strictly filters to avoid interacting with any external FFmpeg on the system.
        """
        active_slots = self.slot_manager.list_slots(self.node_name)
        active_pids = {s.pid for s in active_slots if s.pid is not None}

        orphans: List[Dict[str, Any]] = []
        if psutil is None:
            return orphans

        output_root = str(Path(settings.OUTPUT_ROOT).resolve(strict=False))
        staging_root = str(Path(settings.STAGING_ROOT).resolve(strict=False))

        for proc in psutil.process_iter(attrs=["pid", "name", "cmdline", "create_time"]):
            try:
                pname = (proc.info["name"] or "").lower()
                cmdline = proc.info["cmdline"] or []
                cmdline_str = " ".join(cmdline)

                # Process must be ffmpeg
                if "ffmpeg" not in pname:
                    continue

                # Process must belong strictly to this project's paths
                belongs_to_project = (
                    output_root in cmdline_str
                    or staging_root in cmdline_str
                    or "EnlacePlus" in cmdline_str
                    or "hls_playlist_type" in cmdline_str
                )

                if belongs_to_project and proc.info["pid"] not in active_pids:
                    orphans.append({
                        "pid": proc.info["pid"],
                        "cmdline": cmdline_str[:120],
                        "created_at": proc.info["create_time"],
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        return orphans

    def repair_slots(self) -> Dict[str, Any]:
        """
        Reconciles slots against real OS processes and Redis state.
        Removes slots whose PID is no longer alive on this node.
        Prunes dead keys.
        """
        repaired_slots = 0
        pruned_keys = self.slot_manager.prune_stale_slots(self.node_name)

        slots = self.slot_manager.list_slots(self.node_name)
        for s in slots:
            if s.pid is not None:
                # Check if process is still alive on this node
                is_alive = False
                try:
                    os.kill(s.pid, 0)
                    is_alive = True
                except (OSError, ProcessLookupError):
                    is_alive = False

                if not is_alive:
                    logger.warning(
                        f"transcode.slot.stale: Slot {s.slot_id} has dead PID {s.pid}. Releasing."
                    )
                    self.slot_manager.release_slot(s)
                    repaired_slots += 1

        promoted_jobs = self.promote_scheduled_jobs()

        return {
            "repaired_slots": repaired_slots,
            "pruned_keys": pruned_keys,
            "promoted_jobs": promoted_jobs,
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Collects complete operational metrics for monitoring and CLI."""
        resources = self.get_resources()
        active_slots = self.slot_manager.list_slots(self.node_name)

        # Slots by priority
        prio_counts = {p.value: 0 for p in Priority}
        for s in active_slots:
            prio_counts[s.priority] = prio_counts.get(s.priority, 0) + 1

        available_slots = max(0, self.max_concurrent - len(active_slots))
        orphans = self.detect_orphan_ffmpeg_processes()

        return {
            "node_name": self.node_name,
            "backend": ExecutionBackend.CPU.value,
            "max_concurrent": self.max_concurrent,
            "reserved_priority_slots": self.reserved_priority_slots,
            "active_transcodes": len(active_slots),
            "available_slots": available_slots,
            "slots_by_priority": prio_counts,
            "active_slots_detail": [s.to_dict() for s in active_slots],
            "resources": {
                "cpu_percent": resources.cpu_percent,
                "load_average": resources.load_average,
                "cpu_count": resources.cpu_count,
                "memory_available_mb": resources.memory_available_mb,
                "memory_total_mb": resources.memory_total_mb,
                "disk_free_gb": resources.disk_free_gb,
            },
            "orphan_ffmpeg_count": len(orphans),
            "orphan_ffmpeg_processes": orphans,
        }

    def healthcheck(self) -> Dict[str, Any]:
        """
        Diagnoses orchestrator health:
        - HEALTHY: normal
        - DEGRADED: high CPU, low RAM, orphan FFmpeg detected, Redis latency
        - CRITICAL: disk full (< 1GB), Redis unavailable, active FFmpeg > max allowed
        """
        if not self.slot_manager.is_available():
            return {
                "status": HealthStatus.CRITICAL.value,
                "reason": "Redis is unreachable",
                "details": {},
            }

        resources = self.get_resources()
        active_slots = self.slot_manager.list_slots(self.node_name)
        orphans = self.detect_orphan_ffmpeg_processes()

        # Check CRITICAL conditions
        if resources.disk_free_gb < 1.0:
            return {
                "status": HealthStatus.CRITICAL.value,
                "reason": f"Disk critically low: {resources.disk_free_gb} GB remaining",
                "details": {"disk_free_gb": resources.disk_free_gb},
            }

        if len(active_slots) > self.max_concurrent:
            return {
                "status": HealthStatus.CRITICAL.value,
                "reason": f"Active slots ({len(active_slots)}) exceeds max ({self.max_concurrent})",
                "details": {"active_slots": len(active_slots), "max": self.max_concurrent},
            }

        # Check DEGRADED conditions
        degraded_reasons = []
        if resources.cpu_percent >= settings.TRANSCODE_CPU_HARD_THRESHOLD:
            degraded_reasons.append(f"High CPU load: {resources.cpu_percent}%")

        if resources.memory_available_mb < settings.TRANSCODE_MIN_AVAILABLE_MEMORY_MB:
            degraded_reasons.append(f"Low memory available: {resources.memory_available_mb} MB")

        if len(orphans) > 0:
            degraded_reasons.append(f"Orphan project FFmpeg processes detected: {len(orphans)}")

        if degraded_reasons:
            return {
                "status": HealthStatus.DEGRADED.value,
                "reason": "; ".join(degraded_reasons),
                "details": {
                    "orphan_ffmpeg_count": len(orphans),
                    "cpu_percent": resources.cpu_percent,
                    "memory_available_mb": resources.memory_available_mb,
                },
            }

        return {
            "status": HealthStatus.HEALTHY.value,
            "reason": "All operational metrics within normal parameters",
            "details": {},
        }


def is_safe_to_kill_ffmpeg(pid: int) -> bool:
    """
    Validates that a process ID is truly an FFmpeg process belonging to this VOD project
    before issuing SIGTERM or SIGKILL. Never kills foreign or system FFmpeg processes.
    """
    if pid <= 1:
        return False
    if psutil is None:
        try:
            import subprocess
            out = subprocess.check_output(
                ["ps", "-p", str(pid), "-o", "command="],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            out_lower = out.lower()
            return "ffmpeg" in out_lower and (
                "storage" in out_lower or "enlaceplus" in out_lower or "hls_playlist_type" in out_lower
            )
        except Exception:
            return False
    try:
        proc = psutil.Process(pid)
        pname = (proc.name() or "").lower()
        if "ffmpeg" not in pname:
            return False
        cmdline = " ".join(proc.cmdline())
        output_root = str(Path(settings.OUTPUT_ROOT).resolve(strict=False))
        staging_root = str(Path(settings.STAGING_ROOT).resolve(strict=False))
        return (
            output_root in cmdline
            or staging_root in cmdline
            or "EnlacePlus" in cmdline
            or "hls_playlist_type" in cmdline
        )
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


# Global singleton instance
transcode_orchestrator = TranscodeOrchestrator()
