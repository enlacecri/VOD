import abc
import os
import shutil
from typing import Optional, Tuple
from pathlib import Path

from src.core.config import settings
from src.services.transcode_orchestrator.models import SystemResources

try:
    import psutil
except ImportError:
    psutil = None


class ResourceProvider(abc.ABC):
    """Abstract base class for system/cluster resource providers."""

    @abc.abstractmethod
    def get_resources(self, active_transcodes: int = 0) -> SystemResources:
        """Returns the current system resources snapshot."""
        raise NotImplementedError


class SystemResourceProvider(ResourceProvider):
    """
    Standard OS resource provider using psutil (with standard library fallbacks).
    Gathers CPU, memory, load average, and disk usage for the local node.
    """

    def __init__(self, target_disk_path: Optional[str] = None):
        self.target_disk_path = target_disk_path or settings.OUTPUT_ROOT

    def get_resources(self, active_transcodes: int = 0) -> SystemResources:
        cpu_count = os.cpu_count() or 1

        # Load average (1m, 5m, 15m)
        try:
            load_avg = os.getloadavg()
        except (AttributeError, OSError):
            load_avg = (0.0, 0.0, 0.0)

        # CPU percent (smoothed)
        if psutil is not None:
            try:
                # Interval of 0.1 provides a quick real sample without blocking worker
                cpu_pct = psutil.cpu_percent(interval=0.1)
            except Exception:
                cpu_pct = 0.0
        else:
            # Fallback estimation from 1m load average
            cpu_pct = min(100.0, (load_avg[0] / max(cpu_count, 1)) * 100.0)

        # Memory (MB)
        if psutil is not None:
            try:
                vm = psutil.virtual_memory()
                mem_total_mb = vm.total / (1024 * 1024)
                mem_available_mb = vm.available / (1024 * 1024)
                mem_pct = vm.percent
            except Exception:
                mem_total_mb = 8192.0
                mem_available_mb = 4096.0
                mem_pct = 50.0
        else:
            mem_total_mb = 8192.0
            mem_available_mb = 4096.0
            mem_pct = 50.0

        # Disk free (GB)
        try:
            disk_path = Path(self.target_disk_path).resolve(strict=False)
            disk_path.mkdir(parents=True, exist_ok=True)
            usage = shutil.disk_usage(disk_path)
            disk_free_gb = usage.free / (1024 * 1024 * 1024)
        except Exception:
            disk_free_gb = 50.0

        return SystemResources(
            cpu_percent=round(cpu_pct, 1),
            load_average=(round(load_avg[0], 2), round(load_avg[1], 2), round(load_avg[2], 2)),
            cpu_count=cpu_count,
            memory_total_mb=round(mem_total_mb, 1),
            memory_available_mb=round(mem_available_mb, 1),
            memory_percent=round(mem_pct, 1),
            disk_free_gb=round(disk_free_gb, 2),
            active_transcodes=active_transcodes,
        )


class NvidiaResourceProvider(ResourceProvider):
    """Placeholder implementation for future GPU resource tracking."""

    def get_resources(self, active_transcodes: int = 0) -> SystemResources:
        # Falls back to system resources when GPU provider is instantiated without CUDA
        return SystemResourceProvider().get_resources(active_transcodes=active_transcodes)
