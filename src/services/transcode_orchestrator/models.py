from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import enum
import json
import time
from typing import Optional, Dict, Any, Tuple


class Priority(str, enum.Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"
    BACKGROUND = "BACKGROUND"

    @classmethod
    def from_str(cls, val: str) -> "Priority":
        if isinstance(val, cls):
            return val
        val_upper = str(val).upper().strip()
        for member in cls:
            if member.value == val_upper or member.name == val_upper:
                return member
        return cls.NORMAL

    @property
    def level(self) -> int:
        """Numeric rank: lower number = higher priority."""
        ranks = {
            Priority.CRITICAL: 1,
            Priority.HIGH: 2,
            Priority.NORMAL: 3,
            Priority.LOW: 4,
            Priority.BACKGROUND: 5,
        }
        return ranks.get(self, 3)

    def is_priority(self) -> bool:
        """True if eligible to use reserved priority slots."""
        return self in (Priority.CRITICAL, Priority.HIGH)

    def __lt__(self, other: "Priority") -> bool:
        if not isinstance(other, Priority):
            return NotImplemented
        return self.level > other.level  # higher rank has lower level number

    def __le__(self, other: "Priority") -> bool:
        if not isinstance(other, Priority):
            return NotImplemented
        return self.level >= other.level

    def __gt__(self, other: "Priority") -> bool:
        if not isinstance(other, Priority):
            return NotImplemented
        return self.level < other.level

    def __ge__(self, other: "Priority") -> bool:
        if not isinstance(other, Priority):
            return NotImplemented
        return self.level <= other.level


def get_priority_for_queue(queue_name: str) -> Priority:
    q = (queue_name or "").lower().strip()
    if "priority" in q:
        return Priority.HIGH
    if "batch" in q:
        return Priority.BACKGROUND
    if "ingest" in q:
        return Priority.NORMAL
    if "tasks" in q:
        return Priority.NORMAL
    return Priority.NORMAL


class ExecutionBackend(str, enum.Enum):
    CPU = "cpu"
    GPU = "gpu"


class ResourceType(str, enum.Enum):
    CPU = "cpu"
    NVIDIA = "nvidia"


class HealthStatus(str, enum.Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    CRITICAL = "CRITICAL"


@dataclass
class TranscodeSlot:
    slot_id: str
    job_id: str
    asset_id: str
    worker_name: str
    queue: str
    node_id: str
    priority: str = Priority.NORMAL.value
    profile: str = "default"
    estimated_weight: int = 1
    execution_backend: str = ExecutionBackend.CPU.value
    resource_type: str = ResourceType.CPU.value
    pid: Optional[int] = None
    host: Optional[str] = None
    started_at: float = field(default_factory=time.time)
    heartbeat_at: float = field(default_factory=time.time)
    last_progress_at: Optional[float] = None
    progress: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TranscodeSlot":
        valid_keys = {
            "slot_id", "job_id", "asset_id", "worker_name", "queue", "node_id",
            "priority", "profile", "estimated_weight", "execution_backend",
            "resource_type", "pid", "host", "started_at", "heartbeat_at",
            "last_progress_at", "progress"
        }
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)

    @classmethod
    def from_json(cls, json_str: str) -> "TranscodeSlot":
        return cls.from_dict(json.loads(json_str))


@dataclass
class SystemResources:
    cpu_percent: float
    load_average: Tuple[float, float, float]
    cpu_count: int
    memory_total_mb: float
    memory_available_mb: float
    memory_percent: float
    disk_free_gb: float
    active_transcodes: int = 0


@dataclass
class AdmissionDecision:
    allowed: bool
    reason: str
    metrics: Dict[str, Any] = field(default_factory=dict)


class TranscodeRedisKeys:
    SLOTS_SET = "vod:transcode:slots"
    LOCK = "vod:transcode:lock"
    METRICS = "vod:transcode:metrics"

    @staticmethod
    def slot_key(slot_id: str) -> str:
        return f"vod:transcode:slot:{slot_id}"

    @staticmethod
    def node_slots_key(node_id: str) -> str:
        return f"vod:transcode:node:{node_id}:slots"
