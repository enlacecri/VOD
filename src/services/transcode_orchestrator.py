"""
Transcode Orchestrator Facade Module.
Re-exports the TranscodeOrchestrator and related models and services.
"""

from src.services.transcode_orchestrator.models import (
    Priority,
    ExecutionBackend,
    ResourceType,
    HealthStatus,
    TranscodeSlot,
    SystemResources,
    AdmissionDecision,
    TranscodeRedisKeys,
    get_priority_for_queue,
)
from src.services.transcode_orchestrator.resource_provider import (
    ResourceProvider,
    SystemResourceProvider,
    NvidiaResourceProvider,
)
from src.services.transcode_orchestrator.slot_manager import RedisSlotManager
from src.services.transcode_orchestrator.orchestrator import (
    TranscodeOrchestrator,
    transcode_orchestrator,
)

__all__ = [
    "Priority",
    "ExecutionBackend",
    "ResourceType",
    "HealthStatus",
    "TranscodeSlot",
    "SystemResources",
    "AdmissionDecision",
    "TranscodeRedisKeys",
    "get_priority_for_queue",
    "ResourceProvider",
    "SystemResourceProvider",
    "NvidiaResourceProvider",
    "RedisSlotManager",
    "TranscodeOrchestrator",
    "transcode_orchestrator",
]
