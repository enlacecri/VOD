import os
import time
import uuid
import threading
import concurrent.futures
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
from redis import Redis

from src.core.config import settings
from src.core.queues import get_redis_connection
from src.services.transcode_orchestrator import (
    TranscodeOrchestrator,
    RedisSlotManager,
    SystemResourceProvider,
    ResourceProvider,
    SystemResources,
    Priority,
    TranscodeSlot,
    HealthStatus,
)
from src.services.transcode_orchestrator.orchestrator import is_safe_to_kill_ffmpeg


class MockResourceProvider(ResourceProvider):
    def __init__(
        self,
        cpu_percent: float = 20.0,
        memory_available_mb: float = 8192.0,
        disk_free_gb: float = 100.0,
        load_avg=(1.0, 1.0, 1.0),
    ):
        self.cpu_percent = cpu_percent
        self.memory_available_mb = memory_available_mb
        self.disk_free_gb = disk_free_gb
        self.load_avg = load_avg

    def get_resources(self, active_transcodes: int = 0) -> SystemResources:
        return SystemResources(
            cpu_percent=self.cpu_percent,
            load_average=self.load_avg,
            cpu_count=8,
            memory_total_mb=16384.0,
            memory_available_mb=self.memory_available_mb,
            memory_percent=50.0,
            disk_free_gb=self.disk_free_gb,
            active_transcodes=active_transcodes,
        )


@pytest.fixture
def redis_conn():
    return get_redis_connection()


@pytest.fixture
def mock_resources():
    return MockResourceProvider()


@pytest.fixture
def orchestrator(redis_conn, mock_resources):
    slot_mgr = RedisSlotManager(redis_conn=redis_conn)
    return TranscodeOrchestrator(
        slot_manager=slot_mgr,
        resource_provider=mock_resources,
        node_name="test-node-01",
        max_concurrent=2,
        reserved_priority_slots=0,
    )


# ==============================================================================
# Unit Tests A through R
# ==============================================================================

def test_a_zero_active_allows_start(orchestrator):
    """Case A: 0 active / max 2 -> allows start."""
    decision = orchestrator.can_start_transcode(Priority.NORMAL)
    assert decision.allowed is True
    assert decision.reason == "capacity_available"
    assert decision.metrics["active_slots"] == 0


def test_b_max_concurrent_reached_rejects(orchestrator):
    """Case B: 2 active / max 2 -> rejects."""
    s1 = orchestrator.acquire_slot("job-1", "asset-1", "vod_tasks", "w1", Priority.NORMAL)
    s2 = orchestrator.acquire_slot("job-2", "asset-2", "vod_tasks", "w2", Priority.NORMAL)
    assert s1 is not None
    assert s2 is not None

    decision = orchestrator.can_start_transcode(Priority.NORMAL)
    assert decision.allowed is False
    assert decision.reason == "max_concurrency_reached"
    assert decision.metrics["active_slots"] == 2

    # Attempting to acquire also returns None
    s3 = orchestrator.acquire_slot("job-3", "asset-3", "vod_tasks", "w3", Priority.NORMAL)
    assert s3 is None


def test_c_atomic_exclusion_last_slot(orchestrator):
    """Case C: Two simultaneous requests for the last slot -> exactly one gets slot."""
    # First slot taken
    orchestrator.acquire_slot("job-0", "asset-0", "vod_tasks", "w0", Priority.NORMAL)

    results = []
    barrier = threading.Barrier(2)

    def racer(j_id):
        barrier.wait()
        res = orchestrator.acquire_slot(j_id, f"asset-{j_id}", "vod_tasks", f"w-{j_id}", Priority.NORMAL)
        results.append(res)

    t1 = threading.Thread(target=racer, args=("job-race-1",))
    t2 = threading.Thread(target=racer, args=("job-race-2",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Exactly one succeeded, one failed
    successes = [r for r in results if r is not None]
    assert len(successes) == 1
    assert len(results) == 2


def test_d_cpu_above_threshold_rejects(orchestrator, mock_resources):
    """Case D: CPU above threshold -> does not start."""
    mock_resources.cpu_percent = 85.0  # Above default start threshold 75.0
    with patch.object(settings, "TRANSCODE_CPU_START_THRESHOLD", 75.0):
        decision = orchestrator.can_start_transcode(Priority.NORMAL)
        assert decision.allowed is False
        assert decision.reason == "cpu_threshold_exceeded"

        slot = orchestrator.acquire_slot("job-cpu", "asset-cpu", "vod_tasks", "w-cpu")
        assert slot is None


def test_e_memory_below_minimum_rejects(orchestrator, mock_resources):
    """Case E: Memory below minimum -> does not start."""
    mock_resources.memory_available_mb = 1024.0  # Min required is 2048 MB
    with patch.object(settings, "TRANSCODE_MIN_AVAILABLE_MEMORY_MB", 2048):
        decision = orchestrator.can_start_transcode(Priority.NORMAL)
        assert decision.allowed is False
        assert decision.reason == "insufficient_memory"

        slot = orchestrator.acquire_slot("job-mem", "asset-mem", "vod_tasks", "w-mem")
        assert slot is None


def test_f_disk_below_minimum_rejects(orchestrator, mock_resources):
    """Case F: Disk below minimum -> does not start."""
    mock_resources.disk_free_gb = 2.0  # Min required is 5 GB
    with patch.object(settings, "TRANSCODE_MIN_FREE_DISK_GB", 5):
        decision = orchestrator.can_start_transcode(Priority.NORMAL)
        assert decision.allowed is False
        assert decision.reason == "insufficient_disk"

        slot = orchestrator.acquire_slot("job-disk", "asset-disk", "vod_tasks", "w-disk")
        assert slot is None


def test_g_slot_released_recovers_capacity(orchestrator):
    """Case G: Slot normally released -> capacity recovered."""
    slot = orchestrator.acquire_slot("job-g", "asset-g", "vod_tasks", "w-g")
    assert slot is not None
    assert orchestrator.slot_manager.count_active_slots() == 1

    orchestrator.release_slot(slot)
    assert orchestrator.slot_manager.count_active_slots() == 0
    decision = orchestrator.can_start_transcode(Priority.NORMAL)
    assert decision.allowed is True


def test_h_slot_expiration_recoverable(orchestrator, redis_conn):
    """Case H: Slot expires (TTL elapsed) -> automatically recovered/pruned."""
    slot = orchestrator.acquire_slot("job-h", "asset-h", "vod_tasks", "w-h")
    assert slot is not None

    # Manually expire the slot key in Redis to simulate TTL expiration
    slot_key = f"vod:transcode:slot:{slot.slot_id}"
    redis_conn.delete(slot_key)

    # list_slots / count_active_slots detects missing slot key and prunes it from set
    active = orchestrator.slot_manager.list_slots()
    assert len(active) == 0
    assert orchestrator.slot_manager.count_active_slots() == 0

    # New job can now acquire the slot
    new_slot = orchestrator.acquire_slot("job-h-2", "asset-h-2", "vod_tasks", "w-h-2")
    assert new_slot is not None


def test_i_heartbeat_renews_lease(orchestrator, redis_conn):
    """Case I: Heartbeat -> renews lease."""
    slot = orchestrator.acquire_slot("job-i", "asset-i", "vod_tasks", "w-i")
    assert slot is not None
    slot_key = f"vod:transcode:slot:{slot.slot_id}"

    # Set low TTL
    redis_conn.expire(slot_key, 10)
    assert redis_conn.ttl(slot_key) <= 10

    # Heartbeat renews to settings.TRANSCODE_SLOT_TTL_SECONDS (60s)
    success = orchestrator.heartbeat(slot)
    assert success is True
    assert redis_conn.ttl(slot_key) > 50


def test_j_finally_releases_slot_on_error(orchestrator):
    """Case J: Worker/job fails -> finally releases slot."""
    slot = orchestrator.acquire_slot("job-j", "asset-j", "vod_tasks", "w-j")
    assert slot is not None

    try:
        raise RuntimeError("Simulated transcode crash")
    except RuntimeError:
        pass
    finally:
        orchestrator.release_slot(slot)

    assert orchestrator.slot_manager.count_active_slots() == 0


def test_k_redis_unavailable_fail_closed(orchestrator):
    """Case K: Redis unavailable -> fail closed."""
    with patch.object(orchestrator.slot_manager, "is_available", return_value=False):
        decision = orchestrator.can_start_transcode(Priority.NORMAL)
        assert decision.allowed is False
        assert decision.reason == "orchestrator_unavailable"

        slot = orchestrator.acquire_slot("job-k", "asset-k", "vod_tasks", "w-k")
        assert slot is None


def test_l_feature_flag_false_legacy_mode(orchestrator):
    """Case L: Feature flag false -> legacy mode (always allows)."""
    with patch.object(settings, "TRANSCODE_ORCHESTRATOR_ENABLED", False):
        decision = orchestrator.can_start_transcode(Priority.NORMAL)
        assert decision.allowed is True
        assert decision.reason == "legacy_mode_orchestrator_disabled"

        slot = orchestrator.acquire_slot("job-l", "asset-l", "vod_tasks", "w-l")
        assert slot is not None
        assert slot.slot_id.startswith("legacy-")


def test_m_priority_reservation_blocks_background(redis_conn, mock_resources):
    """Case M: MAX=4, RESERVED=1, 3 BACKGROUND active -> new BACKGROUND rejected."""
    orch = TranscodeOrchestrator(
        slot_manager=RedisSlotManager(redis_conn=redis_conn),
        resource_provider=mock_resources,
        node_name="test-prio",
        max_concurrent=4,
        reserved_priority_slots=1,
    )

    # 3 Background slots acquired (consumes all normal capacity = 4 - 1 = 3)
    s1 = orch.acquire_slot("bg-1", "a-1", "vod_batch", "w1", Priority.BACKGROUND)
    s2 = orch.acquire_slot("bg-2", "a-2", "vod_batch", "w2", Priority.BACKGROUND)
    s3 = orch.acquire_slot("bg-3", "a-3", "vod_batch", "w3", Priority.BACKGROUND)
    assert s1 is not None and s2 is not None and s3 is not None

    # 4th BACKGROUND is rejected
    decision = orch.can_start_transcode(Priority.BACKGROUND)
    assert decision.allowed is False
    assert decision.reason == "reserved_priority_capacity"

    s4 = orch.acquire_slot("bg-4", "a-4", "vod_batch", "w4", Priority.BACKGROUND)
    assert s4 is None


def test_n_priority_reservation_allows_high(redis_conn, mock_resources):
    """Case N: Same scenario (3 BACKGROUND active, 1 reserved slot) -> new HIGH allowed."""
    orch = TranscodeOrchestrator(
        slot_manager=RedisSlotManager(redis_conn=redis_conn),
        resource_provider=mock_resources,
        node_name="test-prio",
        max_concurrent=4,
        reserved_priority_slots=1,
    )

    orch.acquire_slot("bg-1", "a-1", "vod_batch", "w1", Priority.BACKGROUND)
    orch.acquire_slot("bg-2", "a-2", "vod_batch", "w2", Priority.BACKGROUND)
    orch.acquire_slot("bg-3", "a-3", "vod_batch", "w3", Priority.BACKGROUND)

    # HIGH priority is allowed to use the reserved slot!
    decision = orch.can_start_transcode(Priority.HIGH)
    assert decision.allowed is True
    assert decision.reason == "capacity_available"

    s_high = orch.acquire_slot("hi-1", "a-hi", "vod_priority", "w-hi", Priority.HIGH)
    assert s_high is not None


def test_o_priority_reservation_all_4_occupied_rejects_high(redis_conn, mock_resources):
    """Case O: 4 active slots -> HIGH rejected."""
    orch = TranscodeOrchestrator(
        slot_manager=RedisSlotManager(redis_conn=redis_conn),
        resource_provider=mock_resources,
        node_name="test-prio",
        max_concurrent=4,
        reserved_priority_slots=1,
    )

    orch.acquire_slot("bg-1", "a-1", "vod_batch", "w1", Priority.BACKGROUND)
    orch.acquire_slot("bg-2", "a-2", "vod_batch", "w2", Priority.BACKGROUND)
    orch.acquire_slot("bg-3", "a-3", "vod_batch", "w3", Priority.BACKGROUND)
    orch.acquire_slot("hi-1", "a-hi", "vod_priority", "w-hi", Priority.HIGH)

    # Total 4 active = max concurrent reached. HIGH is rejected now.
    decision = orch.can_start_transcode(Priority.HIGH)
    assert decision.allowed is False
    assert decision.reason == "max_concurrency_reached"


def test_p_stale_slot_repair_eliminates_it(orchestrator, redis_conn):
    """Case P: Stale slot (dead PID) -> repair eliminates it."""
    slot = orchestrator.acquire_slot("job-stale", "asset-stale", "vod_tasks", "w-stale")
    assert slot is not None

    # Attach a dead PID (e.g. 99999999)
    orchestrator.update_slot_pid(slot, 99999999)

    res = orchestrator.repair_slots()
    assert res["repaired_slots"] >= 1
    assert orchestrator.slot_manager.count_active_slots() == 0


def test_q_is_safe_to_kill_ffmpeg_never_touches_foreign_pid():
    """Case Q: Foreign PID -> never touched."""
    # Process 1 (launchd / init) is never killed
    assert is_safe_to_kill_ffmpeg(1) is False
    # Non-existent PID
    assert is_safe_to_kill_ffmpeg(99999999) is False
    # Current python test process is not ffmpeg
    assert is_safe_to_kill_ffmpeg(os.getpid()) is False


def test_r_node_registered_correctly(orchestrator):
    """Case R: Node correctly registered in slot and metrics."""
    slot = orchestrator.acquire_slot("job-r", "asset-r", "vod_tasks", "w-r")
    assert slot.node_id == "test-node-01"

    metrics = orchestrator.get_metrics()
    assert metrics["node_name"] == "test-node-01"
    assert len(metrics["active_slots_detail"]) == 1
    assert metrics["active_slots_detail"][0]["node_id"] == "test-node-01"


# ==============================================================================
# Real Concurrency Test (Section 34)
# ==============================================================================

def test_real_concurrency_stress(redis_conn, mock_resources):
    """
    Section 34: Launch multiple concurrent threads attempting to acquire slots
    simultaneously with MAX=2 and 10 contenders.
    Result: Exactly 2 acquisitions, never 3+ due to race condition.
    Repeated multiple iterations for deterministic confidence.
    """
    for iteration in range(5):
        redis_conn.flushdb()
        orch = TranscodeOrchestrator(
            slot_manager=RedisSlotManager(redis_conn=redis_conn),
            resource_provider=mock_resources,
            node_name=f"stress-node-{iteration}",
            max_concurrent=2,
            reserved_priority_slots=0,
        )

        num_contenders = 10
        barrier = threading.Barrier(num_contenders)
        acquired_slots = []
        lock = threading.Lock()

        def contender(idx):
            barrier.wait()
            s = orch.acquire_slot(
                job_id=f"iter-{iteration}-job-{idx}",
                asset_id=f"asset-{idx}",
                queue="vod_tasks",
                worker_name=f"worker-{idx}",
                priority=Priority.NORMAL,
            )
            if s is not None:
                with lock:
                    acquired_slots.append(s)

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_contenders) as executor:
            futures = [executor.submit(contender, i) for i in range(num_contenders)]
            concurrent.futures.wait(futures)

        assert len(acquired_slots) == 2, f"Iteration {iteration}: Expected exactly 2, got {len(acquired_slots)}"
        assert orch.slot_manager.count_active_slots() == 2


# ==============================================================================
# Controlled FFmpeg Integration Test (Section 35)
# ==============================================================================

def test_ffmpeg_concurrency_limit_integration(redis_conn, mock_resources, tmp_path):
    """
    Section 35: Integration test with MAX_CONCURRENT=1.
    Launch two transcode tasks. First obtains slot; second is rejected/must wait;
    upon first completing, second acquires slot.
    Never were 2 heavy FFmpeg processes running simultaneously.
    """
    orch = TranscodeOrchestrator(
        slot_manager=RedisSlotManager(redis_conn=redis_conn),
        resource_provider=mock_resources,
        node_name="test-integration-node",
        max_concurrent=1,
        reserved_priority_slots=0,
    )

    running_ffmpegs = []
    max_simultaneous = 0
    sim_lock = threading.Lock()

    def fake_transcode(job_id: str, duration_sec: float):
        nonlocal max_simultaneous
        slot = orch.acquire_slot(job_id, f"asset-{job_id}", "vod_tasks", "w-int", Priority.NORMAL)
        if slot is None:
            # Rescheduled / waiting for capacity
            return False, "denied"

        try:
            with sim_lock:
                running_ffmpegs.append(job_id)
                if len(running_ffmpegs) > max_simultaneous:
                    max_simultaneous = len(running_ffmpegs)

            time.sleep(duration_sec)
        finally:
            with sim_lock:
                running_ffmpegs.remove(job_id)
            orch.release_slot(slot)

        return True, "completed"

    # Start first transcode in a thread (runs for 0.5s)
    t1 = threading.Thread(target=fake_transcode, args=("job-1", 0.5))
    t1.start()
    time.sleep(0.1)  # Ensure t1 acquired slot

    # Attempt second transcode while t1 is running
    success_2, reason_2 = fake_transcode("job-2", 0.2)
    assert success_2 is False
    assert reason_2 == "denied"
    assert max_simultaneous == 1

    # Wait for t1 to complete
    t1.join()

    # Now that t1 finished, job-2 can acquire and run
    success_3, reason_3 = fake_transcode("job-2", 0.1)
    assert success_3 is True
    assert reason_3 == "completed"

    # Crucial assertion: Peak simultaneous FFmpeg processes was exactly 1, NEVER 2!
    assert max_simultaneous == 1
