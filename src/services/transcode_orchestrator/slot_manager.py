import json
import logging
import time
from typing import List, Optional, Tuple, Dict, Any
from redis import Redis, exceptions as redis_exceptions

from src.core.queues import get_redis_connection
from src.core.config import settings
from src.services.transcode_orchestrator.models import (
    TranscodeSlot,
    TranscodeRedisKeys,
    Priority,
)

logger = logging.getLogger(__name__)


# Lua script for 100% atomic slot admission and registration
LUA_ACQUIRE_SLOT = """
local slots_set = KEYS[1]
local node_slots_set = KEYS[2]

local slot_id = ARGV[1]
local slot_json = ARGV[2]
local ttl = tonumber(ARGV[3])
local max_concurrent = tonumber(ARGV[4])
local reserved_priority = tonumber(ARGV[5])
local is_priority = tonumber(ARGV[6])

-- 1. Prune dead slot references from sets
local existing = redis.call("SMEMBERS", slots_set)
for _, sid in ipairs(existing) do
    if redis.call("EXISTS", "vod:transcode:slot:" .. sid) == 0 then
        redis.call("SREM", slots_set, sid)
        redis.call("SREM", node_slots_set, sid)
    end
end

-- 2. Count active slots
local active_count = redis.call("SCARD", slots_set)

-- 3. Check absolute maximum concurrency
if active_count >= max_concurrent then
    return {"error", "max_concurrency_reached", tostring(active_count)}
end

-- 4. Check priority reservation
if is_priority == 0 then
    local normal_cap = max_concurrent - reserved_priority
    if normal_cap < 0 then
        normal_cap = 0
    end
    if active_count >= normal_cap then
        return {"error", "reserved_priority_capacity", tostring(active_count)}
    end
end

-- 5. Atomically create slot and register
local slot_key = "vod:transcode:slot:" .. slot_id
redis.call("SET", slot_key, slot_json, "EX", ttl)
redis.call("SADD", slots_set, slot_id)
redis.call("SADD", node_slots_set, slot_id)

return {"ok", "slot_acquired", tostring(active_count + 1)}
"""

# Lua script to atomically renew slot lease (heartbeat)
LUA_HEARTBEAT_SLOT = """
local slot_key = KEYS[1]
local slot_json = ARGV[1]
local ttl = tonumber(ARGV[2])

if redis.call("EXISTS", slot_key) == 1 then
    redis.call("SET", slot_key, slot_json, "EX", ttl)
    return 1
else
    return 0
end
"""

# Lua script to atomically release a slot
LUA_RELEASE_SLOT = """
local slots_set = KEYS[1]
local node_slots_set = KEYS[2]
local slot_key = KEYS[3]
local slot_id = ARGV[1]

redis.call("DEL", slot_key)
redis.call("SREM", slots_set, slot_id)
redis.call("SREM", node_slots_set, slot_id)
return 1
"""


class RedisSlotManager:
    """Manages transcode slots in Redis with atomic Lua scripts, TTL leases, and heartbeats."""

    def __init__(self, redis_conn: Optional[Redis] = None):
        self._redis = redis_conn
        self._lua_acquire = None
        self._lua_heartbeat = None
        self._lua_release = None

    @property
    def redis(self) -> Redis:
        if self._redis is None:
            self._redis = get_redis_connection()
        return self._redis

    def _ensure_lua_scripts(self):
        if self._lua_acquire is None:
            self._lua_acquire = self.redis.register_script(LUA_ACQUIRE_SLOT)
            self._lua_heartbeat = self.redis.register_script(LUA_HEARTBEAT_SLOT)
            self._lua_release = self.redis.register_script(LUA_RELEASE_SLOT)

    def is_available(self) -> bool:
        """Pings Redis to check connectivity."""
        try:
            return bool(self.redis.ping())
        except Exception:
            return False

    def acquire_slot(
        self,
        slot: TranscodeSlot,
        max_concurrent: int,
        reserved_priority_slots: int,
        ttl_seconds: int,
    ) -> Tuple[bool, str, int]:
        """
        Atomically requests and claims a transcode slot via Lua script.
        Returns: (success: bool, reason: str, active_slots: int)
        """
        self._ensure_lua_scripts()
        prio_enum = Priority.from_str(slot.priority)
        is_prio = 1 if prio_enum.is_priority() else 0

        keys = [
            TranscodeRedisKeys.SLOTS_SET,
            TranscodeRedisKeys.node_slots_key(slot.node_id),
        ]
        args = [
            slot.slot_id,
            slot.to_json(),
            ttl_seconds,
            max_concurrent,
            reserved_priority_slots,
            is_prio,
            time.time(),
        ]

        result = self._lua_acquire(keys=keys, args=args)
        status = result[0].decode() if isinstance(result[0], bytes) else str(result[0])
        reason = result[1].decode() if isinstance(result[1], bytes) else str(result[1])
        active = int(result[2].decode() if isinstance(result[2], bytes) else result[2])

        return status == "ok", reason, active

    def heartbeat_slot(self, slot: TranscodeSlot, ttl_seconds: int) -> bool:
        """Renews the lease of an existing slot."""
        self._ensure_lua_scripts()
        slot.heartbeat_at = time.time()
        slot_key = TranscodeRedisKeys.slot_key(slot.slot_id)

        result = self._lua_heartbeat(
            keys=[slot_key],
            args=[slot.to_json(), ttl_seconds],
        )
        return int(result) == 1

    def release_slot(self, slot: TranscodeSlot) -> bool:
        """Releases and removes a transcode slot."""
        self._ensure_lua_scripts()
        slot_key = TranscodeRedisKeys.slot_key(slot.slot_id)
        keys = [
            TranscodeRedisKeys.SLOTS_SET,
            TranscodeRedisKeys.node_slots_key(slot.node_id),
            slot_key,
        ]
        args = [slot.slot_id]

        result = self._lua_release(keys=keys, args=args)
        return int(result) == 1

    def update_slot(self, slot: TranscodeSlot) -> bool:
        """Updates slot data in Redis while preserving its remaining TTL."""
        slot_key = TranscodeRedisKeys.slot_key(slot.slot_id)
        ttl = self.redis.ttl(slot_key)
        if ttl <= 0:
            ttl = settings.TRANSCODE_SLOT_TTL_SECONDS
        self.redis.set(slot_key, slot.to_json(), ex=ttl)
        return True

    def get_slot(self, slot_id: str) -> Optional[TranscodeSlot]:
        """Fetches a single slot by ID."""
        slot_key = TranscodeRedisKeys.slot_key(slot_id)
        raw = self.redis.get(slot_key)
        if not raw:
            return None
        raw_str = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        return TranscodeSlot.from_json(raw_str)

    def list_slots(self, node_id: Optional[str] = None) -> List[TranscodeSlot]:
        """Lists active transcode slots (optionally filtered by node). Prunes stale IDs."""
        set_key = (
            TranscodeRedisKeys.node_slots_key(node_id)
            if node_id
            else TranscodeRedisKeys.SLOTS_SET
        )

        slot_ids = self.redis.smembers(set_key)
        slots: List[TranscodeSlot] = []
        dead_ids: List[str] = []

        for sid in slot_ids:
            sid_str = sid.decode("utf-8") if isinstance(sid, bytes) else str(sid)
            s = self.get_slot(sid_str)
            if s:
                slots.append(s)
            else:
                dead_ids.append(sid_str)

        # Prune expired slot keys from set
        if dead_ids:
            with self.redis.pipeline() as pipe:
                for did in dead_ids:
                    pipe.srem(TranscodeRedisKeys.SLOTS_SET, did)
                    if node_id:
                        pipe.srem(TranscodeRedisKeys.node_slots_key(node_id), did)
                pipe.execute()

        return slots

    def count_active_slots(self, node_id: Optional[str] = None) -> int:
        """Returns the number of verified active slots."""
        return len(self.list_slots(node_id=node_id))

    def prune_stale_slots(self, node_id: Optional[str] = None) -> int:
        """Explicitly prunes dangling slot IDs from Redis sets."""
        set_key = (
            TranscodeRedisKeys.node_slots_key(node_id)
            if node_id
            else TranscodeRedisKeys.SLOTS_SET
        )
        slot_ids = self.redis.smembers(set_key)
        pruned = 0
        for sid in slot_ids:
            sid_str = sid.decode("utf-8") if isinstance(sid, bytes) else str(sid)
            slot_key = TranscodeRedisKeys.slot_key(sid_str)
            if not self.redis.exists(slot_key):
                self.redis.srem(TranscodeRedisKeys.SLOTS_SET, sid_str)
                if node_id:
                    self.redis.srem(TranscodeRedisKeys.node_slots_key(node_id), sid_str)
                pruned += 1
        return pruned
