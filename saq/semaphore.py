"""
Distributed Semaphore
─────────────────────
A Redis-backed global concurrency cap that works across every worker
process and host in the fleet.

Key layout
──────────
    saq:slots:<name>   STRING   atomic running-job counter

Algorithm
─────────
    Every worker calls ``try_acquire()`` before starting a job.  The Lua
    script atomically reads the counter and conditionally increments it,
    so the fleet never exceeds ``max_slots`` concurrent executions even
    under thundering-herd conditions.

          ┌─Worker A─────────────────────────────────────────────┐
          │  try_acquire()  →  Lua: GET=7, 7<10 → INCR → return 1│
          │  dequeue()      →  job X                             │
          │  … execute …                                         │
          │  release()      →  Lua: DECR                         │
          └──────────────────────────────────────────────────────┘
          ┌─Worker B (concurrent, same Redis)────────────────────┐
          │  try_acquire()  →  Lua: GET=8, 8<10 → INCR → return 1│
          │  dequeue()      →  job Y                             │
          └──────────────────────────────────────────────────────┘

    The slot is acquired BEFORE dequeue and released AFTER execution.
    A process crash between dequeue and release will leave the counter
    off by one.  Call ``reset()`` from a watchdog if needed.

Usage
─────
    Direct (non-blocking probe):

        sem = DistributedSemaphore(redis, max_slots=10)
        if await sem.try_acquire():
            try:
                ...
            finally:
                await sem.release()

    Blocking until a slot is free:

        await sem.acquire()
        try:
            ...
        finally:
            await sem.release()

    Context-manager (recommended):

        async with sem.slot():
            ...   # slot held for the duration of this block

    Integration with SAQ Worker:

        from saq import Worker
        from saq.semaphore import DistributedSemaphore

        sem = DistributedSemaphore(redis, max_slots=10, name="myqueue")
        worker = Worker(queue, functions=[...], semaphore=sem)
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncIterator

if TYPE_CHECKING:
    from redis.asyncio.client import Redis
    from redis.commands.core import AsyncScript

    from saq.types import SemaphoreInfo

logger = logging.getLogger("saq")

# ── Lua scripts ────────────────────────────────────────────────────────────────
#
# Both scripts operate on a single STRING key that holds an integer counter.
# Using Lua makes the check-then-set atomic: no other Redis command can be
# interleaved between the GET and the INCR/DECR.
#

_LUA_ACQUIRE = """
-- Atomically increment the slot counter iff current value < max_slots.
-- Returns 1 if a slot was granted, 0 if the semaphore is full.
local cur = tonumber(redis.call('GET', KEYS[1])) or 0
if cur < tonumber(ARGV[1]) then
    redis.call('INCR', KEYS[1])
    return 1
end
return 0
"""

_LUA_RELEASE = """
-- Atomically decrement the slot counter, but never below zero.
local cur = tonumber(redis.call('GET', KEYS[1])) or 0
if cur > 0 then
    redis.call('DECR', KEYS[1])
end
return 1
"""

_LUA_RESET = """
-- Force-set the counter to an explicit value (default 0).
-- Use from a watchdog to recover from a crashed worker that never
-- released its slot(s).
redis.call('SET', KEYS[1], ARGV[1])
return 1
"""


# ── DistributedSemaphore ───────────────────────────────────────────────────────


class DistributedSemaphore:
    """
    Global concurrency cap enforced by a single Redis counter.

    Args:
        redis:
            An ``redis.asyncio`` client instance.  The client must
            already be connected (i.e. created via
            ``aioredis.from_url(...)``).
        max_slots:
            Maximum number of concurrent job executions across the
            entire fleet.
        name:
            Logical name of the semaphore.  Allows multiple independent
            semaphores on the same Redis instance.  The Redis key used
            is ``saq:slots:<name>``.  Defaults to ``"default"``.
        poll_interval:
            Seconds to wait between retry attempts in the blocking
            :meth:`acquire` spin-loop.  Defaults to ``0.05``.

    The counter persists in Redis, so it survives a worker restart.  If
    a worker crashes while holding a slot call :meth:`reset` to restore
    the counter to its correct value.
    """

    def __init__(
        self,
        redis: Redis,  # type: ignore[type-arg]
        max_slots: int,
        *,
        name: str = "default",
        poll_interval: float = 0.05,
    ) -> None:
        if max_slots < 1:
            raise ValueError(f"max_slots must be >= 1, got {max_slots!r}")

        self.redis = redis
        self.max_slots = max_slots
        self.name = name
        self.poll_interval = poll_interval

        self._key: str = f"saq:slots:{name}"

        # Register scripts once; Redis caches them by SHA-1 so subsequent
        # calls only transmit the hash, not the full Lua source.
        self._acquire_script: AsyncScript = redis.register_script(_LUA_ACQUIRE)
        self._release_script: AsyncScript = redis.register_script(_LUA_RELEASE)
        self._reset_script: AsyncScript = redis.register_script(_LUA_RESET)

    # ── Core operations ────────────────────────────────────────────────────────

    async def try_acquire(self) -> bool:
        """
        Non-blocking attempt to claim one slot.

        Returns:
            ``True`` if a slot was granted, ``False`` if the semaphore
            is at capacity.

        This call is safe to make from many coroutines / processes
        simultaneously: the underlying Lua script is executed atomically
        inside Redis.
        """
        result = await self._acquire_script(
            keys=[self._key],
            args=[self.max_slots],
        )
        return bool(result)

    async def acquire(self, poll_interval: float | None = None) -> None:
        """
        Blocking acquire — spin until a slot becomes available.

        Args:
            poll_interval:
                Override the instance-level poll interval for this call
                only.  Seconds between retry probes.

        This is a cooperative spin-loop: while waiting it yields the
        event loop on every iteration so other coroutines can run.
        """
        wait = poll_interval if poll_interval is not None else self.poll_interval
        while not await self.try_acquire():
            await asyncio.sleep(wait)

    async def release(self) -> None:
        """
        Return one slot to the semaphore.

        Safe to call even if the counter is already at zero (the Lua
        script clamps it to 0).
        """
        await self._release_script(keys=[self._key])

    # ── Context manager ────────────────────────────────────────────────────────

    @asynccontextmanager
    async def slot(self, poll_interval: float | None = None) -> AsyncIterator[None]:
        """
        Async context manager that acquires on enter and releases on exit.

        Args:
            poll_interval:
                Forwarded to :meth:`acquire`.

        Example::

            async with semaphore.slot():
                result = await do_work()
        """
        await self.acquire(poll_interval)
        try:
            yield
        finally:
            await self.release()

    # ── Introspection ──────────────────────────────────────────────────────────

    async def running(self) -> int:
        """Return the current number of active (acquired) slots."""
        raw = await self.redis.get(self._key)
        return int(raw) if raw else 0

    async def available(self) -> int:
        """Return the number of free slots."""
        return max(0, self.max_slots - await self.running())

    async def status(self) -> SemaphoreInfo:
        """
        Return a snapshot dict with ``running``, ``available``, and
        ``max`` keys — suitable for logging, metrics, and the SAQ web UI.
        """
        current = await self.running()
        return {
            "running": current,
            "available": max(0, self.max_slots - current),
            "max": self.max_slots,
        }

    # ── Administration ─────────────────────────────────────────────────────────

    async def reset(self, value: int = 0) -> None:
        """
        Force the counter to ``value`` (default 0).

        Use this from a watchdog or admin script to recover from a
        crashed worker that did not release its slot(s).

        Args:
            value:
                The value to set the counter to.  Must be in
                ``[0, max_slots]``.  Values outside this range are
                clamped silently.
        """
        safe_value = max(0, min(value, self.max_slots))
        await self._reset_script(keys=[self._key], args=[safe_value])
        logger.info(
            "DistributedSemaphore[%s] reset to %d/%d",
            self.name,
            safe_value,
            self.max_slots,
        )

    def __repr__(self) -> str:
        return (
            f"DistributedSemaphore("
            f"name={self.name!r}, "
            f"max_slots={self.max_slots}, "
            f"key={self._key!r})"
        )
