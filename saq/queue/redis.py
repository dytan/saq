"""
Redis Queue
"""

from __future__ import annotations

import asyncio
import logging
import json
import time
import typing as t

from saq.errors import MissingDependencyError
from saq.job import (
    Job,
    Status,
)
from saq.multiplexer import Multiplexer
from saq.queue.base import Queue, logger
from saq.utils import millis, now, now_seconds

try:
    from redis import asyncio as aioredis
except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for Redis. Install them with `pip install saq[redis]`."
    ) from e

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from redis.asyncio.client import Redis, PubSub
    from redis.commands.core import AsyncScript

    from saq.types import (
        CountKind,
        DumpType,
        ListenCallback,
        LoadType,
        QueueInfo,
        VersionTuple,
        WorkerInfo,
    )

ID_PREFIX = "saq:job:"

_PRIORITY_WEIGHT = 1_000_000_000_000_000  # 10^15

# ── Score helpers ──────────────────────────────────────────────────────────────
#
# score = priority × 10^15 − enqueue_time_ms
#
# Tier separation: adjacent priority weights are 10^15 apart.
# A timestamp in milliseconds is ~1.7e12 today, so the subtracted value is
# ~1000x smaller than the tier gap — tiers never overlap.
#
# Within a tier the score *decreases* as enqueue_ms grows, so the EARLIEST
# job has the HIGHEST score → strict FIFO per priority tier.
#
# Float64 precision: max score = 3×10^15 < 2^53 ≈ 9.007×10^15  ✓
#
# ZPOPMAX always pops the element with the HIGHEST score, so:
#   priority=3 (HIGH)   → scores ≈ [2.998e15, 3.000e15)
#   priority=2 (NORMAL) → scores ≈ [1.998e15, 2.000e15)
#   priority=1 (LOW)    → scores ≈ [0.998e15, 1.000e15)
#   priority=0 (legacy) → plain scheduled timestamp (backward-compat)


def compute_priority_score(priority: int, enqueue_ms: int | None = None) -> float:
    """
    Composite Redis sorted-set score.

    score = priority × 10^15 − enqueue_time_ms

    * Higher priority always wins over lower priority (different magnitude).
    * Within the same priority tier, earlier enqueue → higher score → FIFO.
    * priority=0 falls back to the job's ``scheduled`` epoch-seconds value
      so that pre-existing scheduled jobs continue to work unchanged.
    """
    if priority == 0:
        # Legacy / scheduled path: score is simply the scheduled timestamp
        # (or 0 meaning "run immediately").  Returning it as-is keeps the
        # existing schedule-based ordering for jobs that never set a priority.
        return float(enqueue_ms or 0)
    if enqueue_ms is None:
        enqueue_ms = int(time.time() * 1000)
    return float(priority * _PRIORITY_WEIGHT - enqueue_ms)


def decode_priority_score(score: float) -> tuple[int, int]:
    """
    Reverse a composite score back to (priority, enqueue_ms).

    Uses ``round(score / W)`` rather than ``int(score) // W`` because the
    composite score is ``priority * W - enqueue_ms``, which sits strictly
    *below* the next integer multiple of W (e.g. priority=3 gives a score
    in [2.998e15, 3.000e15)).  Plain integer floor-division would therefore
    always return ``priority - 1`` for any non-zero enqueue_ms; rounding to
    the nearest multiple correctly recovers the original priority.

    Returns (0, int(score)) for legacy/scheduled scores (priority == 0).
    """
    weight = round(score / _PRIORITY_WEIGHT)
    if weight == 0:
        return 0, int(score)
    enqueue = weight * _PRIORITY_WEIGHT - int(score)
    return weight, enqueue


# ── Lua scripts ────────────────────────────────────────────────────────────────
#
# All scripts are registered once at __init__ time; Redis caches them by SHA-1
# so subsequent calls only transmit the 40-byte hash, not the full source.

# ENQUEUE
# ───────
# Atomically:
#   1. Guard: skip if already in the incomplete set OR an abort sentinel exists.
#   2. ZADD the job_id into the *priority* sorted set with its composite score.
#   3. ZADD the job_id into the *incomplete* sorted set with its scheduled time
#      (used by the scheduler to promote delayed jobs).
#   4. Persist the serialised job blob under its own key.
#
# KEYS[1]  incomplete sorted set   (saq:<name>:incomplete)
# KEYS[2]  job id string           (saq:job:<name>:<key>)
# KEYS[3]  priority sorted set     (saq:<name>:queued)   ← was a LIST
# KEYS[4]  abort sentinel key      (saq:abort:<key>)
# ARGV[1]  serialised job bytes
# ARGV[2]  scheduled epoch-seconds (0 = enqueue immediately)
# ARGV[3]  priority score          (composite float as string)
#
_LUA_ENQUEUE = """
if not redis.call('ZSCORE', KEYS[1], KEYS[2]) and redis.call('EXISTS', KEYS[4]) == 0 then
    redis.call('SET',  KEYS[2], ARGV[1])
    redis.call('ZADD', KEYS[1], ARGV[2], KEYS[2])
    if ARGV[2] == '0' then
        redis.call('ZADD', KEYS[3], ARGV[3], KEYS[2])
    end
    return 1
else
    return nil
end
"""

# DEQUEUE  (non-blocking)
# ───────────────────────
# Atomically ZPOPMAX the highest-score job_id from the priority queue,
# push it into the active list, and return the job_id.
#
# KEYS[1]  priority sorted set  (saq:<name>:queued)
# KEYS[2]  active list          (saq:<name>:active)
#
_LUA_DEQUEUE = """
local res = redis.call('ZPOPMAX', KEYS[1])
if #res == 0 then return false end
local job_id = res[1]
redis.call('RPUSH', KEYS[2], job_id)
return job_id
"""

# SCHEDULE PROMOTE
# ────────────────
# Move jobs whose scheduled time has arrived from the incomplete set into
# the priority queue sorted set.
#
# KEYS[1]  schedule lock key
# KEYS[2]  incomplete sorted set
# KEYS[3]  priority sorted set
# ARGV[1]  lock TTL seconds
# ARGV[2]  current epoch-seconds
#
_LUA_SCHEDULE = """
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('SETEX', KEYS[1], ARGV[1], 1)
    local jobs = redis.call('ZRANGEBYSCORE', KEYS[2], 1, ARGV[2])
    for _, v in ipairs(jobs) do
        redis.call('ZADD', KEYS[2], 0, v)
        local raw = redis.call('GET', v)
        if raw then
            -- Re-use priority=0 score (scheduled timestamp) so the job
            -- lands at the front of the NORMAL tier when promoted.
            redis.call('ZADD', KEYS[3], 0, v)
        end
    end
    return jobs
end
"""

# SWEEP  (stuck-job cleanup)
# ──────────────────────────
# KEYS[1]  sweep lock key
# KEYS[2]  active list
# ARGV[1]  lock TTL seconds
#
_LUA_SWEEP = """
local id_jobs = {}
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('SETEX', KEYS[1], ARGV[1], 1)
    for i, v in ipairs(redis.call('LRANGE', KEYS[2], 0, -1)) do
        id_jobs[i] = {v, redis.call('GET', v)}
    end
end
return id_jobs
"""

# ABORT
# ─────
# Remove from the priority queue (ZREM) instead of LREM.
#
# KEYS[1]  priority sorted set
# KEYS[2]  incomplete sorted set
# KEYS[3]  job key
# KEYS[4]  abort sentinel key
# ARGV[1]  serialised job bytes
# ARGV[2]  abort sentinel TTL
# ARGV[3]  abort error string
# ARGV[4]  status string
#
_LUA_ABORT = """
local removed = redis.call('ZREM', KEYS[1], KEYS[3])
redis.call('ZREM', KEYS[2], KEYS[3])
redis.call('SET',  KEYS[3], ARGV[1])
redis.call('SETEX', KEYS[4], ARGV[2], ARGV[3])
redis.call('PUBLISH', KEYS[3], ARGV[4])
return removed
"""

# RETRY
# ─────
# Remove from active list and either re-add to the priority queue (immediate
# retry) or reschedule in the incomplete set (delayed retry).
#
# KEYS[1]  active list
# KEYS[2]  priority sorted set
# KEYS[3]  incomplete sorted set
# KEYS[4]  job key
# ARGV[1]  serialised job bytes
# ARGV[2]  priority score  (or "" to skip immediate re-queue)
# ARGV[3]  scheduled timestamp (used when ARGV[2] == "")
# ARGV[4]  status string
#
_LUA_RETRY = """
redis.call('LREM', KEYS[1], 1, KEYS[4])
if ARGV[2] ~= '' then
    redis.call('ZADD', KEYS[2], ARGV[2], KEYS[4])
    redis.call('ZADD', KEYS[3], 0, KEYS[4])
else
    redis.call('ZADD', KEYS[3], ARGV[3], KEYS[4])
end
redis.call('SET', KEYS[4], ARGV[1])
redis.call('PUBLISH', KEYS[4], ARGV[4])
return 1
"""

# DLQ SEND
# ─────────
# Move a permanently-failed job into the dead-letter sorted set.
# Score = failed_at unix timestamp so it is ordered by failure time.
#
# KEYS[1]  DLQ sorted set   (saq:<name>:dlq)
# KEYS[2]  job key
# ARGV[1]  failed_at timestamp (float as string)
# ARGV[2]  serialised job bytes
#
_LUA_DLQ_SEND = """
redis.call('ZADD', KEYS[1], ARGV[1], KEYS[2])
redis.call('SET',  KEYS[2], ARGV[2])
return 1
"""


class RedisQueue(Queue):
    """
    Queue is used to interact with redis.

    The queued key is now a **sorted set** (ZADD / BZPOPMAX) instead of a
    plain LIST.  Each member's score is a composite value that encodes both
    priority tier and enqueue timestamp::

        score = priority × 10^15 − enqueue_time_ms

    This gives strict priority ordering across tiers with FIFO ordering
    within each tier, using a single ZPOPMAX call with no tier scanning.

    Additional features over the original list-based implementation:

    * **Dead-letter queue (DLQ)** — permanently-failed jobs are moved to
      ``saq:<name>:dlq`` for inspection and replay.
    * **Peek** — inspect the top-N queued jobs without dequeuing them.
    * **Priority stats** — ``count("queued_by_priority")`` breaks the queue
      depth down by priority tier.

    Args:
        redis: instance of redis.asyncio pool
        name: name of the queue (default "default")
        dump: lambda that takes a dictionary and outputs bytes (default ``json.dumps``)
        load: lambda that takes str or bytes and outputs a python dictionary (default ``json.loads``)
        max_concurrent_ops: maximum concurrent operations (default 20).
            Throttles calls to ``enqueue``, ``job``, and ``abort`` to prevent
            the Queue from consuming too many Redis connections.
        swept_error_message: The error message to use when sweeping jobs (default "swept").
    """

    @classmethod
    def from_url(cls: type[RedisQueue], url: str, **kwargs: t.Any) -> RedisQueue:
        """Create a queue with a redis url and a name."""
        return cls(aioredis.from_url(url), **kwargs)

    def __init__(
        self,
        redis: Redis[bytes],  # type: ignore[type-arg]
        name: str = "default",
        dump: DumpType | None = None,
        load: LoadType | None = None,
        max_concurrent_ops: int = 20,
        swept_error_message: str | None = None,
    ) -> None:
        super().__init__(name=name, dump=dump, load=load, swept_error_message=swept_error_message)

        self.redis = redis
        self._version: VersionTuple | None = None

        # Lazy-registered Lua scripts (registered on first use)
        self._schedule_script: AsyncScript | None = None
        self._enqueue_script: AsyncScript | None = None
        self._cleanup_script: AsyncScript | None = None
        self._dequeue_script: AsyncScript | None = None
        self._abort_script: AsyncScript | None = None
        self._retry_script: AsyncScript | None = None
        self._dlq_send_script: AsyncScript | None = None

        # Redis key names
        self._incomplete = self.namespace("incomplete")  # sorted set — scheduled epoch
        self._queued     = self.namespace("queued")      # sorted set — priority score  ← was LIST
        self._active     = self.namespace("active")      # list       — in-flight job ids
        self._schedule   = self.namespace("schedule")    # string     — schedule lock
        self._sweep      = self.namespace("sweep")       # string     — sweep lock
        self._stats      = self.namespace("stats")       # sorted set — worker heartbeats
        self._dlq        = self.namespace("dlq")         # sorted set — dead-letter queue

        self._op_sem = asyncio.Semaphore(max_concurrent_ops)
        self._pubsub = PubSubMultiplexer(redis.pubsub(), prefix=f"{ID_PREFIX}{self.name}")

    # ── Helpers ────────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<redis={self.redis}, name='{self.name}'>"

    def job_id(self, job_key: str) -> str:
        return f"{ID_PREFIX}{self.name}:{job_key}"

    def job_key_from_id(self, job_id: str) -> str:
        return job_id[len(f"{ID_PREFIX}{self.name}:"):]

    def namespace(self, key: str) -> str:
        return ":".join(["saq", self.name, key])

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def disconnect(self) -> None:
        await self._pubsub.close()
        if hasattr(self.redis, "aclose"):
            await self.redis.aclose()
        else:
            await self.redis.close()
        await self.redis.connection_pool.disconnect()

    async def version(self) -> VersionTuple:
        if self._version is None:
            info = await self.redis.info()
            self._version = tuple(int(i) for i in str(info["redis_version"]).split("."))
        return self._version

    # ── Info / counts ──────────────────────────────────────────────────────────

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> QueueInfo:
        """
        Returns info on the queue.

        Args:
            jobs: Include job info (default False)
            offset: Offset of job info for pagination (default 0)
            limit: Max length of job info (default 10)
        """
        worker_uuids = []
        for key in await self.redis.zrangebyscore(self._stats, now(), "inf"):
            key_str = key.decode("utf-8")
            *_, worker_uuid = key_str.split(":")
            worker_uuids.append(worker_uuid)

        worker_metadata = await self.redis.mget(
            self.namespace(f"worker_info:{worker_uuid}") for worker_uuid in worker_uuids
        )
        workers: dict[str, WorkerInfo] = {}
        worker_metadata_dict = dict(zip(worker_uuids, worker_metadata))
        for worker in worker_uuids:
            metadata = worker_metadata_dict.get(worker)
            if metadata:
                workers[worker] = json.loads(metadata)

        queued     = await self.count("queued")
        active     = await self.count("active")
        incomplete = await self.count("incomplete")

        if jobs:
            # Fetch from the active list + the top of the priority queue
            active_ids = await self.redis.lrange(self._active, offset, limit - 1)
            queued_ids = await self.redis.zrevrange(self._queued, offset, limit - 1)
            all_ids    = list(dict.fromkeys(active_ids + queued_ids))  # dedup, preserve order
            deserialized_jobs = (
                self.deserialize(job_bytes)
                for job_bytes in await self.redis.mget(all_ids)
            )
            job_info = list(
                {
                    job["key"]: job
                    for job in (job.to_dict() for job in deserialized_jobs if job is not None)
                }.values()
            )
        else:
            job_info = []

        return {
            "workers":   workers,
            "name":      self.name,
            "queued":    queued,
            "active":    active,
            "scheduled": incomplete - queued - active,
            "jobs":      job_info,
        }

    async def count(self, kind: CountKind) -> int:
        """
        Gets count of the kind provided by CountKind.

        Args:
            kind: The type of count you want.  One of:
                ``"queued"``     — jobs in the priority queue waiting to run
                ``"active"``     — jobs currently being executed
                ``"incomplete"`` — all non-terminal jobs (queued + active + scheduled)
        """
        if kind == "queued":
            return await self.redis.zcard(self._queued)   # sorted set, not list
        if kind == "active":
            return await self.redis.llen(self._active)
        if kind == "incomplete":
            return await self.redis.zcard(self._incomplete)
        raise ValueError(f"Can't count unknown type {kind!r}")

    # ── Scheduling ─────────────────────────────────────────────────────────────

    async def schedule(self, lock: int = 1) -> t.List[str]:
        """
        Promote scheduled jobs whose time has arrived into the priority queue.

        Returns a list of job IDs that were promoted.
        """
        if not self._schedule_script:
            self._schedule_script = self.redis.register_script(_LUA_SCHEDULE)

        promoted = await self._schedule_script(
            keys=[self._schedule, self._incomplete, self._queued],
            args=[lock, now_seconds()],
        ) or []

        return [
            job_id.decode("utf-8") if isinstance(job_id, bytes) else job_id
            for job_id in promoted
        ]

    # ── Sweep ──────────────────────────────────────────────────────────────────

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        """
        Abort stuck/expired active jobs and optionally retry or fail them.

        Returns a list of job IDs that were swept.
        """
        if not self._cleanup_script:
            self._cleanup_script = self.redis.register_script(_LUA_SWEEP)

        id_jobs = await self._cleanup_script(
            keys=[self._sweep, self._active],
            args=[lock],
            client=self.redis,
        )

        swept = []

        for job_id, job_bytes in id_jobs:
            job = self.deserialize(job_bytes)

            if job:
                if job.status != Status.ACTIVE or job.stuck:
                    logger.info(
                        "Sweeping job %s",
                        job.info(logger.isEnabledFor(logging.DEBUG)),
                    )
                    swept.append(job_id)
                    await self.abort(job, error=self.swept_error_message)

                    try:
                        await job.refresh(abort)
                    except asyncio.TimeoutError:
                        logger.info("Could not abort job %s", job_id)

                    if job.retryable:
                        await self.retry(job, error=self.swept_error_message)
                    else:
                        await self.finish(job, Status.ABORTED, error=self.swept_error_message)
            else:
                swept.append(job_id)
                async with self.redis.pipeline(transaction=True) as pipe:
                    await (
                        pipe.lrem(self._active, 0, job_id)
                            .zrem(self._incomplete, job_id)
                            .execute()
                    )
                logger.info("Sweeping missing job %s", job_id)

        return [
            job_id.decode("utf-8") if isinstance(job_id, bytes) else job_id
            for job_id in swept
        ]

    # ── Pub/sub notify ─────────────────────────────────────────────────────────

    async def notify(self, job: Job) -> None:
        await self.redis.publish(job.id, job.status)

    # ── Job storage ────────────────────────────────────────────────────────────

    async def _update(self, job: Job, status: Status | None = None, **kwargs: t.Any) -> None:
        if not status:
            stored = await self.job(job.key)
            status = stored.status if stored else None
        job.status = status or job.status
        await self.redis.set(job.id, self.serialize(job))
        await self.notify(job)

    async def job(self, job_key: str) -> Job | None:
        return await self._get_job_by_id(self.job_id(job_key))

    async def jobs(self, job_keys: Iterable[str]) -> t.List[Job | None]:
        return [
            self.deserialize(job_bytes)
            for job_bytes in await self.redis.mget(self.job_id(key) for key in job_keys)
        ]

    async def iter_jobs(
        self,
        statuses: t.List[Status] = list(Status),
        batch_size: int = 100,
    ) -> t.AsyncIterator[Job]:
        cursor = 0
        while True:
            cursor, job_ids = await self.redis.scan(
                cursor=cursor, match=self.job_id("*"), count=batch_size
            )
            statuses_set = set(statuses)
            for job in await self.jobs(
                self.job_key_from_id(job_id.decode("utf-8")) for job_id in job_ids
            ):
                if job and job.status in statuses_set:
                    yield job

            if cursor <= 0:
                break

    async def _get_job_by_id(self, job_id: bytes | str) -> Job | None:
        async with self._op_sem:
            return self.deserialize(await self.redis.get(job_id))

    # ── Abort ──────────────────────────────────────────────────────────────────

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        """
        Mark a job as aborting and remove it from the priority queue.

        If the job was still in the queued sorted set it is considered
        immediately aborted.  If it was already active the worker will
        notice the abort sentinel key and finish the abort.
        """
        if not self._abort_script:
            self._abort_script = self.redis.register_script(_LUA_ABORT)

        async with self._op_sem:
            job.status = Status.ABORTING
            job.error  = error

            dequeued = await self._abort_script(
                keys=[self._queued, self._incomplete, job.id, job.abort_id],
                args=[
                    self.serialize(job),
                    int(ttl),
                    error,
                    job.status,
                ],
            )

            if dequeued:
                await self.finish(job, Status.ABORTED, error=error)
                await self.redis.delete(job.abort_id)
            else:
                await self.redis.lrem(self._active, 0, job.id)

    # ── Dequeue ────────────────────────────────────────────────────────────────

    async def dequeue(self, timeout: float = 0.0, poll_interval: float = 0.0) -> Job | None:
        """
        Dequeue the highest-priority ready job.

        Uses ``BZPOPMAX`` (blocking pop from sorted set) which requires
        Redis ≥ 5.0.  Falls back to a non-blocking ``ZPOPMAX`` loop when
        ``timeout == 0`` and ``poll_interval > 0``.

        Args:
            timeout:
                How long to block waiting for a job, in seconds.
                ``0`` means return immediately if the queue is empty.
            poll_interval:
                When > 0, used as the sleep interval between non-blocking
                polls instead of a true blocking call.  Only relevant for
                the Postgres backend; Redis always uses BZPOPMAX.
        """
        if not self._dequeue_script:
            self._dequeue_script = self.redis.register_script(_LUA_DEQUEUE)

        if timeout > 0:
            # Blocking path: BZPOPMAX blocks up to `timeout` seconds.
            # Returns (key_bytes, member_bytes, score) or None on timeout.
            result = await self.redis.bzpopmax(self._queued, timeout=timeout)
            if result is None:
                logger.debug("Dequeue timed out")
                return None
            _key, job_id_raw, _score = result
            job_id = (
                job_id_raw.decode("utf-8")
                if isinstance(job_id_raw, bytes)
                else job_id_raw
            )
            # BZPOPMAX already removed the job from the sorted set;
            # push it onto the active list so sweep can observe it.
            await self.redis.rpush(self._active, job_id)
            return await self._get_job_by_id(job_id)

        # Non-blocking path: atomic Lua ZPOPMAX + RPUSH.
        job_id_raw = await self._dequeue_script(
            keys=[self._queued, self._active],
        )
        if not job_id_raw:
            return None

        job_id = (
            job_id_raw.decode("utf-8")
            if isinstance(job_id_raw, bytes)
            else job_id_raw
        )
        return await self._get_job_by_id(job_id)

    # ── Pub/sub listen ─────────────────────────────────────────────────────────

    async def listen(
        self,
        job_keys: Iterable[str],
        callback: ListenCallback,
        timeout: float | None = 10,
        poll_interval: float = 0.5,
    ) -> None:
        job_ids = [self.job_id(job_key) for job_key in job_keys]
        if not job_ids:
            return

        async for message in self._pubsub.listen(*job_ids, timeout=timeout):
            job_id  = message["channel"]
            job_key = self.job_key_from_id(job_id)
            status  = Status[message["data"].decode("utf-8").upper()]
            if asyncio.iscoroutinefunction(callback):
                stop = await callback(job_key, status)
            else:
                stop = callback(job_key, status)
            if stop:
                break

    # ── Finish abort ───────────────────────────────────────────────────────────

    async def finish_abort(self, job: Job) -> None:
        await self.redis.delete(job.abort_id)
        await super().finish_abort(job)

    # ── Worker info ────────────────────────────────────────────────────────────

    async def write_worker_info(
        self,
        worker_id: str,
        info: WorkerInfo,
        ttl: int,
    ) -> None:
        current = now()
        async with self.redis.pipeline(transaction=True) as pipe:
            key = self.namespace(f"worker_info:{worker_id}")
            await (
                pipe.setex(key, ttl, json.dumps(info))
                    .zremrangebyscore(self._stats, 0, current)
                    .zadd(self._stats, {key: current + millis(ttl)})
                    .expire(self._stats, ttl)
                    .execute()
            )

    # ── Retry ──────────────────────────────────────────────────────────────────

    async def _retry(self, job: Job, error: str | None) -> None:
        """
        Re-queue a job after a failure.

        Immediate retries (no delay) get re-scored with the job's current
        priority so they land at the correct position in the priority queue.
        Delayed retries are placed back in the incomplete sorted set with
        their scheduled timestamp, and the schedule poller will promote them.
        """
        if not self._retry_script:
            self._retry_script = self.redis.register_script(_LUA_RETRY)

        job_id           = job.id
        next_retry_delay = job.next_retry_delay()

        if next_retry_delay:
            # Delayed retry: schedule in the future; score = "" signals Lua
            # to use the incomplete-set path.
            scheduled = time.time() + next_retry_delay
            priority_score_arg = ""
            scheduled_arg      = str(scheduled)
        else:
            # Immediate retry: compute a fresh priority score so ZPOPMAX
            # serves it at the correct tier position.
            priority_score_arg = str(
                compute_priority_score(job.priority, int(time.time() * 1000))
            )
            scheduled_arg = str(job.scheduled)

        await self._retry_script(
            keys=[self._active, self._queued, self._incomplete, job_id],
            args=[
                self.serialize(job),
                priority_score_arg,
                scheduled_arg,
                str(job.status),
            ],
        )
        await self.notify(job)

    # ── Finish ─────────────────────────────────────────────────────────────────

    async def _finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
    ) -> None:
        job_id = job.id

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe = (
                pipe.lrem(self._active, 1, job_id)
                    .zrem(self._incomplete, job_id)
                    .zrem(self._queued, job_id)   # remove from priority queue too
            )

            if job.ttl > 0:
                pipe = pipe.setex(job_id, job.ttl, self.serialize(job))
            elif job.ttl == 0:
                pipe = pipe.set(job_id, self.serialize(job))
            else:
                pipe.delete(job_id)

            await pipe.execute()
            await self.notify(job)

        # Permanently-failed jobs go to the DLQ for inspection / replay.
        if status == Status.FAILED:
            await self._send_to_dlq(job)

    # ── Enqueue ────────────────────────────────────────────────────────────────

    async def _enqueue(self, job: Job) -> Job | None:
        """
        Atomically enqueue a job into both the priority sorted set and the
        incomplete sorted set.

        The priority score encodes both the job's ``priority`` field and its
        enqueue timestamp so that ZPOPMAX always returns the highest-priority,
        earliest-queued job in O(log N).
        """
        if not self._enqueue_script:
            self._enqueue_script = self.redis.register_script(_LUA_ENQUEUE)

        # Compute composite score from priority + current time
        enqueue_ms     = int(time.time() * 1000)
        priority_score = compute_priority_score(job.priority, enqueue_ms)

        async with self._op_sem:
            if not await self._enqueue_script(
                keys=[self._incomplete, job.id, self._queued, job.abort_id],
                args=[
                    self.serialize(job),
                    job.scheduled,           # 0 = enqueue immediately
                    priority_score,          # composite score for sorted set
                ],
                client=self.redis,
            ):
                return None

        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job

    # ── Dead-letter queue ──────────────────────────────────────────────────────

    async def _send_to_dlq(self, job: Job) -> None:
        """
        Move a permanently-failed job into the dead-letter sorted set.

        The DLQ is a Redis sorted set keyed by ``saq:<name>:dlq`` where
        each member is the full job ID and the score is the failure
        Unix timestamp.  This makes it easy to paginate by failure time.
        """
        if not self._dlq_send_script:
            self._dlq_send_script = self.redis.register_script(_LUA_DLQ_SEND)

        await self._dlq_send_script(
            keys=[self._dlq, job.id],
            args=[time.time(), self.serialize(job)],
        )
        logger.warning(
            "DLQ  %s  id=%s",
            job.function,
            job.id,
        )

    async def replay_dlq(self, limit: int = 100) -> int:
        """
        Re-enqueue up to ``limit`` dead-letter jobs.

        Each replayed job is reset to ``Status.QUEUED`` with its original
        priority and a fresh enqueue timestamp, then removed from the DLQ.

        Args:
            limit: Maximum number of DLQ jobs to replay (default 100).

        Returns:
            The number of jobs actually re-enqueued.
        """
        job_ids  = await self.redis.zrange(self._dlq, 0, limit - 1)
        replayed = 0

        for raw_id in job_ids:
            job_id = raw_id.decode("utf-8") if isinstance(raw_id, bytes) else raw_id
            job    = await self._get_job_by_id(job_id)
            if not job:
                continue

            # Reset job state for replay
            job.status   = Status.QUEUED
            job.error    = None
            job.attempts = 0
            job.queued   = now()
            job.started  = 0
            job.completed = 0
            job.scheduled = 0

            # Re-enqueue via the normal path so all hooks fire
            if await self._enqueue(job) is not None:
                await self.redis.zrem(self._dlq, job_id)
                replayed += 1

        logger.info("DLQ replay: %d jobs re-enqueued", replayed)
        return replayed

    async def dlq_count(self) -> int:
        """Return the current number of jobs in the dead-letter queue."""
        return await self.redis.zcard(self._dlq)

    # ── Peek ──────────────────────────────────────────────────────────────────

    async def peek(self, count: int = 10) -> list[tuple[Job, float]]:
        """
        Return the top ``count`` jobs WITHOUT removing them from the queue.

        Uses ``ZREVRANGE … WITHSCORES`` so no jobs are consumed.

        Args:
            count: How many jobs to peek at (default 10).

        Returns:
            List of ``(Job, score)`` tuples in descending priority order.
        """
        raw_pairs = await self.redis.zrevrange(
            self._queued, 0, count - 1, withscores=True
        )
        results: list[tuple[Job, float]] = []

        for job_id_raw, score in raw_pairs:
            job = await self._get_job_by_id(job_id_raw)
            if job is not None:
                results.append((job, score))

        return results

    # ── Priority stats ─────────────────────────────────────────────────────────

    async def priority_counts(self) -> dict[str, int]:
        """
        Return the number of queued jobs broken down by priority tier.

        Uses ``ZCOUNT`` with the score ranges derived from
        ``compute_priority_score`` to count jobs in each tier without
        iterating the full set.

        Returns:
            Dict with keys ``"HIGH"``, ``"NORMAL"``, ``"LOW"``, ``"legacy"``
            and ``"total"``.

        Example::

            {"HIGH": 3, "NORMAL": 12, "LOW": 5, "legacy": 0, "total": 20}
        """
        tiers = {
            "HIGH":   3,
            "NORMAL": 2,
            "LOW":    1,
        }
        counts: dict[str, int] = {}
        now_ms = int(time.time() * 1000)

        for name, weight in tiers.items():
            # Score range for this tier:
            #   lo = weight × W − (now_ms + 1h)   ← furthest future enqueue
            #   hi = weight × W − 0               ← zero timestamp (theoretical max)
            lo = compute_priority_score(weight, now_ms + 3_600_000)
            hi = compute_priority_score(weight, 0)
            counts[name] = await self.redis.zcount(self._queued, lo, hi)

        # Legacy jobs (priority == 0) have scores in [0, 1e15)
        counts["legacy"] = await self.redis.zcount(self._queued, 0, _PRIORITY_WEIGHT - 1)
        counts["total"]  = sum(counts.values())
        return counts


# ── PubSub multiplexer ─────────────────────────────────────────────────────────


class PubSubMultiplexer(Multiplexer):
    """
    Handle multiple in-process channels over a single Redis pub/sub connection.

    We use pub/sub for real-time job status updates.  Each pub/sub instance
    needs its own connection, and that connection cannot be reused after it
    is closed.  This class multiplexes all job-status messages for a queue
    over a single ``psubscribe`` pattern subscription and routes them
    in-process, so we only ever hold one extra Redis connection per queue
    regardless of how many jobs are being awaited concurrently.
    """

    def __init__(self, pubsub: PubSub, prefix: str) -> None:
        super().__init__()
        self.prefix = prefix
        self.pubsub = pubsub

    async def _start(self) -> None:
        await self.pubsub.psubscribe(f"{self.prefix}*")

        while True:
            try:
                message = await self.pubsub.get_message(timeout=None)  # type: ignore[arg-type]
                if message and message["type"] == "pmessage":
                    message["channel"] = message["channel"].decode("utf-8")
                    self.publish(message["channel"], message)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Failed to consume pub/sub message")

    async def _close(self) -> None:
        await self.pubsub.punsubscribe()
