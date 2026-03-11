"""
Priority Queue + Distributed Semaphore — SAQ Integration Example
─────────────────────────────────────────────────────────────────
Demonstrates every feature added by the pqueue.py integration:

  • Priority.HIGH / NORMAL / LOW jobs  →  strict tier ordering via ZPOPMAX
  • DistributedSemaphore              →  fleet-wide concurrency cap
  • Dead-letter queue (DLQ)           →  inspect & replay permanently-failed jobs
  • Peek                              →  non-destructive sorted-set inspection
  • Priority stats                    →  per-tier queue depth

Run modes
─────────
  # 1 — start a worker (blocks until SIGINT / SIGTERM)
  python -m saq.examples.priority_semaphore worker

  # 2 — enqueue a demo batch and show the sorted-set layout
  python -m saq.examples.priority_semaphore submit

  # 3 — replay every job currently in the DLQ
  python -m saq.examples.priority_semaphore replay-dlq

  # 4 — print current queue stats and semaphore status
  python -m saq.examples.priority_semaphore stats

Prerequisites
─────────────
  pip install "saq[redis]"
  # Redis must be reachable at REDIS_URL (default redis://localhost:6379)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from typing import Any

APP_NAME = "test_saq"
os.environ["APP_NAME"] = APP_NAME
# ── SAQ public API ─────────────────────────────────────────────────────────────
import saq

print(saq.__file__)
from saq import Job, Status, Worker
from saq.priority import Priority
from saq.queue.redis import RedisQueue, compute_priority_score, decode_priority_score
from saq.semaphore import DistributedSemaphore

try:
    from redis import asyncio as aioredis
except ImportError as exc:
    raise SystemExit(
        "Redis driver not found.  Install it with:  pip install 'saq[redis]'"
    ) from exc

# ── Configuration ──────────────────────────────────────────────────────────────


REDIS_URL  = os.getenv("REDIS_URL", "redis://10.200.16.74:6379/14")
QUEUE_NAME = "demo"
MAX_SLOTS  = int(os.getenv("MAX_SLOTS", "6"))   # global concurrency cap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  [%(name)s]  %(message)s",
)
log = logging.getLogger("example")


# ══════════════════════════════════════════════════════════════════════════════
# Task functions
# ══════════════════════════════════════════════════════════════════════════════

async def send_notification(ctx: dict[str, Any], *, user_id: str, message: str) -> dict[str, Any]:
    """Simulate sending a push notification (fast, HIGH priority)."""
    await asyncio.sleep(5)
    log.info("  [notify]  user=%s  msg=%r", user_id, message)
    return {"user_id": user_id, "sent": True}


async def generate_report(ctx: dict[str, Any], *, report_id: str) -> dict[str, Any]:
    """Simulate building a report (medium, NORMAL priority)."""
    await asyncio.sleep(4)
    log.info("  [report]  id=%s  rows=2500", report_id)
    return {"report_id": report_id, "rows": 2500}


async def process_batch(
    ctx: dict[str, Any], *, batch_id: str, size: int = 100
) -> dict[str, Any]:
    """Simulate background batch processing (slow, LOW priority)."""
    await asyncio.sleep(size * 0.5)
    log.info("  [batch]   id=%s  size=%d", batch_id, size)
    return {"batch_id": batch_id, "processed": size}


async def failing_task(ctx: dict[str, Any], *, reason: str = "oops") -> None:
    """Always fails — exercises the retry → DLQ path."""
    raise ValueError(reason)


# ══════════════════════════════════════════════════════════════════════════════
# Lifecycle hooks
# ══════════════════════════════════════════════════════════════════════════════

async def on_startup(ctx: dict[str, Any]) -> None:
    import httpx

    ctx["http"] = httpx.AsyncClient()
    log.info("startup: HTTP client ready")


async def on_shutdown(ctx: dict[str, Any]) -> None:
    client = ctx.get("http")
    if client is not None:
        await client.aclose()
    log.info("shutdown: HTTP client closed")


async def before_process(ctx: dict[str, Any]) -> None:
    job: Job | None = ctx.get("job")
    if job:
        log.debug("before_process  fn=%s  key=%s", job.function, job.key)


async def after_process(ctx: dict[str, Any]) -> None:
    job: Job | None = ctx.get("job")
    if job:
        print(job)
        log.debug(
            "after_process   fn=%s  status=%s",
            job.function,
            job.status.name if job.status else "?",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Factory helpers
# ══════════════════════════════════════════════════════════════════════════════

async def build_queue_and_semaphore() -> tuple[RedisQueue, DistributedSemaphore]:
    """Create and return a connected (RedisQueue, DistributedSemaphore) pair."""
    redis     = aioredis.from_url(REDIS_URL, decode_responses=False)
    queue     = RedisQueue(redis, name=QUEUE_NAME)
    print(queue)
    semaphore = DistributedSemaphore(redis, max_slots=MAX_SLOTS, name=QUEUE_NAME)
    await queue.connect()
    return queue, semaphore


def build_worker(queue: RedisQueue, semaphore: DistributedSemaphore) -> Worker:  # type: ignore[type-arg]
    """Assemble the SAQ Worker with all task functions and the semaphore."""
    return Worker(
        queue          = queue,
        functions      = [
            send_notification,
            generate_report,
            process_batch,
            failing_task,
        ],
        concurrency    = MAX_SLOTS,       # local cap (semaphore is the global cap)
        semaphore      = semaphore,
        startup        = on_startup,
        shutdown       = on_shutdown,
        before_process = before_process,
        after_process  = after_process,
        dequeue_timeout= 2.0,
        timers         = {"sweep": 30, "abort": 1, "schedule": 1, "worker_info": 10},
    )


# ══════════════════════════════════════════════════════════════════════════════
# Demo: enqueue a mixed-priority batch
# ══════════════════════════════════════════════════════════════════════════════

async def demo_submit() -> None:
    """
    Enqueue a batch of jobs at different priorities and then
    print the sorted-set layout before any worker touches them.

    Expected ZREVRANGE order (highest score → lowest):
        send_notification   HIGH    score ≈ 2.998e15
        generate_report     NORMAL  score ≈ 1.998e15
        failing_task        NORMAL  score ≈ 1.998e15  (enqueued slightly later)
        process_batch B-000 LOW     score ≈ 0.998e15  (first of the two)
        process_batch B-001 LOW     score slightly lower (FIFO within tier)
    """
    queue, semaphore = await build_queue_and_semaphore()

    # ── NORMAL — standard report ──────────────────────────────────────────────
    await queue.enqueue(
        "generate_report",
        priority = Priority.NORMAL,
        report_id= "R-001",
        retries  = 1,
        timeout  = 30,
    )
    # ── HIGH — user-facing alert ──────────────────────────────────────────────
    await queue.enqueue(
        "send_notification",
        priority    = Priority.HIGH,
        user_id     = "u42",
        message     = "Your export is ready",
        retries     = 2,
        timeout     = 10,
    )

    

    # ── NORMAL — will fail → DLQ after retries exhausted ─────────────────────
    await queue.enqueue(
        "failing_task",
        priority = Priority.NORMAL,
        reason   = "simulated permanent failure",
        retries  = 2,       # will retry twice, then land in DLQ
        timeout  = 5,
    )

    # ── LOW × 2 — background batch work; 10 ms apart to verify FIFO ──────────
    for i in range(2):
        await asyncio.sleep(0.01)   # distinct timestamps → deterministic FIFO order
        await queue.enqueue(
            "process_batch",
            priority = Priority.LOW,
            batch_id = f"B-{i:03d}",
            size     = 50,
            timeout  = 60,
        )

    # ── Peek: non-destructive ZREVRANGE ──────────────────────────────────────
    print("\n── Peek (top 5, highest priority first) ──────────────────────────")
    for job, score in await queue.peek(5):
        priority_val, enqueue_ms = decode_priority_score(score)
        tier = Priority(priority_val).name if priority_val in (1, 2, 3) else "legacy"
        print(
            f"  score={score:>22.0f}  [{tier:<6}]  "
            f"fn={job.function!r:<20}  key={job.key[:12]}"
        )

    # ── Priority counts ───────────────────────────────────────────────────────
    print("\n── Priority counts ───────────────────────────────────────────────")
    counts = await queue.priority_counts()
    print(json.dumps(counts, indent=2))

    # ── Semaphore status ──────────────────────────────────────────────────────
    print("\n── Semaphore status ──────────────────────────────────────────────")
    print(json.dumps(await semaphore.status(), indent=2))

    # ── General queue info ────────────────────────────────────────────────────
    print("\n── Queue info ────────────────────────────────────────────────────")
    info = await queue.info()
    print(json.dumps(
        {k: v for k, v in info.items() if k != "workers"},
        indent=2,
    ))

    await queue.disconnect()


# ══════════════════════════════════════════════════════════════════════════════
# DLQ replay
# ══════════════════════════════════════════════════════════════════════════════

async def demo_replay_dlq() -> None:
    """Re-enqueue all jobs currently sitting in the dead-letter queue."""
    queue, _ = await build_queue_and_semaphore()

    dlq_depth = await queue.dlq_count()
    print(f"DLQ depth before replay: {dlq_depth}")

    replayed = await queue.replay_dlq(limit=200)
    print(f"Replayed {replayed} job(s)")

    print(f"DLQ depth after replay:  {await queue.dlq_count()}")
    await queue.disconnect()


# ══════════════════════════════════════════════════════════════════════════════
# Stats snapshot
# ══════════════════════════════════════════════════════════════════════════════

async def demo_stats() -> None:
    """Print queue counts, priority breakdown, and semaphore status."""
    queue, semaphore = await build_queue_and_semaphore()

    print("\n── Queue counts ──────────────────────────────────────────────────")
    for kind in ("queued", "active", "incomplete"):
        print(f"  {kind:<12} {await queue.count(kind)}")          # type: ignore[arg-type]

    dlq = await queue.dlq_count()
    print(f"  {'dlq':<12} {dlq}")

    print("\n── Priority breakdown ────────────────────────────────────────────")
    print(json.dumps(await queue.priority_counts(), indent=2))

    print("\n── Semaphore ─────────────────────────────────────────────────────")
    print(json.dumps(await semaphore.status(), indent=2))

    print("\n── Top 10 queued jobs ────────────────────────────────────────────")
    for job, score in await queue.peek(10):
        priority_val, _ = decode_priority_score(score)
        tier = Priority(priority_val).name if priority_val in (1, 2, 3) else "legacy"
        print(f"  [{tier:<6}]  {job.function!r:<20}  status={job.status.name if job.status else '?'}")

    await queue.disconnect()


# ══════════════════════════════════════════════════════════════════════════════
# Worker entry-point
# ══════════════════════════════════════════════════════════════════════════════

async def run_worker() -> None:
    """Start the SAQ worker.  Blocks until SIGINT / SIGTERM."""
    queue, semaphore = await build_queue_and_semaphore()
    worker = build_worker(queue, semaphore)

    sem_info = await semaphore.status()
    log.info(
        "Starting worker  queue=%r  concurrency=%d  "
        "semaphore_slots=%d/%d  redis=%s",
        QUEUE_NAME,
        MAX_SLOTS,
        sem_info["running"],
        sem_info["max"],
        REDIS_URL,
    )

    try:
        await worker.start()
    finally:
        await queue.disconnect()


# ══════════════════════════════════════════════════════════════════════════════
# CLI entry-point
# ══════════════════════════════════════════════════════════════════════════════

_USAGE = """\
usage: python -m saq.examples.priority_semaphore <command>

commands:
  worker      Start the SAQ worker (default)
  submit      Enqueue a demo batch and show sorted-set layout
  replay-dlq  Re-enqueue all dead-letter jobs
  stats       Print queue counts, priority breakdown, and semaphore status
"""


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "worker"

    match command:
        case "worker":
            asyncio.run(run_worker())

        case "submit":
            asyncio.run(demo_submit())

        case "replay-dlq":
            asyncio.run(demo_replay_dlq())

        case "stats":
            asyncio.run(demo_stats())

        case "--help" | "-h":
            print(_USAGE)

        case _:
            print(f"Unknown command: {command!r}\n", file=sys.stderr)
            print(_USAGE, file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
