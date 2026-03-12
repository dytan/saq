"""
Workers
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import os
import signal
import sys
import traceback
import threading
import typing as t
import typing_extensions as te
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, tzinfo

from croniter import croniter

from saq.job import Status
from saq.queue import Queue
from saq.types import (
    CtxType,
    FunctionsType,
    JobTaskContext,
    LifecycleFunctionsType,
    SettingsDict,
)
from saq.utils import cancel_tasks, millis, now, uuid1

if t.TYPE_CHECKING:
    from asyncio import Task
    from collections.abc import Callable, Collection, Coroutine

    from aiohttp.web_app import Application

    from saq.job import CronJob, Job
    from saq.semaphore import DistributedSemaphore
    from saq.types import (
        Function,
        PartialTimersDict,
        SemaphoreInfo,
        TimersDict,
        WorkerInfo,
    )


logger = logging.getLogger("saq")

# Type that represents arbitrary json
JsonDict = t.Dict[str, t.Any]


class Worker(t.Generic[CtxType]):
    """
    Worker is used to process and monitor jobs.

    Args:
        id: optional override for the worker id, if not provided, uuid will be used
        queue: instance of saq.queue.Queue
        functions: list of async functions
        concurrency: number of jobs to process concurrently
        cron_jobs: List of CronJob instances.
        cron_tz: timezone for cron scheduler
        startup: async function to call on startup
        shutdown: async function to call on shutdown
        before_process: async function to call before a job processes
        after_process: async function to call after a job processes
        timers: dict with various timer overrides in seconds
            schedule: how often we poll to schedule jobs
            worker_info: how often to update worker info, stats and metadata
            sweep: how often to clean up stuck jobs
            abort: how often to check if a job is aborted
        dequeue_timeout: how long it will wait to dequeue
        burst: whether to stop the worker once all jobs have been processed
        max_burst_jobs: the maximum number of jobs to process in burst mode
        shutdown_grace_period_s: how long to wait for jobs to finish before sending cancellation signals.
        cancellation_hard_deadline_s: how long to wait for a job to finish after sending a cancellation signal.
        metadata: arbitrary data to pass to the worker which it will register with saq
        poll_interval: If > 0.0, dequeue will use polling instead of listen/notify
            to trigger dequeues. This only affects Postgres. (default 0.0)
        semaphore: optional :class:`~saq.semaphore.DistributedSemaphore` instance.
            When provided, the worker acquires one slot from the semaphore before
            starting each job and releases it when the job finishes.  This caps the
            total number of concurrently running jobs **across the entire fleet**
            (all worker processes / hosts sharing the same Redis instance), not just
            within this single process.

            The local ``concurrency`` limit still applies independently — the
            effective per-worker parallelism is ``min(concurrency, semaphore.max_slots)``.

            If the semaphore is full the worker backs off for ``dequeue_timeout``
            seconds before retrying, so it never busy-spins.
    """

    SIGNALS = [signal.SIGINT, signal.SIGTERM] if os.name != "nt" else []

    def __init__(
        self,
        queue: Queue,
        functions: FunctionsType[CtxType],
        *,
        id: t.Optional[str] = None,
        concurrency: int = 10,
        cron_jobs: Collection[CronJob[CtxType]] | None = None,
        cron_tz: tzinfo = timezone.utc,
        startup: LifecycleFunctionsType[CtxType] | None = None,
        shutdown: LifecycleFunctionsType[CtxType] | None = None,
        before_process: LifecycleFunctionsType[CtxType] | None = None,
        after_process: LifecycleFunctionsType[CtxType] | None = None,
        timers: PartialTimersDict | None = None,
        dequeue_timeout: float = 0.0,
        burst: bool = False,
        max_burst_jobs: int | None = None,
        shutdown_grace_period_s: int | None = None,
        cancellation_hard_deadline_s: float = 1.0,
        metadata: t.Optional[JsonDict] = None,
        poll_interval: float = 0.0,
        semaphore: DistributedSemaphore | None = None,
        empty_queue_wait_time: float = 1.0,
    ) -> None:
        self.queue = queue
        self.concurrency = concurrency
        self.semaphore = semaphore
        self.pool = ThreadPoolExecutor()
        self.startup = ensure_coroutine_function_many(startup, self.pool) if startup else None
        self.shutdown = shutdown
        self.before_process = (
            ensure_coroutine_function_many(before_process, self.pool) if before_process else None
        )
        self.after_process = (
            ensure_coroutine_function_many(after_process, self.pool) if after_process else None
        )
        self.timers: TimersDict = {
            "schedule": 1,
            "worker_info": 10,
            "sweep": 60,
            "abort": 1,
        }
        if timers is not None:
            self.timers.update(timers)
        self.event = asyncio.Event()
        functions = set(functions)
        self.functions: dict[str, Function[CtxType]] = {}
        self.cron_jobs: Collection[CronJob] = cron_jobs or []
        self.cron_tz: tzinfo = cron_tz
        self.context: CtxType = t.cast(CtxType, {"worker": self})
        self.tasks: set[Task[t.Any]] = set()
        self.job_task_contexts: dict[Job, JobTaskContext] = {}
        self.dequeue_timeout = dequeue_timeout
        self.empty_queue_wait_time = empty_queue_wait_time or self.dequeue_timeout or 1.0
        self.burst = burst
        self.max_burst_jobs = max_burst_jobs
        self.burst_jobs_processed = 0
        self.burst_jobs_processed_lock = threading.Lock()
        self.burst_condition_met = False
        self._metadata = metadata
        self._poll_interval = poll_interval
        self._stop_lock = asyncio.Lock()
        self._stopped = False
        self._shutdown_grace_period_s = shutdown_grace_period_s
        self._cancellation_hard_deadline_s = cancellation_hard_deadline_s
        self.id = uuid1() if id is None else id

        if self.burst:
            if self.dequeue_timeout <= 0:
                raise ValueError(
                    "dequeue_timeout must be a positive value greater than 0 when the burst mode is enabled"
                )
            if self.max_burst_jobs is not None:
                self.concurrency = min(self.concurrency, self.max_burst_jobs)

        for job in self.cron_jobs:
            if not croniter.is_valid(job.cron):
                raise ValueError(f"Cron is invalid {job.cron}")
            functions.add(job.function)

        for function in functions:
            if isinstance(function, tuple):
                name, function = function
            else:
                name = function.__qualname__

            self.functions[name] = function

    async def _before_process(self, ctx: CtxType) -> None:
        if self.before_process:
            for bp in self.before_process:
                await bp(ctx)

    async def _after_process(self, ctx: CtxType) -> None:
        if self.after_process:
            for ap in self.after_process:
                await ap(ctx)

    async def start(self) -> None:
        """Start processing jobs and upkeep tasks."""
        logger.info("Worker starting: %s", repr(self.queue))
        logger.debug("Registered functions:\n%s", "\n".join(f"  {key}" for key in self.functions))

        if self.semaphore is not None:
            # Reconcile the semaphore counter against the actual number of
            # jobs currently in the active list.  A previous crash may have
            # left the counter inflated (slots were acquired but never
            # released because the process died before the finally block ran).
            actual_active = await self.queue.count("active")
            current_slots = await self.semaphore.running()
            if current_slots != actual_active:
                logger.warning(
                    "DistributedSemaphore counter mismatch on startup — "
                    "resetting %d → %d  key=%s",
                    current_slots,
                    actual_active,
                    self.semaphore._key,
                )
                await self.semaphore.reset(actual_active)
            sem_info = await self.semaphore.status()
            logger.info(
                "DistributedSemaphore active — slots %d/%d  key=%s",
                sem_info["running"],
                sem_info["max"],
                self.semaphore._key,
            )

        try:
            self.event = asyncio.Event()
            async with self._stop_lock:
                self._stopped = False
            loop = asyncio.get_running_loop()

            for signum in self.SIGNALS:
                loop.add_signal_handler(signum, self.event.set)

            if self.startup:
                for s in self.startup:
                    await s(self.context)

            self.tasks.update(await self.upkeep())

            for _ in range(self.concurrency):
                self._process()

            await self.event.wait()

        except asyncio.CancelledError:
            pass
        finally:
            logger.info("Working shutting down")
            await self.stop()
            for signum in self.SIGNALS:
                loop.remove_signal_handler(signum)

    async def stop(self) -> None:
        """Stop the worker and cleanup."""
        self.event.set()
        async with self._stop_lock:
            if self._stopped:
                return

            try:
                all_tasks = list(self.tasks)
                self.tasks.clear()
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*all_tasks, return_exceptions=True),
                        timeout=self._shutdown_grace_period_s or 0,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Some tasks did not finish within the shutdown grace period, requesting cancellation"
                    )
                    cancelled = await cancel_tasks(
                        all_tasks, timeout=self._cancellation_hard_deadline_s
                    )
                    if not cancelled:
                        logger.warning(
                            "Some tasks did not finish cancellation in time, they may be stuck or blocked"
                        )

                if sys.version_info[0:2] < (3, 9):
                    self.pool.shutdown(True)
                else:
                    self.pool.shutdown(True, cancel_futures=True)

                if not self.shutdown:
                    return

                # We can't reuse our task pool here, because we shut it to close tasks
                with ThreadPoolExecutor() as shutdown_pool:
                    shutdown_callbacks = ensure_coroutine_function_many(
                        self.shutdown, shutdown_pool
                    )
                    for s in shutdown_callbacks:
                        await s(self.context)
            finally:
                self._stopped = True

    async def schedule(self, lock: int = 1) -> None:
        for cron_job in self.cron_jobs:
            kwargs = cron_job.__dict__.copy()
            function = kwargs.pop("function").__qualname__
            kwargs["key"] = f"cron:{function}" if kwargs.pop("unique") else None
            start_time = datetime.now(self.cron_tz)
            scheduled = croniter(kwargs.pop("cron"), start_time).get_next()

            await self.queue.enqueue(
                function,
                scheduled=int(scheduled),
                **{k: v for k, v in kwargs.items() if v is not None},
            )

        job_ids = await self.queue.schedule(lock)

        if job_ids:
            logger.info("Scheduled %s", job_ids)

    async def worker_info(self, ttl: int = 60) -> WorkerInfo:
        metadata = dict(self._metadata) if self._metadata else {}
        if self.semaphore is not None:
            try:
                sem_info: SemaphoreInfo = await self.semaphore.status()
                metadata["semaphore"] = sem_info
            except Exception:
                logger.debug("Failed to fetch semaphore status for worker_info", exc_info=True)
        return await self.queue.worker_info(
            self.id, queue_key=self.queue.name, metadata=metadata or None, ttl=ttl
        )

    async def upkeep(self) -> list[Task[None]]:
        """Start various upkeep tasks async."""

        async def poll(
            func: Callable[[int], Coroutine], sleep: int, arg: int | None = None
        ) -> None:
            while not self.event.is_set():
                try:
                    await func(arg or sleep)
                except (Exception, asyncio.CancelledError):
                    if self.event.is_set():
                        return
                    logger.exception("Upkeep task failed unexpectedly")

                await asyncio.sleep(sleep)

        return [
            asyncio.create_task(poll(self.abort, self.timers["abort"])),
            asyncio.create_task(poll(self.schedule, self.timers["schedule"])),
            asyncio.create_task(poll(self._sweep, self.timers["sweep"])),
            asyncio.create_task(
                poll(
                    self.worker_info,
                    self.timers["worker_info"],
                    self.timers["worker_info"] + 1,
                )
            ),
        ]

    async def _sweep(self, lock: int = 60) -> list[str]:
        """Wrapper around ``queue.sweep`` that reconciles the distributed
        semaphore counter after swept jobs are removed from the active list.

        ``queue.sweep`` calls ``queue.finish`` / ``queue.retry`` directly,
        bypassing ``worker.process`` and its ``finally`` block.  That means
        the semaphore is never released for swept jobs, causing the counter
        to permanently over-count after a crash.  After each sweep we reset
        the counter to the number of jobs still in the active list, which is
        the ground-truth for how many slots should be held.
        """
        swept = await self.queue.sweep(lock)
        if swept and self.semaphore is not None:
            actual_active = await self.queue.count("active")
            current_slots = await self.semaphore.running()
            if current_slots != actual_active:
                logger.warning(
                    "Post-sweep semaphore reconcile: counter %d → %d  swept=%s",
                    current_slots,
                    actual_active,
                    swept,
                )
                await self.semaphore.reset(actual_active)
        return swept

    async def abort(self, abort_threshold: float) -> None:
        def get_duration(job: Job) -> float:
            return job.duration("running") or 0

        jobs = [
            job for job in self.job_task_contexts if get_duration(job) >= millis(abort_threshold)
        ]

        if not jobs:
            return

        for job in await self.queue.jobs(job.key for job in jobs):
            if not job or job.status not in (Status.ABORTING, Status.ABORTED):
                continue

            task_data = self.job_task_contexts.get(job, None)
            if not task_data:
                logger.warning("No task data found for job %s", job.id)
                continue

            task = task_data["task"]
            logger.info("Aborting %s", job.id)

            if not task.done():
                task_data["aborted"] = "abort" if job.error is None else job.error
                # abort should be a blocking operation
                _ = await cancel_tasks([task], None)

            await self.queue.finish_abort(job)

    async def process(self) -> bool:
        context: CtxType | None = None
        job: Job | None = None
        _semaphore_acquired = False

        try:
            # ── Distributed semaphore: guard against idle counter inflation ───
            # Only attempt to acquire a semaphore slot when there is actually
            # work queued.  This prevents all `concurrency` worker coroutines
            # from simultaneously holding slots during idle periods (which made
            # the "running" counter fluctuate even with no jobs submitted).
            #
            # Flow:
            #   1. Cheap ZCARD check — skip entirely if queue is empty.
            #   2. try_acquire() — back off if fleet is already at capacity.
            #   3. dequeue() — release immediately if another worker raced us.
            if self.semaphore is not None:
                if await self.queue.count("queued") == 0:
                    await asyncio.sleep(self.empty_queue_wait_time)
                    return False

                if not await self.semaphore.try_acquire():
                    if logger.isEnabledFor(logging.DEBUG):
                        sem_info = await self.semaphore.status()
                        logger.debug(
                            "Semaphore full [%d/%d] — backing off",
                            sem_info["running"],
                            sem_info["max"],
                        )
                    # Yield for dequeue_timeout seconds so we don't busy-spin,
                    # then signal the process loop to try again.
                    if self.dequeue_timeout > 0:
                        await asyncio.sleep(self.dequeue_timeout)
                    return False
                _semaphore_acquired = True

            job = await self.queue.dequeue(
                timeout=self.dequeue_timeout,
                poll_interval=self._poll_interval,
            )

            if job is None:
                # Another worker raced us between the queue check and dequeue.
                # The semaphore slot (if acquired) is released in the finally
                # block via _semaphore_acquired.
                return False

            job.started = now()
            job.attempts += 1
            job.worker_id = self.id
            await job.update(status=Status.ACTIVE)
            context = t.cast(CtxType, {**self.context, "job": job})
            await self._before_process(context)
            logger.info("Processing %s", job.info(logger.isEnabledFor(logging.DEBUG)))

            function = ensure_coroutine_function(self.functions[job.function], self.pool)
            task = asyncio.create_task(function(context, **(job.kwargs or {})))
            self.job_task_contexts[job] = JobTaskContext(task=task, aborted=None)
            try:
                result = await asyncio.wait_for(
                    asyncio.shield(task), job.timeout if job.timeout else None
                )
            except asyncio.TimeoutError:
                # Since we have a shield around the task passed to wait_for,
                # we need to explicitly cancel it on timeout.
                task.cancel()
                raise
            if self.job_task_contexts[job]["aborted"] is None:
                await job.finish(Status.COMPLETE, result=result)
        except asyncio.CancelledError:
            if not job:
                return False
            task_ctx = self.job_task_contexts.get(job)
            assert task_ctx is not None

            task = task_ctx["task"]
            aborted = task_ctx["aborted"]
            if aborted is not None:
                await job.finish(Status.ABORTED, error=aborted)
                return False

            if not task.done():
                cancelled = await cancel_tasks([task], self._cancellation_hard_deadline_s)
                if not cancelled:
                    logger.warning(
                        "Function: %s did not finish cancellation in time, it may be stuck or blocked",
                        job.function,
                        extra={"job_id": job.id},
                    )
                await job.retry("cancelled")
        except Exception as ex:
            if context is not None:
                context["exception"] = ex

            if job:
                logger.exception("Error processing job %s", job)

                # Ensure that the task is done or cancelled
                if task_context := self.job_task_contexts.get(job, None):
                    task = task_context["task"]
                    if not task.done():
                        cancelled = await cancel_tasks([task], self._cancellation_hard_deadline_s)
                        if not cancelled:
                            logger.warning(
                                "Function '%s' did not finish cancellation in time, it may be stuck or blocked",
                                job.function,
                                extra={"job_id": job.id},
                            )

                error = traceback.format_exc()

                if job.retryable:
                    await job.retry(error)
                else:
                    await job.finish(Status.FAILED, error=error)
        finally:
            # Release the distributed semaphore slot regardless of outcome.
            # This runs even when the process coroutine is cancelled so the
            # global counter never permanently leaks.
            if _semaphore_acquired and self.semaphore is not None:
                try:
                    await self.semaphore.release()
                except Exception:
                    logger.warning(
                        "Failed to release semaphore slot — counter may be off by one",
                        exc_info=True,
                    )

            if context:
                if job is not None:
                    self.job_task_contexts.pop(job, None)

                try:
                    await self._after_process(context)
                except (Exception, asyncio.CancelledError):
                    logger.exception("Failed to run after process hook")
        return True

    def _process(self, previous_task: Task | None = None) -> None:
        if previous_task:
            self.tasks.discard(previous_task)

            if self.burst and self._check_burst(previous_task):
                if not any(t.get_name() == "process" for t in self.tasks):
                    # Stop the worker if all process tasks are done
                    self.event.set()
                return

        if not self.event.is_set():
            new_task = asyncio.create_task(self.process(), name="process")
            self.tasks.add(new_task)
            new_task.add_done_callback(self._process)

    def _check_burst(self, previous_task: Task) -> bool:
        if self.burst_condition_met:
            return self.burst_condition_met

        job_dequeued = previous_task.result()
        if not job_dequeued:
            self.burst_condition_met = True
        elif self.max_burst_jobs is not None:
            with self.burst_jobs_processed_lock:
                self.burst_jobs_processed += 1
                if self.burst_jobs_processed >= self.max_burst_jobs:
                    self.burst_condition_met = True
        return self.burst_condition_met


P = te.ParamSpec("P")
R = te.TypeVar("R")

OneOrManyCallable = t.Union[t.Callable[P, R], t.Collection[t.Callable[P, R]]]


def ensure_coroutine_function_many(
    func: OneOrManyCallable[P, R] | OneOrManyCallable[P, Coroutine[t.Any, t.Any, R]],
    pool: ThreadPoolExecutor,
) -> t.List[Callable[P, Coroutine[t.Any, t.Any, R]]]:
    if callable(func):
        return [ensure_coroutine_function(func, pool)]
    return [ensure_coroutine_function(f, pool) for f in func]


def ensure_coroutine_function(
    func: Callable[P, R] | Callable[P, Coroutine[t.Any, t.Any, R]],
    pool: ThreadPoolExecutor,
) -> Callable[P, Coroutine[t.Any, t.Any, R]]:
    if asyncio.iscoroutinefunction(func):
        return func

    async def wrapped(*args: t.Any, **kwargs: t.Any) -> t.Any:
        future = None
        try:
            ctx = contextvars.copy_context()
            future = pool.submit(lambda: ctx.run(func, *args, **kwargs))
            return await asyncio.wrap_future(future)
        except asyncio.CancelledError:
            try:
                # job has already been cancelled, swallow all errors
                if future is not None:
                    await asyncio.wrap_future(future)
            except Exception:
                pass
            raise

    return wrapped


def import_settings(settings: str) -> SettingsDict:
    import importlib

    # given a.b.c, parses out a.b as the module path and c as the variable
    module_path, name = settings.strip().rsplit(".", 1)
    module = importlib.import_module(module_path)
    settings_obj = getattr(module, name)

    if callable(settings_obj):
        settings_obj = settings_obj()

    if not isinstance(settings_obj, dict):
        raise TypeError(
            f"Settings {settings} must be a dictionary or a callable that returns a dictionary, got final type '{type(settings_obj)}'"
        )

    return t.cast(SettingsDict, settings_obj)


def start(
    settings: str,
    web: bool = False,
    extra_web_settings: list[str] | None = None,
    port: int = 8080,
) -> None:
    settings_obj = import_settings(settings)

    if "queue" not in settings_obj:
        settings_obj["queue"] = Queue.from_url("redis://localhost")

    loop = asyncio.new_event_loop()
    worker = Worker(**settings_obj)

    async def worker_start() -> None:
        try:
            await worker.queue.connect()
            await worker.start()
        finally:
            await worker.queue.disconnect()

    if web:
        import aiohttp.web

        from saq.web.aiohttp import create_app

        extra_web_settings = extra_web_settings or []
        web_settings = [settings_obj] + [import_settings(s) for s in extra_web_settings]
        queues = [s["queue"] for s in web_settings if s.get("queue")]

        async def shutdown(_app: Application) -> None:
            await worker.stop()

        app = create_app(queues)
        app.on_shutdown.append(shutdown)

        loop.create_task(worker_start()).add_done_callback(
            lambda _: signal.raise_signal(signal.SIGTERM)
        )
        aiohttp.web.run_app(app, port=port, loop=loop)
    else:
        loop.run_until_complete(worker_start())


async def async_check_health(queue: Queue) -> int:
    await queue.connect()
    info = await queue.info()
    name = info.get("name")
    if name != queue.name:
        logger.warning(
            "Health check failed. Unknown queue name %s. Expected %s",
            name,
            queue.name,
        )
        status = 1
    elif not info.get("workers"):
        logger.warning("No active workers found for queue %s", name)
        status = 1
    else:
        workers = len(info["workers"].values())
        logger.info("Found %d active workers for queue %s", workers, name)
        status = 0

    await queue.disconnect()
    return status


def check_health(settings: str) -> int:
    settings_dict = import_settings(settings)
    loop = asyncio.new_event_loop()
    queue = settings_dict.get("queue") or Queue.from_url("redis://localhost")
    return loop.run_until_complete(async_check_health(queue))
