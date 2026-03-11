"""
Types
"""

from __future__ import annotations

import typing as t
import typing_extensions as te
from collections.abc import Collection
from typing_extensions import Required, TypedDict, Generic

JsonDict = t.Dict[str, t.Any]

if t.TYPE_CHECKING:
    from asyncio import Task

    from saq.job import CronJob, Job, Status
    from saq.semaphore import DistributedSemaphore
    from saq.worker import Worker
    from saq.queue import Queue

# Provide a runtime-safe alias for DistributedSemaphore so that
# get_type_hints() on SettingsDict can resolve the annotation without
# triggering circular imports.  Static type-checkers see the real type
# from the TYPE_CHECKING block above; at runtime this is just Any.
if not t.TYPE_CHECKING:
    DistributedSemaphore = t.Any  # type: ignore[assignment,misc]


class Context(TypedDict, total=False):
    """
    Task context.

    Extra context fields are allowed.
    """

    worker: Required[Worker]
    "Worker currently executing the task"
    job: Job
    "Job() instance of the task"
    queue: Queue
    "Queue the task is running on"
    exception: t.Optional[Exception]
    "Exception raised by the task if any"


class JobTaskContext(TypedDict, total=True):
    """
    Jobs Task Context
    """

    task: Task[t.Any]
    "asyncio Task of the Job"
    aborted: t.Optional[str]
    "If this task has been aborted, this is the reason"


class WorkerInfo(TypedDict):
    """
    Worker Info
    """

    queue_key: t.Optional[str]
    stats: t.Optional[WorkerStats]
    metadata: t.Optional[dict[str, t.Any]]


class QueueInfo(TypedDict):
    """
    Queue Info
    """

    workers: dict[str, WorkerInfo]
    "Worker information"
    name: str
    "Queue name"
    queued: int
    "Number of jobs currently in the queue"
    active: int
    "Number of jobs currently active"
    scheduled: int
    jobs: list[dict[str, t.Any]]
    "A truncated list containing the jobs that are scheduled to execute soonest"


class WorkerStats(TypedDict):
    """
    Worker Stats
    """

    complete: int
    "Number of complete tasks"
    failed: int
    "Number of failed tasks"
    retried: int
    "Number of retries"
    aborted: int
    "Number of aborted tasks"
    uptime: int
    "Queue uptime in milliseconds"


class SemaphoreInfo(TypedDict):
    """
    Distributed Semaphore status snapshot.

    Returned by :meth:`saq.semaphore.DistributedSemaphore.status`.
    """

    running: int
    "Number of slots currently acquired (jobs in flight across the fleet)"
    available: int
    "Number of slots still free"
    max: int
    "Total capacity (max_slots supplied at construction time)"


class TimersDict(TypedDict):
    """
    Timers Dictionary
    """

    schedule: int
    "How often we poll to schedule jobs in seconds (default 1)"
    worker_info: int
    "How often to update worker info, stats and metadata in seconds (default 10)"
    sweep: int
    "How often to clean up stuck jobs in seconds (default 60)"
    abort: int
    "How often to check if a job is aborted in seconds (default 1)"


class PartialTimersDict(TimersDict, total=False):
    """
    For argument to `Worker`, all keys are not required
    """


CtxType = t.TypeVar("CtxType", bound=Context)

# Re-export so callers can import from saq.types without reaching into
# individual sub-modules.
__all__ = [
    "BeforeEnqueueType",
    "Context",
    "CountKind",
    "CtxType",
    "DumpType",
    "DurationKind",
    "Function",
    "FunctionsType",
    "JobTaskContext",
    "JsonDict",
    "LifecycleFunctionsType",
    "ListenCallback",
    "LoadType",
    "PartialTimersDict",
    "QueueInfo",
    "ReceivesContext",
    "SemaphoreInfo",
    "SettingsDict",
    "TimersDict",
    "VersionTuple",
    "WorkerInfo",
    "WorkerStats",
]




class SettingsDict(TypedDict, Generic[CtxType], total=False):
    """
    Settings
    """

    queue: Queue
    functions: Required[FunctionsType[CtxType]]
    concurrency: int
    cron_jobs: Collection[CronJob]
    startup: ReceivesContext[CtxType]
    shutdown: ReceivesContext[CtxType]
    before_process: ReceivesContext[CtxType]
    after_process: ReceivesContext[CtxType]
    timers: PartialTimersDict
    dequeue_timeout: float
    semaphore: DistributedSemaphore
    "Optional :class:`~saq.semaphore.DistributedSemaphore` for fleet-wide concurrency control"


P = te.ParamSpec("P")

BeforeEnqueueType = t.Callable[["Job"], t.Awaitable[t.Any]]
CountKind = t.Literal["queued", "active", "incomplete"]
DumpType = t.Callable[[t.Mapping[t.Any, t.Any]], t.Union[bytes, str]]
DurationKind = t.Literal["process", "start", "total", "running"]
Function = t.Callable[te.Concatenate[CtxType, ...], t.Any]
FunctionsType: te.TypeAlias = Collection[t.Union[Function[CtxType], tuple[str, Function[CtxType]]]]
ReceivesContext = t.Callable[[CtxType], t.Any]
LifecycleFunctionsType = t.Union[ReceivesContext[CtxType], Collection[ReceivesContext[CtxType]]]
ListenCallback = t.Callable[[str, "Status"], t.Any]
LoadType = t.Callable[[t.Union[bytes, str]], t.Any]
VersionTuple = t.Tuple[int, ...]
