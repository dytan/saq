# SAQ
SAQ (Simple Async Queue) is a simple and performant job queueing framework built on top of asyncio and redis or postgres. It can be used for processing background jobs with workers. For example, you could use SAQ to schedule emails, execute long queries, or do expensive data analysis.

[Documentation](https://saq-py.readthedocs.io/)

It uses [redis-py](https://github.com/redis/redis-py) >= 4.2.

It is similar to [RQ](https://github.com/rq/rq) and heavily inspired by [ARQ](https://github.com/samuelcolvin/arq). Unlike RQ, it is async and thus [significantly faster](benchmarks) if your jobs are async. Even if they are not, SAQ is still considerably faster due to lower overhead.

SAQ optionally comes with a simple UI for monitor workers and jobs.

<img src="docs/web.png" alt="SAQ Web UI" style="width:100%;"/>

## Install
```
# minimal install for redis
pip install saq[redis]

# minimal install for postgres
pip install saq[postgres]

# web + hiredis
pip install saq[web,hiredis]
```

## Usage
```
usage: saq [-h] [--workers WORKERS] [--verbose] [--web]
           [--extra-web-settings EXTRA_WEB_SETTINGS]
           [--port PORT] [--check]
           settings

Start Simple Async Queue Worker

positional arguments:
  settings              Namespaced variable containing
                        worker settings eg: eg
                        module_a.settings

options:
  -h, --help            show this help message and exit
  --workers WORKERS     Number of worker processes
  --verbose, -v         Logging level: 0: ERROR, 1: INFO,
                        2: DEBUG
  --web                 Start web app. By default, this
                        only monitors the current
                        worker's queue. To monitor
                        multiple queues, see '--extra-
                        web-settings'
  --extra-web-settings EXTRA_WEB_SETTINGS, -e EXTRA_WEB_SETTINGS
                        Additional worker settings to
                        monitor in the web app
  --port PORT           Web app port, defaults to 8080
  --check               Perform a health check

environment variables:
  AUTH_USER     basic auth user, defaults to admin
  AUTH_PASSWORD basic auth password, if not specified, no auth will be used
```

## Example

```python
import asyncio

from saq import CronJob, Queue


class DBHelper:
    """Helper class for demo purposes"""

    async def disconnect(self):
        print("Disconnecting from the database")

    async def connect(self):
        print("Connectiong...")

    def __str__(self):
        return "Your DBHelper at work"

# all functions take in context dict and kwargs
async def test(ctx, *, a):
    await asyncio.sleep(0.5)
    # result should be json serializable
    # custom serializers and deserializers can be used through Queue(dump=,load=)
    return {"x": a}

async def cron(ctx):
    print("i am a cron job")

async def startup(ctx):
    helper = DBHelper()
    await helper.connect()
    ctx["db"] = helper

async def shutdown(ctx):
    await ctx["db"].disconnect()

async def before_process(ctx):
    print(ctx["job"], ctx["db"])

async def after_process(ctx):
    pass

queue = Queue.from_url("redis://localhost")

settings = {
    "queue": queue,
    "functions": [test],
    "concurrency": 10,
    "cron_jobs": [CronJob(cron, cron="* * * * * */5")], # run every 5 seconds
    "startup": startup,
    "shutdown": shutdown,
    "before_process": before_process,
    "after_process": after_process,
}
```

To start the worker, assuming the previous is available in the python path

```
saq module.file.settings
```

> **_Note:_** `module.file.settings` can also be a callable returning the settings dictionary.

To enqueue jobs

```python
# schedule a job normally
job = await queue.enqueue("test", a=1)

# wait 1 second for the job to complete
await job.refresh(1)
print(job.results)

# run a job and return the result
print(await queue.apply("test", a=2))

# run a job with custom polling interval to check status more frequently
print(await queue.apply("test", a=2, poll_interval=0.1))

# Run multiple jobs concurrently and collect the results into a list
print(await queue.map("test", [{"a": 3}, {"a": 4}]))

# schedule a job in 10 seconds
await queue.enqueue("test", a=1, scheduled=time.time() + 10)
```

## Demo

Start the worker

```
python -m saq examples.simple.settings --web
```

Navigate to the [web ui](http://localhost:8080])

Enqueue jobs
```
python examples/simple.py
```

## Comparison to ARQ
SAQ is heavily inspired by [ARQ](https://github.com/samuelcolvin/arq) but has several enhancements.

1. Avoids polling by leveraging [BLMOVE](https://redis.io/commands/blmove) or [RPOPLPUSH](https://redis.io/commands/rpoplpush) and NOTIFY
    1. SAQ has much lower latency than ARQ, with delays of < 5ms. ARQ's default polling frequency is 0.5 seconds
	  2. SAQ is up to [8x faster](benchmarks) than ARQ
2. Web interface for monitoring queues and workers
3. Heartbeat monitor for abandoned jobs
4. More robust failure handling
    1. Storage of stack traces
    2. Sweeping stuck jobs
    3. Handling of cancelled jobs different from failed jobs (machine redeployments)
5. Before and after job hooks
6. Easily run multiple workers to leverage more cores


---

## Update Summary (base: v0.26.3 → current)

### New Features

#### DistributedSemaphore (`saq/semaphore.py`)
A Redis-backed global concurrency cap enforced across every worker process and host in the fleet via atomic Lua scripts.

- `try_acquire()` / `acquire()` — non-blocking / blocking slot acquisition
- `release()` — return a slot (Lua atomically clamps to zero)
- `reset(value)` — admin recovery from crashed workers
- `slot()` — async context manager
- Exposed as `saq.DistributedSemaphore`

#### Priority Queue (`saq/priority.py`, `saq/queue/redis.py`)
Tri-level job priority implemented as composite Redis sorted-set scores.

- `Priority.HIGH = 3` / `Priority.NORMAL = 2` / `Priority.LOW = 1`
- Score formula: `priority × 10¹⁵ − enqueue_ms` — different priority tiers never overlap; within a tier, earlier enqueue wins (FIFO)
- `compute_priority_score(priority, enqueue_ms)` / `decode_priority_score(score)` exposed as `saq.compute_priority_score` / `saq.decode_priority_score`
- Legacy scheduled jobs (priority=0) use plain timestamp scores for backward compatibility

#### Redis Cluster Support (`saq/queue/redis.py`)
- `REDIS_USE_CLUSTER=true` env var enables Redis Cluster mode
- Uses hash-tagged keys (`{app:saq:name}:…`) so multi-key Lua scripts operate on one slot
- `get_queue_redis_namespace()` / `get_queue_redis_pending_key()` / `get_queue_redis_job_key()` helper functions

#### APP_PREFIX Isolation (`saq/queue/base.py`, `saq/job.py`)
- `APP_NAME` env var (default `"saq"`) prefixes all Redis keys
- Affects: `saq:job:`, `saq:abort:`, `saq:slots:` namespaces
- Allows multiple independent SAQ deployments on the same Redis instance

### Worker Changes (`saq/worker.py`)

- `Worker(semaphore=DistributedSemaphore(...))` — integrates fleet-wide slot control into the job loop
- `Worker(empty_queue_wait_time=...)` — backoff interval when queue is empty
- Semaphore counter auto-reconciled on startup against actual active job count
- `_sweep()` wrapper re-reconciles semaphore after stuck-job cleanup
- `worker_info()` includes semaphore status in metadata
- `dequeue_timeout` used as semaphore-backoff duration when fleet is at capacity

### Type Changes (`saq/types.py`)

- `SemaphoreInfo` TypedDict: `running`, `available`, `max`
- `PartialTimersDict` — all `Worker` timer keys optional
- `semaphore: DistributedSemaphore` field added to `SettingsDict`

### New Examples

- `examples/priority_semaphore.py` — demo combining `Priority` with `DistributedSemaphore`
- `examples/monitor_fastapi.py` — FastAPI-based monitoring endpoint

---

## Development

```
python -m venv env
source env/bin/activate
pip install -e ".[dev,web]"
docker run -d -p 6379:6379 redis
docker run -d -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres
make style test
```
