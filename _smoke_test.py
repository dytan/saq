"""Smoke test — run with: python _smoke_test.py"""
import sys, traceback

errors = []

def check(label, fn):
    try:
        fn()
        print(f"  OK  {label}")
    except Exception as e:
        errors.append(label)
        print(f"  FAIL {label}")
        traceback.print_exc()

# ── imports ────────────────────────────────────────────────────────────────────
def test_imports():
    from saq import CronJob, Job, Status, Worker
    from saq import Priority, DistributedSemaphore
    from saq import compute_priority_score, decode_priority_score
    from saq.priority import Priority as P2
    from saq.semaphore import DistributedSemaphore as DS2
    from saq.queue.redis import RedisQueue
    from saq.types import SemaphoreInfo, JsonDict

check("imports", test_imports)

# ── Priority enum ──────────────────────────────────────────────────────────────
def test_priority():
    from saq.priority import Priority
    assert Priority.HIGH > Priority.NORMAL > Priority.LOW, "ordering broken"
    assert Priority.HIGH   == 3
    assert Priority.NORMAL == 2
    assert Priority.LOW    == 1
    # int sub-class arithmetic
    assert Priority.HIGH * 2 == 6
    # from_value coercions
    assert Priority.from_value(3)        is Priority.HIGH
    assert Priority.from_value("2")      is Priority.NORMAL
    assert Priority.from_value("low")    is Priority.LOW
    assert Priority.from_value("HIGH")   is Priority.HIGH
    assert Priority.from_value(Priority.NORMAL) is Priority.NORMAL
    # default alias
    from saq.priority import Priority as P
    assert P.default is P.NORMAL  # type: ignore[attr-defined]

check("Priority enum", test_priority)

# ── Score helpers ──────────────────────────────────────────────────────────────
def test_scores():
    from saq.queue.redis import compute_priority_score, decode_priority_score

    s3 = compute_priority_score(3, 1_700_000_000_000)
    s2 = compute_priority_score(2, 1_700_000_000_000)
    s1 = compute_priority_score(1, 1_700_000_000_000)
    assert s3 > s2 > s1, f"tier ordering broken: {s3} {s2} {s1}"

    # FIFO within a tier: earlier enqueue → higher score
    s_early = compute_priority_score(2, 1_000_000_000)
    s_late  = compute_priority_score(2, 2_000_000_000)
    assert s_early > s_late, "FIFO ordering broken"

    # round-trip
    p, ms = decode_priority_score(s3)
    assert p == 3, f"priority round-trip failed: {p}"
    assert ms == 1_700_000_000_000, f"enqueue_ms round-trip failed: {ms}"

    # legacy path (priority == 0): returns raw score unchanged
    s_legacy = compute_priority_score(0, 42)
    assert s_legacy == 42.0, f"legacy score: {s_legacy}"
    p0, ms0 = decode_priority_score(42.0)
    assert p0 == 0 and ms0 == 42, f"legacy decode: {p0} {ms0}"

    # float64 precision: max score 3e15 < 2^53
    s_max = compute_priority_score(3, 0)
    assert s_max == float(3 * 1_000_000_000_000_000), f"precision: {s_max}"

check("score helpers", test_scores)

# ── DistributedSemaphore construction ─────────────────────────────────────────
def test_semaphore_construct():
    from saq.semaphore import DistributedSemaphore

    class FakeScript:
        def __call__(self, **kw): pass

    class FakeRedis:
        def register_script(self, src):
            return FakeScript()

    sem = DistributedSemaphore(FakeRedis(), max_slots=10, name="myq")  # type: ignore[arg-type]
    assert sem.max_slots == 10
    assert sem.name == "myq"
    assert sem._key == "saq:slots:myq"
    r = repr(sem)
    assert "myq" in r and "10" in r, r

    # default name
    sem2 = DistributedSemaphore(FakeRedis(), max_slots=5)  # type: ignore[arg-type]
    assert sem2._key == "saq:slots:default"

    # invalid max_slots
    try:
        DistributedSemaphore(FakeRedis(), max_slots=0)  # type: ignore[arg-type]
        assert False, "should have raised"
    except ValueError:
        pass

check("DistributedSemaphore construction", test_semaphore_construct)

# ── SemaphoreInfo TypedDict ────────────────────────────────────────────────────
def test_semaphore_info_type():
    from saq.types import SemaphoreInfo
    info: SemaphoreInfo = {"running": 3, "available": 7, "max": 10}
    assert info["running"] + info["available"] == info["max"]

check("SemaphoreInfo TypedDict", test_semaphore_info_type)

# ── RedisQueue key layout ──────────────────────────────────────────────────────
def test_redis_queue_keys():
    # Only test key naming — no live Redis needed
    from saq.queue.redis import RedisQueue

    class FakeConnectionPool:
        async def disconnect(self): pass

    class FakeRedis:
        connection_pool = FakeConnectionPool()
        def register_script(self, src): return src
        def pubsub(self): return self
        def psubscribe(self, *a): pass

    rq = RedisQueue.__new__(RedisQueue)
    rq.name = "testq"
    assert rq.namespace("queued")     == "saq:testq:queued"
    assert rq.namespace("incomplete") == "saq:testq:incomplete"
    assert rq.namespace("active")     == "saq:testq:active"
    assert rq.namespace("dlq")        == "saq:testq:dlq"
    assert rq.job_id("abc")           == "saq:job:testq:abc"
    assert rq.job_key_from_id("saq:job:testq:abc") == "abc"

check("RedisQueue key layout", test_redis_queue_keys)

# ── Worker accepts semaphore kwarg ─────────────────────────────────────────────
def test_worker_semaphore_param():
    import inspect
    from saq.worker import Worker
    sig = inspect.signature(Worker.__init__)
    assert "semaphore" in sig.parameters, "semaphore param missing from Worker.__init__"
    param = sig.parameters["semaphore"]
    # default must be None
    assert param.default is None, f"default should be None, got {param.default!r}"

check("Worker semaphore param", test_worker_semaphore_param)

# ── SettingsDict has semaphore field ──────────────────────────────────────────
def test_settings_dict_semaphore():
    from saq.types import SettingsDict
    # Use __annotations__ directly to avoid get_type_hints() chasing
    # forward-refs through circular-import-guarded TYPE_CHECKING blocks.
    all_annotations: dict = {}
    for klass in getattr(SettingsDict, "__mro__", [SettingsDict]):
        all_annotations.update(getattr(klass, "__annotations__", {}))
    # TypedDict stores its own annotations under __annotations__
    own = SettingsDict.__annotations__
    assert "semaphore" in own, (
        f"'semaphore' not in SettingsDict.__annotations__: {list(own)}"
    )

check("SettingsDict.semaphore field", test_settings_dict_semaphore)

# ── __all__ exports ────────────────────────────────────────────────────────────
def test_all_exports():
    import saq
    for name in (
        "Priority",
        "DistributedSemaphore",
        "compute_priority_score",
        "decode_priority_score",
        "Job",
        "Status",
        "Worker",
        "Queue",
        "CronJob",
    ):
        assert name in saq.__all__, f"{name!r} missing from saq.__all__"
        assert hasattr(saq, name), f"{name!r} not importable from saq"

check("__all__ exports", test_all_exports)

# ── Final summary ──────────────────────────────────────────────────────────────
print()
if errors:
    print(f"FAILED: {len(errors)} check(s) failed: {errors}")
    sys.exit(1)
else:
    print("All checks passed.")
