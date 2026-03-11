"""
Priority
────────
Tri-level job priority used by the Redis sorted-set queue.

Importing from this module gives a stable public symbol that the rest of
SAQ (queue, worker, web UI) can reference without depending on the full
Redis queue implementation.
"""

from __future__ import annotations

import enum


class Priority(int, enum.Enum):
    """
    Tri-level job priority.

    Values are intentionally plain integers so they can be stored
    directly in Redis sorted-set scores and serialised to JSON without
    any conversion step.

    Ordering (highest → lowest):
        HIGH   = 3   user-facing / time-critical work
        NORMAL = 2   default
        LOW    = 1   background / batch work

    Because ``Priority`` is an ``int`` subclass you can use it wherever
    a plain ``int`` is expected::

        score = priority * _WEIGHT - enqueue_ms   # works without .value

    String round-trip::

        Priority["HIGH"]        # → Priority.HIGH
        Priority(2)             # → Priority.NORMAL
        Priority.LOW.name       # → "LOW"
    """

    LOW    = 1   # background / batch
    NORMAL = 2   # default
    HIGH   = 3   # user-facing / time-critical

    # ── helpers ──────────────────────────────────────────────────────

    @classmethod
    def from_value(cls, value: int | str | "Priority") -> "Priority":
        """
        Coerce an int, string name, or Priority instance to Priority.

        Accepts:
            * ``Priority.HIGH``          → returned as-is
            * ``3`` / ``"3"``            → ``Priority.HIGH``
            * ``"HIGH"`` / ``"high"``    → ``Priority.HIGH``

        Raises:
            ValueError if the value cannot be mapped.
        """
        if isinstance(value, cls):
            return value
        try:
            return cls(int(value))
        except (ValueError, KeyError):
            pass
        try:
            return cls[str(value).upper()]
        except KeyError:
            raise ValueError(f"Cannot convert {value!r} to Priority")

    def __str__(self) -> str:  # pragma: no cover
        return self.name


# Convenience aliases so callers can write ``Priority.default()`` in
# contexts where no explicit priority has been supplied.
Priority.default = Priority.NORMAL  # type: ignore[attr-defined]
