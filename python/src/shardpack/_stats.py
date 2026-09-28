"""Bounded, opt-in CPU-side loader instrumentation (no GPU synchronization)."""

from collections import deque
from threading import Lock
from time import perf_counter
from typing import Any


class LoaderStats:
    def __init__(self) -> None:
        self._lock = Lock()
        self._started = perf_counter()
        self._ended: float | None = None
        self._status = "running"
        self._timings: dict[str, dict[str, Any]] = {}
        self._gauges: dict[str, dict[str, int]] = {}
        self._batches = 0

    def observe(self, name: str, seconds: float, items: int = 1) -> None:
        with self._lock:
            metric = self._timings.get(name)
            if metric is None:
                metric = {"calls": 0, "items": 0, "total_seconds": 0.0, "recent": deque(maxlen=256)}
                self._timings[name] = metric
            metric["calls"] += 1
            metric["items"] += items
            metric["total_seconds"] += seconds
            metric["recent"].append(seconds)

    def gauge(self, name: str, value: int) -> None:
        with self._lock:
            metric = self._gauges.get(name)
            if metric is None:
                metric = {"current": 0, "peak": 0}
                self._gauges[name] = metric
            metric["current"] = value
            metric["peak"] = max(metric["peak"], value)

    def delivered(self) -> None:
        with self._lock:
            self._batches += 1

    def finish(self, status: str) -> None:
        with self._lock:
            self._status = status
            self._ended = perf_counter()
            for metric in self._gauges.values():
                metric["current"] = 0

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            timings = {}
            for name, metric in self._timings.items():
                recent = sorted(metric["recent"])
                timings[name] = {
                    "calls": metric["calls"],
                    "items": metric["items"],
                    "total_seconds": metric["total_seconds"],
                    "mean_seconds": metric["total_seconds"] / metric["calls"],
                    "recent_samples": len(recent),
                    "p50_seconds": recent[(len(recent) - 1) // 2],
                    "p95_seconds": recent[(95 * len(recent) + 99) // 100 - 1],
                }
            return {
                "enabled": True,
                "status": self._status,
                "elapsed_seconds": (self._ended or perf_counter()) - self._started,
                "batches_delivered": self._batches,
                "timings": timings,
                "queues": {name: dict(value) for name, value in self._gauges.items()},
            }


def measured(stats: LoaderStats | None, name: str, function, *args, items: int = 1):
    if stats is None:
        return function(*args)
    started = perf_counter()
    try:
        return function(*args)
    finally:
        stats.observe(name, perf_counter() - started, items)
