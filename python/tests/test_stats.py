import queue
import threading

import pytest

from shardpack import ThreadedLoader
from shardpack._stats import LoaderStats


def test_stats_are_optional_and_reset_for_each_iteration():
    plain = ThreadedLoader(range(7), batch_size=3)
    assert list(plain) == [[0, 1, 2], [3, 4, 5], [6]]
    assert plain.stats() == {"enabled": False}
    loader = ThreadedLoader(range(7), batch_size=3, enable_stats=True)
    assert loader.stats()["status"] == "not_started"
    for _ in range(2):
        assert list(loader) == [[0, 1, 2], [3, 4, 5], [6]]
        stats = loader.stats()
        assert stats["status"] == "completed"
        assert stats["batches_delivered"] == 3
        assert stats["timings"]["source"]["items"] == 7
        assert stats["timings"]["decode"]["items"] == 7
        assert stats["timings"]["collate"]["calls"] == 3
        assert stats["timings"]["consumer"]["calls"] == 3
        assert stats["timings"]["batch_delivery"]["calls"] == 3
        assert stats["queues"]["batch_queue"]["peak"] <= 2
        assert stats["queues"]["decode_inflight_records"]["peak"] <= loader.decode_prefetch
        assert all(q["current"] == 0 for q in stats["queues"].values())


@pytest.mark.parametrize("stage", ["decode", "collate_fn", "consumer_fn"])
def test_stats_preserve_failures(stage):
    def fail(_):
        raise ValueError("expected failure")

    loader = ThreadedLoader(range(8), enable_stats=True, **{stage: fail})
    with pytest.raises(ValueError, match="expected failure"):
        list(loader)
    assert loader.stats()["status"] == "failed"


def test_stats_preserve_early_close():
    loader = ThreadedLoader(range(1000), enable_stats=True, prefetch_batches=1)
    iterator = iter(loader)
    next(iterator)
    assert loader.stats()["status"] == "running"
    iterator.close()
    assert loader.stats()["status"] == "closed"
    assert loader.stats()["batches_delivered"] == 1


def test_stats_snapshot_is_bounded_detached_and_thread_safe():
    stats = LoaderStats()

    def record():
        for _ in range(1000):
            stats.observe("decode", 0.01, 4)

    threads = [threading.Thread(target=record) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    metric = stats.snapshot()["timings"]["decode"]
    assert metric["calls"] == 4000
    assert metric["items"] == 16000
    assert metric["recent_samples"] == 256
    assert metric["p95_seconds"] == 0.01
    metric["calls"] = 0
    assert stats.snapshot()["timings"]["decode"]["calls"] == 4000


def test_empty_source_stats():
    loader = ThreadedLoader([], enable_stats=True)
    assert list(loader) == []
    assert loader.stats()["status"] == "completed"
    assert loader.stats()["batches_delivered"] == 0


def test_disabled_stats_do_not_read_clocks(monkeypatch):
    def fail():
        raise AssertionError("disabled instrumentation used a clock")

    monkeypatch.setattr("shardpack.loader.perf_counter", fail)
    monkeypatch.setattr("shardpack._stats.perf_counter", fail)
    assert list(ThreadedLoader(range(3))) == [[0], [1], [2]]


def test_source_iteration_failure_is_forwarded():
    class Broken:
        def __iter__(self):
            raise ValueError("cannot open source")

    loader = ThreadedLoader(Broken(), enable_stats=True)
    with pytest.raises(ValueError, match="cannot open source"):
        list(loader)
    assert loader.stats()["status"] == "failed"


def test_source_read_failure_preserves_timing():
    def broken():
        yield 1
        raise ValueError("source read failed")

    loader = ThreadedLoader(broken(), enable_stats=True)
    with pytest.raises(ValueError, match="source read failed"):
        list(loader)
    assert loader.stats()["timings"]["source"]["items"] == 1
    assert loader.stats()["status"] == "failed"


def test_non_exception_worker_failure_is_forwarded():
    class NativeFailure(BaseException):
        pass

    def decode(_):
        raise NativeFailure("binding failed")

    loader = ThreadedLoader(range(3), decode=decode, enable_stats=True)
    with pytest.raises(NativeFailure, match="binding failed"):
        list(loader)
    assert loader.stats()["status"] == "failed"


def test_backpressure_is_recorded():
    entered = threading.Event()

    class ObservedQueue(queue.Queue):
        def put(self, item, block=True, timeout=None):
            if block:
                entered.set()
            return super().put(item, block, timeout)

    output = ObservedQueue(maxsize=1)
    output.put_nowait(1)
    stats = LoaderStats()
    stop = threading.Event()
    loader = ThreadedLoader([])
    worker = threading.Thread(target=loader._put, args=(output, 2, stop, stats))
    worker.start()
    try:
        assert entered.wait(5)
        assert output.get() == 1
        worker.join(5)
        assert not worker.is_alive()
        assert stats.snapshot()["timings"]["batch_backpressure"]["calls"] == 1
    finally:
        stop.set()
        worker.join(5)
