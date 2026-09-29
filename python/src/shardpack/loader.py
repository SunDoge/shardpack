"""Framework-neutral bounded threaded loading pipeline."""

from __future__ import annotations

import os
import queue
import threading
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from os import PathLike
from time import perf_counter
from typing import Any

from ._native import EpochScheduler
from ._stats import LoaderStats, measured
from .api import Config, Dataset
from .sampling import SamplePoolSource


@dataclass(frozen=True)
class _Batch:
    value: Any


@dataclass(frozen=True)
class _Failure:
    error: BaseException


class _End:
    pass


_END = _End()


def _identity(value: Any) -> Any:
    return value


def _keep_list(samples: list[Any]) -> list[Any]:
    return samples


def _decode_chunk(decode: Callable[[Any], Any], records: list[Any]) -> list[Any]:
    return [decode(record) for record in records]


class ShardPackSource:
    """Framework-neutral, rank-partitioned stream of encoded records."""

    def __init__(
        self,
        layout: str,
        *,
        config: Config | None = None,
        config_path: str | PathLike[str] | None = None,
        store: str | None = None,
        epoch: int = 0,
        shard_seed: int = 0,
        rank: int | None = None,
        world_size: int | None = None,
        cache_dir: str | PathLike[str] | None = None,
        cache_max_bytes: int = 256 * 1024**3,
        cache_mode: str = "ciphertext",
        network_concurrency: int = 2,
        network_prefetch_bytes: int = 64 * 1024**2,
        shuffle_max_records: int = 0,
        shuffle_max_bytes: int = 0,
        shuffle_seed: int = 1,
        auto_advance_epoch: bool = True,
        encryption_key_id: str | None = None,
        encryption_key: bytes | None = None,
        encryption_key_path: str | PathLike[str] | None = None,
    ) -> None:
        if (rank is None) != (world_size is None):
            raise ValueError("rank and world_size must be provided together")
        if (config is None) == (config_path is None):
            raise ValueError("exactly one of config or config_path is required")
        self.config = config
        self.config_path = None if config_path is None else os.fspath(config_path)
        self.layout = layout
        self.store = store
        self.epoch = epoch
        self.shard_seed = shard_seed
        self.rank = rank
        self.world_size = world_size
        self.cache_dir = None if cache_dir is None else os.fspath(cache_dir)
        self.cache_max_bytes = cache_max_bytes
        self.cache_mode = cache_mode
        self.network_concurrency = network_concurrency
        self.network_prefetch_bytes = network_prefetch_bytes
        self.shuffle_max_records = shuffle_max_records
        self.shuffle_max_bytes = shuffle_max_bytes
        self.shuffle_seed = shuffle_seed
        self.auto_advance_epoch = auto_advance_epoch
        self._epoch_scheduler = EpochScheduler(epoch, shard_seed, shuffle_seed)
        self.encryption_key_id = encryption_key_id
        self.encryption_key = encryption_key
        self.encryption_key_path = (
            None if encryption_key_path is None else os.fspath(encryption_key_path)
        )
        self._record_loader: Any = None
        self._last_metrics: dict[str, int] = {}

    def stats(self) -> dict[str, int]:
        """Native counters for the latest started epoch (also retained at EOF)."""
        loader = self._record_loader
        return loader.metrics() if loader is not None else dict(self._last_metrics)

    def set_epoch(self, epoch: int) -> None:
        self.epoch = epoch
        self._epoch_scheduler.set_epoch(epoch)

    def _distributed_context(self) -> tuple[int, int]:
        if self.rank is not None:
            assert self.world_size is not None
            return self.rank, self.world_size
        return int(os.environ.get("RANK", "0")), int(os.environ.get("WORLD_SIZE", "1"))

    def __iter__(self) -> Iterator[Any]:
        rank, world_size = self._distributed_context()
        epoch, shard_seed, shuffle_seed = self._epoch_scheduler.next()
        if not self.auto_advance_epoch:
            self._epoch_scheduler.set_epoch(epoch)
        dataset = Dataset(
            self.layout,
            config=self.config,
            config_path=self.config_path,
            store=self.store,
        )
        records = dataset.records(
            cache_dir=self.cache_dir,
            cache_max_bytes=self.cache_max_bytes,
            cache_mode=self.cache_mode,
            network_concurrency=self.network_concurrency,
            network_prefetch_bytes=self.network_prefetch_bytes,
            shuffle_max_records=self.shuffle_max_records,
            shuffle_max_bytes=self.shuffle_max_bytes,
            shuffle_seed=shuffle_seed,
            epoch=epoch,
            shard_seed=shard_seed,
            rank=rank,
            world_size=world_size,
            worker=0,
            workers_per_rank=1,
            encryption_key_id=self.encryption_key_id,
            encryption_key=self.encryption_key,
            encryption_key_path=self.encryption_key_path,
        )
        self._record_loader = records
        self._last_metrics = {}
        try:
            yield from records
        finally:
            if self._record_loader is records:
                self._last_metrics = records.metrics()
                self._record_loader = None


class ThreadedLoader:
    """Single-process, framework-neutral threaded decode and batch pipeline.

    Decode functions should spend most of their time in native code that releases
    the GIL. The source, in-flight decode work, and completed batch queue are all
    bounded independently.
    """

    def __init__(
        self,
        source: Iterable[Any],
        *,
        decode: Callable[[Any], Any] | None = None,
        batch_size: int = 1,
        decode_workers: int = 4,
        decode_prefetch: int | None = None,
        decode_task_records: int = 16,
        prefetch_batches: int = 2,
        collate_fn: Callable[[list[Any]], Any] | None = None,
        consumer_fn: Callable[[Any], Any] | None = None,
        drop_last: bool = False,
        enable_stats: bool = False,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if decode_workers < 1:
            raise ValueError("decode_workers must be positive")
        if prefetch_batches < 1:
            raise ValueError("prefetch_batches must be positive")
        if decode_prefetch is None:
            decode_prefetch = max(batch_size, decode_workers * 2)
        if decode_prefetch < decode_workers:
            raise ValueError("decode_prefetch must be at least decode_workers")
        if decode_task_records < 1:
            raise ValueError("decode_task_records must be positive")
        self.source = source
        self.decode = _identity if decode is None else decode
        self.batch_size = batch_size
        self.decode_workers = decode_workers
        self.decode_prefetch = decode_prefetch
        self.decode_task_records = decode_task_records
        self.prefetch_batches = prefetch_batches
        self.collate_fn = _keep_list if collate_fn is None else collate_fn
        self.consumer_fn = _identity if consumer_fn is None else consumer_fn
        self.drop_last = drop_last
        self.enable_stats = enable_stats
        self._stats: LoaderStats | None = None

    def stats(self) -> dict[str, Any]:
        """Detached CPU-side snapshot for the latest started iteration."""
        if not self.enable_stats:
            return {"enabled": False}
        if self._stats is None:
            return {"enabled": self.enable_stats, "status": "not_started"}
        result = self._stats.snapshot()
        if isinstance(self.source, (ShardPackSource, SamplePoolSource)):
            result["source"] = self.source.stats()
        return result

    def __iter__(self) -> Iterator[Any]:
        stats = LoaderStats() if self.enable_stats else None
        self._stats = stats
        output: queue.Queue[_Batch | _Failure | _End] = queue.Queue(self.prefetch_batches)
        stop = threading.Event()
        producer = threading.Thread(
            target=self._produce,
            args=(output, stop, stats),
            name="shardpack-threaded-loader",
        )
        producer.start()
        status = "closed"
        try:
            while True:
                delivery_started = perf_counter() if stats is not None else 0.0
                if stats is None:
                    item = output.get()
                else:
                    try:
                        item = output.get_nowait()
                    except queue.Empty:
                        item = measured(stats, "batch_wait", output.get)
                    stats.gauge("batch_queue", output.qsize())
                if isinstance(item, _Batch):
                    value = (
                        self.consumer_fn(item.value)
                        if stats is None
                        else measured(stats, "consumer", self.consumer_fn, item.value)
                    )
                    if stats is not None:
                        stats.observe("batch_delivery", perf_counter() - delivery_started)
                        stats.delivered()
                    yield value
                elif isinstance(item, _Failure):
                    raise item.error
                else:
                    status = "completed"
                    return
        except BaseException as error:
            if not isinstance(error, GeneratorExit):
                status = "failed"
            raise
        finally:
            stop.set()
            producer.join()
            if stats is not None:
                stats.finish(status)

    def _put(
        self,
        output: queue.Queue[_Batch | _Failure | _End],
        item: _Batch | _Failure | _End,
        stop: threading.Event,
        stats: LoaderStats | None = None,
    ) -> bool:
        if stop.is_set():
            return False
        started = None
        if stats is not None:
            try:
                output.put_nowait(item)
                stats.gauge("batch_queue", output.qsize())
                return True
            except queue.Full:
                started = perf_counter()
        while not stop.is_set():
            try:
                output.put(item, timeout=0.1)
                if stats is not None:
                    stats.gauge("batch_queue", output.qsize())
                    assert started is not None
                    stats.observe("batch_backpressure", perf_counter() - started)
                return True
            except queue.Full:
                pass
        if stats is not None and started is not None:
            stats.observe("batch_backpressure", perf_counter() - started)
        return False

    def _produce(
        self,
        output: queue.Queue[_Batch | _Failure | _End],
        stop: threading.Event,
        stats: LoaderStats | None,
    ) -> None:
        pending: deque[Future[list[Any]]] = deque()
        pending_records = 0
        batch: list[Any] = []
        exhausted = False
        executor = ThreadPoolExecutor(
            max_workers=self.decode_workers,
            thread_name_prefix="shardpack-decode",
        )
        try:
            source = iter(self.source)
            while not stop.is_set():
                while not exhausted and pending_records < self.decode_prefetch:
                    records = []
                    capacity = min(
                        self.decode_task_records,
                        self.decode_prefetch - pending_records,
                    )
                    started = perf_counter() if stats is not None else 0.0
                    try:
                        for _ in range(capacity):
                            try:
                                records.append(next(source))
                            except StopIteration:
                                exhausted = True
                                break
                    finally:
                        if stats is not None:
                            stats.observe("source", perf_counter() - started, len(records))
                    if records:
                        if stats is None:
                            future = executor.submit(_decode_chunk, self.decode, records)
                        else:
                            future = executor.submit(
                                measured,
                                stats,
                                "decode",
                                _decode_chunk,
                                self.decode,
                                records,
                                items=len(records),
                            )
                        pending.append(future)
                        pending_records += len(records)
                        if stats is not None:
                            stats.gauge("decode_inflight_records", pending_records)

                if not pending:
                    break
                future = pending.popleft()
                samples = (
                    future.result()
                    if stats is None
                    else measured(stats, "decode_result_wait", future.result)
                )
                pending_records -= len(samples)
                if stats is not None:
                    stats.gauge("decode_inflight_records", pending_records)
                for sample in samples:
                    batch.append(sample)
                    if len(batch) == self.batch_size:
                        value = (
                            self.collate_fn(batch)
                            if stats is None
                            else measured(stats, "collate", self.collate_fn, batch)
                        )
                        if not self._put(output, _Batch(value), stop, stats):
                            return
                        batch = []

            if batch and not self.drop_last and not stop.is_set():
                value = (
                    self.collate_fn(batch)
                    if stats is None
                    else measured(stats, "collate", self.collate_fn, batch)
                )
                self._put(output, _Batch(value), stop, stats)
            if not stop.is_set():
                self._put(output, _END, stop, stats)
        except BaseException as error:  # noqa: BLE001 - includes native binding panics
            self._put(output, _Failure(error), stop, stats)
        finally:
            stop.set()
            for future in pending:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
