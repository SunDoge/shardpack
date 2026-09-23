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
from typing import Any

from ._native import EpochScheduler
from .api import Config, Dataset


@dataclass(frozen=True)
class _Batch:
    value: Any


@dataclass(frozen=True)
class _Failure:
    error: Exception


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
        yield from dataset.records(
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

    def __iter__(self) -> Iterator[Any]:
        output: queue.Queue[_Batch | _Failure | _End] = queue.Queue(self.prefetch_batches)
        stop = threading.Event()
        producer = threading.Thread(
            target=self._produce,
            args=(output, stop),
            name="shardpack-threaded-loader",
        )
        producer.start()
        try:
            while True:
                item = output.get()
                if isinstance(item, _Batch):
                    yield self.consumer_fn(item.value)
                elif isinstance(item, _Failure):
                    raise item.error
                else:
                    return
        finally:
            stop.set()
            producer.join()

    def _put(
        self,
        output: queue.Queue[_Batch | _Failure | _End],
        item: _Batch | _Failure | _End,
        stop: threading.Event,
    ) -> bool:
        while not stop.is_set():
            try:
                output.put(item, timeout=0.1)
                return True
            except queue.Full:
                pass
        return False

    def _produce(
        self,
        output: queue.Queue[_Batch | _Failure | _End],
        stop: threading.Event,
    ) -> None:
        pending: deque[Future[list[Any]]] = deque()
        pending_records = 0
        batch: list[Any] = []
        source = iter(self.source)
        exhausted = False
        executor = ThreadPoolExecutor(
            max_workers=self.decode_workers,
            thread_name_prefix="shardpack-decode",
        )
        try:
            while not stop.is_set():
                while not exhausted and pending_records < self.decode_prefetch:
                    records = []
                    capacity = min(
                        self.decode_task_records,
                        self.decode_prefetch - pending_records,
                    )
                    for _ in range(capacity):
                        try:
                            records.append(next(source))
                        except StopIteration:
                            exhausted = True
                            break
                    if records:
                        pending.append(executor.submit(_decode_chunk, self.decode, records))
                        pending_records += len(records)

                if not pending:
                    break
                samples = pending.popleft().result()
                pending_records -= len(samples)
                for sample in samples:
                    batch.append(sample)
                    if len(batch) == self.batch_size:
                        if not self._put(output, _Batch(self.collate_fn(batch)), stop):
                            return
                        batch = []

            if batch and not self.drop_last and not stop.is_set():
                self._put(output, _Batch(self.collate_fn(batch)), stop)
            if not stop.is_set():
                self._put(output, _END, stop)
        except Exception as error:  # noqa: BLE001 - forward producer failures
            self._put(output, _Failure(error), stop)
        finally:
            stop.set()
            for future in pending:
                future.cancel()
            executor.shutdown(wait=True, cancel_futures=True)
