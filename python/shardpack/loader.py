"""Framework-neutral bounded threaded loading pipeline."""

from __future__ import annotations

import queue
import threading
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any


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
