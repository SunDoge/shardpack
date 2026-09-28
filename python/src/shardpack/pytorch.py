"""Thin PyTorch batching adapter for the framework-neutral threaded loader."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

try:
    from torch.utils.data import default_collate
except ImportError as error:  # pragma: no cover - depends on the training environment
    raise ImportError(
        "shardpack.pytorch requires PyTorch; install torch in the training environment"
    ) from error

from .loader import ThreadedLoader


class ThreadedDataLoader(ThreadedLoader):
    """PyTorch collation and pin-memory over :class:`ThreadedLoader`."""

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
        pin_memory: bool = False,
        pin_memory_device: str | None = None,
        enable_stats: bool = False,
    ) -> None:
        base_collate = default_collate if collate_fn is None else collate_fn

        def collate(samples: list[Any]) -> Any:
            batch = base_collate(samples)
            if pin_memory:
                from torch.utils.data._utils.pin_memory import pin_memory as torch_pin_memory

                batch = torch_pin_memory(batch, pin_memory_device)
            return batch

        super().__init__(
            source,
            decode=decode,
            batch_size=batch_size,
            decode_workers=decode_workers,
            decode_prefetch=decode_prefetch,
            decode_task_records=decode_task_records,
            prefetch_batches=prefetch_batches,
            collate_fn=collate,
            consumer_fn=consumer_fn,
            drop_last=drop_last,
            enable_stats=enable_stats,
        )
