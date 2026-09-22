"""Minimal PyTorch input adapter backed by ShardPack's sequential Rust reader."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from os import PathLike
from typing import Any

try:
    import torch.distributed as dist
    from torch.utils.data import IterableDataset, get_worker_info
except ImportError as error:  # pragma: no cover - depends on the host training environment
    raise ImportError(
        "shardpack.pytorch requires PyTorch; install torch in the training environment"
    ) from error

from . import Dataset, EpochScheduler, Record


class ShardPackDataset(IterableDataset[Any]):
    """Shard-partitioned encoded records for ``torch.utils.data.DataLoader``.

    ``decode`` runs in the DataLoader worker and should return a value accepted by
    the chosen PyTorch collation function. Without it, use ``batch_size=None`` or a
    custom ``collate_fn`` because :class:`Record` is intentionally schema-opaque.
    """

    def __init__(
        self,
        config: str | PathLike[str],
        layout: str,
        *,
        store: str | None = None,
        decode: Callable[[Record], Any] | None = None,
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
    ) -> None:
        super().__init__()
        if (rank is None) != (world_size is None):
            raise ValueError("rank and world_size must be provided together")
        self.config = os.fspath(config)
        self.layout = layout
        self.store = store
        self.decode = decode
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
        self._epoch_scheduler: EpochScheduler | None = None
        self.encryption_key_id = encryption_key_id
        self.encryption_key = encryption_key

    def set_epoch(self, epoch: int) -> None:
        """Reset the next epoch before workers are created or when they are recreated."""
        self.epoch = epoch
        if self._epoch_scheduler is not None:
            self._epoch_scheduler.set_epoch(epoch)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        # Native epoch state is deliberately constructed after spawn. A worker
        # must never inherit another process's cursor.
        state["_epoch_scheduler"] = None
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)

    def _distributed_context(self) -> tuple[int, int]:
        if self.rank is not None:
            assert self.world_size is not None
            return self.rank, self.world_size
        if dist.is_available() and dist.is_initialized():
            return dist.get_rank(), dist.get_world_size()
        return int(os.environ.get("RANK", "0")), int(os.environ.get("WORLD_SIZE", "1"))

    def __iter__(self) -> Iterator[Any]:
        worker = get_worker_info()
        worker_id = 0 if worker is None else worker.id
        workers_per_rank = 1 if worker is None else worker.num_workers
        rank, world_size = self._distributed_context()

        if self._epoch_scheduler is None:
            # This executes after worker creation. Each persistent worker owns a
            # Rust scheduler and advances it once per DataLoader iteration.
            self._epoch_scheduler = EpochScheduler(
                self.epoch,
                self.shard_seed,
                self.shuffle_seed,
            )
        epoch, shard_seed, shuffle_seed = self._epoch_scheduler.next()
        if not self.auto_advance_epoch:
            self._epoch_scheduler.set_epoch(epoch)

        # Construct all OpenDAL and Tokio state after DataLoader worker creation;
        # native handles must never cross a fork boundary.
        dataset = Dataset(self.config, self.layout, self.store)
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
            worker=worker_id,
            workers_per_rank=workers_per_rank,
            encryption_key_id=self.encryption_key_id,
            encryption_key=self.encryption_key,
        )
        if self.decode is None:
            yield from records
        else:
            for record in records:
                yield self.decode(record)
