from collections.abc import Iterator
from os import PathLike

class Record:
    @property
    def sample_id(self) -> bytes: ...
    @property
    def flags(self) -> int: ...
    @property
    def payload(self) -> bytes: ...
    def __len__(self) -> int: ...

class LoaderCheckpoint:
    @staticmethod
    def from_bytes(value: bytes, encryption_key: bytes | None = None) -> LoaderCheckpoint: ...
    def to_bytes(self, encryption_key: bytes | None = None) -> bytes: ...
    def __len__(self) -> int: ...

class RecordLoader(Iterator[Record]):
    def __iter__(self) -> RecordLoader: ...
    def __next__(self) -> Record: ...
    def metrics(self) -> dict[str, int]: ...
    def checkpoint(self) -> LoaderCheckpoint: ...

class BatchLoader(Iterator[list[Record]]):
    def __iter__(self) -> BatchLoader: ...
    def __next__(self) -> list[Record]: ...
    def metrics(self) -> dict[str, int]: ...
    def checkpoint(self) -> LoaderCheckpoint: ...

class Dataset:
    def __init__(
        self,
        config: str | PathLike[str],
        layout: str,
        store: str | None = None,
    ) -> None: ...
    @property
    def layout_id(self) -> bytes: ...
    @property
    def record_count(self) -> int: ...
    @property
    def shard_count(self) -> int: ...
    @property
    def stored_bytes(self) -> int: ...
    @property
    def layout_key(self) -> str: ...
    def records(
        self,
        *,
        cache_dir: str | PathLike[str] | None = None,
        cache_max_bytes: int = 274_877_906_944,
        network_concurrency: int = 2,
        network_prefetch_bytes: int = 67_108_864,
        shuffle_max_records: int = 4096,
        shuffle_max_bytes: int = 268_435_456,
        shuffle_seed: int = 1,
        epoch: int = 0,
        shard_seed: int = 0,
        rank: int = 0,
        world_size: int = 1,
        worker: int = 0,
        workers_per_rank: int = 1,
        encryption_key_id: str | None = None,
        encryption_key: bytes | None = None,
    ) -> RecordLoader: ...
    def batches(
        self,
        batch_size: int,
        *,
        drop_last: bool = False,
        cache_dir: str | PathLike[str] | None = None,
        cache_max_bytes: int = 274_877_906_944,
        network_concurrency: int = 2,
        network_prefetch_bytes: int = 67_108_864,
        shuffle_max_records: int = 4096,
        shuffle_max_bytes: int = 268_435_456,
        shuffle_seed: int = 1,
        epoch: int = 0,
        shard_seed: int = 0,
        rank: int = 0,
        world_size: int = 1,
        worker: int = 0,
        workers_per_rank: int = 1,
        encryption_key_id: str | None = None,
        encryption_key: bytes | None = None,
    ) -> BatchLoader: ...
    def resume_records(
        self,
        checkpoint: LoaderCheckpoint,
        *,
        cache_dir: str | PathLike[str] | None = None,
        cache_max_bytes: int = 274_877_906_944,
        network_concurrency: int = 2,
        network_prefetch_bytes: int = 67_108_864,
        encryption_key_id: str | None = None,
        encryption_key: bytes | None = None,
    ) -> RecordLoader: ...

class DatasetWriter:
    def __init__(
        self,
        config: str | PathLike[str],
        snapshot_id: bytes,
        staging_dir: str | PathLike[str],
        *,
        store: str | None = None,
        target_shard_bytes: int = 1_073_741_824,
        target_block_bytes: int = 16_777_216,
        block_codec: str = "none",
        zstd_level: int = 1,
        object_prefix: str = "shards",
        layout_prefix: str = "layouts",
        queue_batches: int = 2,
        max_batch_records: int = 1_000_000,
        max_batch_bytes: int = 268_435_456,
        encryption_key_id: str | None = None,
        encryption_key: bytes | None = None,
    ) -> None: ...
    def write_batch(
        self,
        payload_data: object,
        offsets: object,
        sample_ids: object,
        flags: object | None = None,
    ) -> int: ...
    def finish(self) -> str: ...

__version__: str
