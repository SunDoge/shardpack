from collections.abc import Iterator
from os import PathLike

class EpochScheduler:
    def __init__(
        self,
        initial_epoch: int = 0,
        shard_seed: int = 0,
        shuffle_seed: int = 1,
    ) -> None: ...
    def next(self) -> tuple[int, int, int]: ...
    def set_epoch(self, epoch: int) -> None: ...

class Record:
    @property
    def sample_id(self) -> bytes: ...
    @property
    def flags(self) -> int: ...
    @property
    def payload(self) -> memoryview: ...
    def __len__(self) -> int: ...

class RecordLoader(Iterator[Record]):
    def __iter__(self) -> RecordLoader: ...
    def __next__(self) -> Record: ...
    def metrics(self) -> dict[str, int]: ...
    def metrics_handle(self) -> LoaderMetrics: ...

class LoaderMetrics:
    def snapshot(self) -> dict[str, int]: ...

class SamplePool:
    def __init__(self, *, max_records: int, max_bytes: int, seed: int = 0) -> None: ...
    def insert(self, record: Record) -> None: ...
    def sample(
        self, count: int, *, exclude_ids: list[bytes] | None = None, replace: bool = False
    ) -> list[Record]: ...
    def available(self, *, exclude_ids: list[bytes] | None = None) -> int: ...
    def reset(self, *, seed: int = 0) -> None: ...
    def __len__(self) -> int: ...
    @property
    def buffered_bytes(self) -> int: ...

class Dataset:
    def __init__(
        self,
        layout: str,
        *,
        config: str | None = None,
        config_path: str | PathLike[str] | None = None,
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
        cache_mode: str = "ciphertext",
        network_concurrency: int = 2,
        network_prefetch_bytes: int = 67_108_864,
        shuffle_max_records: int = 0,
        shuffle_max_bytes: int = 0,
        shuffle_seed: int = 1,
        epoch: int = 0,
        shard_seed: int = 0,
        rank: int = 0,
        world_size: int = 1,
        worker: int = 0,
        workers_per_rank: int = 1,
        encryption_key_id: str | None = None,
        encryption_key: bytes | None = None,
        encryption_key_path: str | PathLike[str] | None = None,
    ) -> RecordLoader: ...

class DatasetWriter:
    def __init__(
        self,
        snapshot_id: bytes,
        staging_dir: str | PathLike[str],
        *,
        config: str | None = None,
        config_path: str | PathLike[str] | None = None,
        store: str | None = None,
        target_shard_bytes: int = 1_073_741_824,
        max_shard_bytes: int | None = None,
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
        encryption_key_path: str | PathLike[str] | None = None,
    ) -> None: ...
    def write_batch(
        self,
        payload_data: object,
        offsets: object,
        sample_ids: object,
        flags: object | None = None,
    ) -> int: ...
    def finish(self) -> str: ...

def cli_main(args: list[str]) -> int: ...

__version__: str
