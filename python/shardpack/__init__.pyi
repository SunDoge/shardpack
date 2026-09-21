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

class RecordLoader(Iterator[Record]):
    def __iter__(self) -> RecordLoader: ...
    def __next__(self) -> Record: ...

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
