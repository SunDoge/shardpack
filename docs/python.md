# Python SDK

The Python package is a mixed Python/Rust project managed by `uv` and `maturin`.
It builds one abi3 extension for CPython 3.11 and newer.

```bash
cd python
uv sync
uv run maturin develop
uv run pytest
```

Release artifacts are reproducible through the repository tasks:

```bash
mise run test:python
mise run build:python
```

The wheel uses abi3 for CPython 3.11+ and Zig to retain a manylinux 2.17 baseline.
The maturin source distribution includes the Rust path dependencies and has been
rebuilt and tested in an isolated uv environment.

`DatasetWriter` accepts packed CPU buffers without linking against the NumPy C
ABI. One batch consists of a contiguous `uint8` payload arena, `uint64` offsets,
`uint8[B, 16]` sample IDs, and optional `uint32` flags:

```python
import numpy as np
from shardpack import DatasetWriter

payloads = [b"first encoded record", b"second encoded record"]
data = np.frombuffer(b"".join(payloads), dtype=np.uint8)
offsets = np.array([0, len(payloads[0]), sum(map(len, payloads))], dtype=np.uint64)
sample_ids = np.zeros((2, 16), dtype=np.uint8)

writer = DatasetWriter(
    config="~/.config/shardpack/config.toml",
    snapshot_id=bytes.fromhex("00" * 32),
    staging_dir="/local-nvme/shardpack-staging",
)
writer.write_batch(data, offsets, sample_ids)
layout_key = writer.finish()
```

The native call copies the arena before returning, so callers may immediately
reuse writable NumPy buffers. Admission is bounded by batch count, records, and
bytes; waiting for channel capacity releases the GIL.

`Dataset` loads the authoritative protobuf layout through a named store in the
normal ShardPack TOML configuration. Shards referenced by that layout may reside
in any of the configured stores.

```python
from shardpack import Dataset

dataset = Dataset(
    config="~/.config/shardpack/config.toml",
    layout="layouts/ab/cd/<layout-id>.layout.pb",
    store="training",
)

loader = dataset.records(
    cache_dir="/local-nvme/shardpack",
    cache_max_bytes=2 * 1024**4,
    network_concurrency=4,
    network_prefetch_bytes=256 * 1024**2,
    shuffle_max_records=4096,
    shuffle_max_bytes=256 * 1024**2,
    shuffle_seed=29,
    epoch=3,
    shard_seed=17,
    rank=0,
    world_size=8,
)

for record in loader:
    sample_id = record.sample_id
    encoded_payload = record.payload
```

The Python iterator is synchronous for straightforward trainer integration. Each
blocking call releases the GIL while a private Tokio runtime performs asynchronous
OpenDAL reads. Rust tasks concurrently prefetch compressed shard bytes, and a
record/byte-bounded pool shuffles encoded records before Python allocation. Network
and shuffle memory are independently bounded. Set both shuffle limits to zero to
disable record shuffle. `Dataset.batches()` pulls a whole encoded batch during one
Rust runtime drive, avoiding one Tokio/GIL scheduling round trip per record. It
also handles final batches; schema-specific decode and tensor transforms remain
application code.

Encrypted layouts accept the external key explicitly. The manifest contains only
the key ID:

```python
loader = dataset.records(
    encryption_key_id="kms://training/key-7",
    encryption_key=key_bytes,  # exactly 32 bytes
)
```

## PyTorch worker integration

Construct `Dataset` and its loader inside each worker process. This avoids sharing
S3 clients, Tokio runtimes, or open cache descriptors across `fork` boundaries.

```python
import os
from torch.utils.data import IterableDataset, get_worker_info
from shardpack import Dataset


class TrainingRecords(IterableDataset):
    def __init__(self, config: str, layout: str, cache: str, epoch: int):
        self.config = config
        self.layout = layout
        self.cache = cache
        self.epoch = epoch

    def __iter__(self):
        worker = get_worker_info()
        worker_id = 0 if worker is None else worker.id
        workers = 1 if worker is None else worker.num_workers
        dataset = Dataset(self.config, self.layout)
        return dataset.records(
            cache_dir=self.cache,
            epoch=self.epoch,
            shard_seed=0x5A17,
            rank=int(os.environ.get("RANK", "0")),
            world_size=int(os.environ.get("WORLD_SIZE", "1")),
            worker=worker_id,
            workers_per_rank=workers,
        )
```

`RecordLoader` and `BatchLoader` are intentionally thread-affine. Multiple Python
threads or training workers should create independent loaders. Multiple processes
may safely share one local cache directory; cache publication and eviction are
cross-process coordinated.

The loader exposes lock-free counters using `loader.metrics()`, including records,
encoded bytes, completed shards, exact-identity-verified shards, prefetched shards,
retries, checksum failures, queue backpressure, cache hits/misses, downloaded bytes,
corruption, stale partial cleanup, and eviction. A suffix-only Range GET resumed
without cache increments `shards_completed` but not `shards_verified`, because its
unread prefix cannot be authenticated.

## Exact resume

A loader checkpoint includes the block-boundary reader position, distributed
assignment, every buffered encoded record in the bounded shuffle pool, and its RNG
state. It can therefore be persisted without changing the future sample order:

```python
checkpoint_bytes = loader.checkpoint().to_bytes()

# Store checkpoint_bytes in the trainer checkpoint.
checkpoint = shardpack.LoaderCheckpoint.from_bytes(checkpoint_bytes)
loader = dataset.resume_records(
    checkpoint,
    cache_dir="/local-nvme/shardpack",
)
```

Encrypted layouts require an encryption key when serializing the checkpoint so
buffered record payloads can never be persisted in plaintext. This key can be the
trainer-checkpoint key and need not equal the dataset key:

```python
checkpoint_bytes = loader.checkpoint().to_bytes(checkpoint_key)
checkpoint = shardpack.LoaderCheckpoint.from_bytes(checkpoint_bytes, checkpoint_key)
```

The dataset key is still required again by `resume_records`. Because buffered
encoded records are part of the checkpoint, its serialized size is bounded by
`shuffle_max_bytes` but may be substantial. Persist it at the same safe point as
the model and optimizer state.
