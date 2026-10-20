# Python SDK

See [runtime refactor and migration notes](runtime-refactor.md) for trusted
dataset IDs, resolve-once sources, cancellation, `NodeAffinity`, and distributed
epoch validation.

For Mosaic/MixUp companion selection, see [SamplePool and SamplePoolSource](sample-pool.md).
They reuse a bounded history of encoded records without changing primary coverage
or fetching additional remote objects.

For training bottleneck diagnostics, enable `ThreadedLoader(..., enable_stats=True)`
and query `loader.stats()`. See [loader observability](loader-observability.md)
for stage timings, queue waits, native source counters, and measurement limits.

The Python package is the offline ingestion and upload interface for ShardPack.
It is a mixed Python/Rust project managed by `uv` and `maturin`, and builds one
abi3 extension for CPython 3.11 and newer.

Python sources use the `python/src/shardpack` layout. The PyO3 extension remains
in `crates/shardpack-python`; maturin installs it as `shardpack._native`. Run tests
against the installed package after `uv sync` / `maturin develop`.

The wheel includes `py.typed`, public API stubs, and `shardpack/_native.pyi` for
direct extension imports. Native `config` parameters accept JSON strings; the
public Python facade accepts dictionaries and Pydantic models. The native stub is
maintained alongside PyO3 signatures; tests compare exposed signatures and mypy
checks record, loader, and facade type inference.

```bash
cd python
uv sync
uv run maturin develop
uv run pytest
```

PyTorch/Fory/OpenCV are isolated in an optional uv dependency group and are not
dependencies of the ShardPack wheel:

```bash
uv sync --group torch
uv run pytest tests/test_pytorch_integration.py
```

Release artifacts use Zig to retain a manylinux 2.17 baseline:

```bash
mise run test:python
mise run build:python
```

Every storage-facing API accepts exactly one of `config` or `config_path`.
Inline mappings are convenient for tests and accept `Path` values directly:

```python
config = {
    "version": 1,
    "default_store": "local",
    "stores": {
        "local": {"type": "filesystem", "root": tmp_path / "objects"},
    },
}
```

The mapping is parsed and validated by the same Rust configuration layer as a
TOML file, including unknown-field rejection and `SHARDPACK__...` overrides.

## Writing datasets

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
    snapshot_id=bytes.fromhex("00" * 32),
    staging_dir="/local-nvme/shardpack-staging",
    config_path="~/.config/shardpack/config.toml",
    store="public-datasets",
    encryption_key_id="kms://training/key-7",
    encryption_key_path="/dev/shm/shardpack-dataset.key",
)
writer.write_batch(data, offsets, sample_ids)
layout_key = writer.finish()
```

The native call copies the arena before returning, so the caller may immediately
reuse writable NumPy buffers. A bounded channel connects Python producers to one
Rust materialization worker. Waiting for channel capacity releases the GIL.
Record and byte limits reject unexpectedly large batches before admission.

The writer is safe to call from multiple Python threads: admission is serialized
and each accepted batch remains intact. It is process-affine and rejects use after
`fork`; each child process must create its own writer. Multiple processes should
write different snapshots or object prefixes rather than sharing one writer.

The Rust worker performs block construction, CRC32C, optional Zstd compression,
optional authenticated encryption, immutable object publication, and layout
manifest publication. OpenDAL supplies the same storage path for local filesystem
tests and S3-compatible production stores.

## Simple validation reader

Python intentionally exposes only a plain sequential reader. It exists for smoke
tests, dataset inspection, and writer round-trip validation; it is not the
production training loader.

```python
from shardpack import Dataset

dataset = Dataset(
    layout=layout_key,
    config_path="~/.config/shardpack/config.toml",
    store="public-datasets",
)

for record in dataset.records(
    cache_dir="/local-nvme/shardpack",
    cache_max_bytes=2 * 1024**4,
    cache_mode="plaintext",
    network_concurrency=4,
    network_prefetch_bytes=512 * 1024**2,
    encryption_key_id="kms://training/key-7",
    encryption_key_path="/dev/shm/shardpack-dataset.key",
):
    assert len(record.sample_id) == 16
    consume(record.flags, record.payload)
```

`record.payload` is a read-only, C-contiguous `memoryview` over the Rust `Bytes`
allocation. Creating it allocates only the small Python view object; it does not
allocate or copy the payload bytes. The view retains
its owning `Record`, so it remains valid even if the iterator advances or the
original Python variable is dropped. Use `bytes(record.payload)` only when a
consumer explicitly requires an owned Python `bytes`, because that conversion
copies.

Read-only decoders can consume the memoryview directly. Be careful with
`torch.frombuffer`: PyTorch warns that it does not enforce a source buffer's
read-only flag and exposes a writable tensor. Never mutate such a tensor. If the
payload represents raw tensor storage rather than an encoded asset, immediately
clone it before handing it to code that may write:

```python
tensor = torch.frombuffer(record.payload, dtype=torch.uint8).clone()
```

For JPEG, audio, protobuf, and similar encoded records, prefer decoders that
accept the buffer protocol so the encoded bytes stay zero-copy until decode.
This describes the Python handoff: the optional Rust shuffle pool first compacts
retained payloads once, so small records cannot pin whole source blocks. The
returned memoryview adds no further payload copy. Live views prevent reader
buffer reuse and remain valid after advancing or dropping the loader.

Apache Fory 1.7 accepts the memoryview through its zero-copy `Buffer` wrapper;
`Fory.deserialize()` itself currently accepts `bytes` or `Buffer`, not a raw
memoryview:

```python
import pyfory

fory = pyfory.Fory()


def decode(record):
    buffer = pyfory.Buffer(record.payload)
    assert not buffer.own_data()  # references the Rust-backed memoryview
    return fory.deserialize(buffer)
```

The opt-in `mise run test:python-torch` suite covers the complete path: Fory
structured objects → packed buffer writer → ShardPack shards → cache/prefetch →
zero-copy memoryview/`pyfory.Buffer` → threaded decode → PyTorch collation.

The iterator reads shards in deterministic shard order, validates the normal
ShardPack checksums and identities, optionally decrypts records, and releases the
GIL while waiting for asynchronous OpenDAL I/O. It supports shard-level
rank/worker partitioning, a verified local cache, and bounded concurrent shard
prefetch. It deliberately provides no record shuffle, batching, or checkpoint
surface. `loader.metrics()` reports cache hits/misses, downloaded bytes,
prefetched and verified shards, retries, and checksum failures.

The default `cache_mode="ciphertext"` stores exact remote shard bytes. For an
encrypted layout, `cache_mode="plaintext"` authenticates and decrypts each block
while the first epoch consumes it, atomically publishes a valid unencrypted but
still-compressed local shard after full remote identity verification, and lets
later epochs skip both the network and AEAD work. Plaintext cache entries are
bound to a one-way commitment of the supplied key. Use plaintext mode only when
the node-local disk is inside the trusted boundary.

`network_prefetch_bytes` is a chunk-buffer budget shared by at most
`network_concurrency` in-flight shards, including the currently consumed shard.
It bounds buffered stored-stream bytes (decrypted for plaintext cache), not
decoded tensors, HTTP buffers or cache block allocations. Cache
population and training consume the same stream, so a
cold epoch does not wait for a complete shard download before yielding records.
These limits apply to each `RecordLoader`. The framework-neutral source creates
one reader per training rank, so framework worker processes do not multiply the
network and cache budgets.

Call `source.cache_plan()` before training for a read-only whole-layout cache
budget/free-space estimate. See [capacity planning and runtime counters](loader-observability.md).

Production training should integrate the Rust crates directly—especially
`shardpack-runtime`, `shardpack-cache`, `shardpack-storage`, and
`shardpack-format`—inside the Rust dataloader that feeds the Python model. This
keeps all hot-path scheduling, decode, transforms, batching, cache affinity, and
distributed assignment outside Python while leaving Python free for Lance
filtering and offline dataset construction.

## Framework-neutral threaded loader

The core threaded loader has no PyTorch, NumPy, CuPy, or JAX dependency. For
native decoders and transforms that release the GIL, it provides bounded decode,
ordered collation, and batch prefetch:

```python
from shardpack import ShardPackSource, ThreadedLoader

source = ShardPackSource(
    layout="layouts/<layout-id>.layout.pb",
    config_path="shardpack.toml",
    rank=rank,
    world_size=world_size,
    cache_dir="/local-nvme/shardpack",
    shuffle_max_records=4096,
    shuffle_max_bytes=256 * 1024**2,
)

loader = ThreadedLoader(
    source,
    decode=decode,
    batch_size=64,
    decode_workers=4,
    collate_fn=collate,
    # Runs in the thread calling next(loader), suitable for device transfer.
    consumer_fn=move_to_device,
)
```

With CuPy, keep decode and collation on CPU and create GPU arrays in the consumer
thread:

```python
loader = ThreadedLoader(
    source,
    decode=decode_numpy,
    batch_size=64,
    collate_fn=np.stack,
    consumer_fn=cp.asarray,
)
```

The optional PyTorch adapter supplies its default collation and pin-memory
behavior:

```python
from shardpack import ShardPackSource
from shardpack.pytorch import ThreadedDataLoader

source = ShardPackSource(
    layout="layouts/<layout-id>.layout.pb",
    config_path="shardpack.toml",
    cache_dir="/local-nvme/shardpack",
    shuffle_max_records=4096,
    shuffle_max_bytes=256 * 1024**2,
)
loader = ThreadedDataLoader(
    source,
    decode=decode_with_opencv,
    batch_size=64,
    decode_workers=4,
    decode_task_records=8,
    decode_prefetch=128,
    prefetch_batches=2,
    pin_memory=True,
)
```

For asynchronous CUDA transfer, `pin_memory=True` recursively pins the final
collated batch in the producer thread before it enters the bounded batch queue.
This includes the last partial batch. `consumer_fn` therefore receives pinned
tensors on the training thread and can use its CUDA stream:

```python
transfer_stream = torch.cuda.Stream()

def move_to_cuda(batch):
    assert batch.is_pinned()
    with torch.cuda.stream(transfer_stream):
        return batch.to("cuda", non_blocking=True)
```

The Rust source remains the single owner of sequential I/O, cache, epoch state,
and encoded-record shuffle. A fixed Python thread pool decodes small ordered
chunks, the coordinator collates complete batches, and a bounded queue passes
only batches to the training thread. There is no multiprocessing queue or
shared-memory sample transfer.

OpenCV and Torch may create native CPU pools of their own. When they run below
this loader, cap those inner pools (for example `cv2.setNumThreads(1)` and an
appropriate `torch.set_num_threads(...)`) to avoid multiplying native threads by
`decode_workers`. The loader does not mutate process-global library settings.

An experimental [Ultralytics streaming adapter](ultralytics.md) demonstrates real
YOLO training and validation without a map-style dataset or unpacking images.
