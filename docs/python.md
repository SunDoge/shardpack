# Python SDK

The Python package is the offline ingestion and upload interface for ShardPack.
It is a mixed Python/Rust project managed by `uv` and `maturin`, and builds one
abi3 extension for CPython 3.11 and newer.

```bash
cd python
uv sync
uv run maturin develop
uv run pytest
```

Release artifacts use Zig to retain a manylinux 2.17 baseline:

```bash
mise run test:python
mise run build:python
```

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
    config="~/.config/shardpack/config.toml",
    snapshot_id=bytes.fromhex("00" * 32),
    staging_dir="/local-nvme/shardpack-staging",
    store="public-datasets",
    encryption_key_id="kms://training/key-7",
    encryption_key=key_bytes,
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
    config="~/.config/shardpack/config.toml",
    layout=layout_key,
    store="public-datasets",
)

for record in dataset.records(
    cache_dir="/local-nvme/shardpack",
    cache_max_bytes=2 * 1024**4,
    cache_mode="plaintext",
    network_concurrency=4,
    network_prefetch_bytes=512 * 1024**2,
    encryption_key_id="kms://training/key-7",
    encryption_key=key_bytes,
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

`network_prefetch_bytes` is a total byte budget shared by at most
`network_concurrency` in-flight shards. It bounds compressed/ciphertext bytes,
not decoded tensors. Cache population and training consume the same stream, so a
cold epoch does not wait for a complete shard download before yielding records.
These limits apply to each `RecordLoader`; with eight DataLoader workers, the
node-level upper bounds are eight times the configured concurrency and byte
budget. Tune them together rather than blindly increasing `num_workers`.

Production training should integrate the Rust crates directly—especially
`shardpack-runtime`, `shardpack-cache`, `shardpack-storage`, and
`shardpack-format`—inside the Rust dataloader that feeds the Python model. This
keeps all hot-path scheduling, decode, transforms, batching, cache affinity, and
distributed assignment outside Python while leaving Python free for Lance
filtering and offline dataset construction.

## Transitional PyTorch data source

Until the dedicated Rust training dataloader is ready, `shardpack.pytorch`
provides a minimal `IterableDataset`. PyTorch remains an optional host dependency
and is not installed by the ShardPack wheel.

```python
from io import BytesIO

import torch
from PIL import Image
from shardpack.pytorch import ShardPackDataset


def decode(record):
    image = Image.open(BytesIO(record.payload)).convert("RGB")
    return transform(image), record.flags


source = ShardPackDataset(
    config="~/.config/shardpack/config.toml",
    layout="layouts/ab/cd/<layout-id>.layout.pb",
    store="training",
    decode=decode,
    shard_seed=0x5A17,
    cache_dir="/local-nvme/shardpack",
    cache_max_bytes=2 * 1024**4,
    cache_mode="plaintext",
    network_concurrency=4,
    network_prefetch_bytes=512 * 1024**2,
    encryption_key_id="kms://training/key-7",
    encryption_key=key_bytes,
)
loader = torch.utils.data.DataLoader(
    source,
    batch_size=128,
    num_workers=8,
    pin_memory=True,
    multiprocessing_context="spawn",
)
```

The adapter detects initialized `torch.distributed` rank/world size, falling back
to `RANK` and `WORLD_SIZE`, then combines them with DataLoader worker identity.
The Rust reader assigns whole shards so ranks and workers are disjoint without
sample-level range requests. Native store/runtime state is constructed inside
each worker and never crosses `fork`.

`multiprocessing_context="spawn"` is recommended. It avoids forking a process
after PyTorch or another native library has created threads, at the cost of worker
startup time. Consequently, `decode` must be a picklable module-level callable;
do not use a lambda or nested function when workers use `spawn`.

Call `source.set_epoch(epoch)` before constructing each epoch iterator to change
the deterministic shard order. With `persistent_workers=True`, mutations in the
parent dataset object do not propagate into existing worker copies; recreate the
DataLoader workers or pass an explicit cross-process epoch mechanism. Shard-level
partitioning can yield different record counts per rank, so DDP training must use
a fixed step budget, coordinated truncation/padding, or join semantics to prevent
collective mismatch. The adapter now exposes the Rust verified cache and bounded
shard prefetch, but schema decode, transforms and PyTorch collation still run in
DataLoader workers until the dedicated Rust loader replaces this bridge.
