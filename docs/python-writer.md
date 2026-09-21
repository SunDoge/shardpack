# Python writer design

Python is expected to be the primary ingestion surface. The first writer should
therefore optimize a single Python producer process rather than begin with a
distributed materialization protocol. Schema-specific Python code produces complete
encoded records; Rust owns packing, compression, encryption, staging, upload,
validation, and publication.

## Execution model

```text
Python write_batch()
        |
        v
bounded record + byte channel
        |
        v
one deterministic Rust shard packer
        |
        v
stage one completed shard
        |
        v
OpenDAL immutable upload
        |
        v
single layout publisher
```

One packer is intentional. The measured plain and encrypted writers already
produce roughly 0.95 and 0.84 GiB/s on the development host, normally faster than
a Python producer or one S3 destination. It also makes record order, shard
boundaries, block nonces, BLAKE3 identity, and reproducibility independent of task
scheduling. OpenDAL may perform concurrent multipart transfer within one object,
but the MVP does not yet overlap uploading one completed shard with packing the
next. A bounded completed-shard upload queue is the next useful concurrency
boundary.

The first public ingestion API is a packed CPU-buffer batch:

```python
writer.write_batch(payload_data, offsets, sample_ids, flags=None)
```

`payload_data` is contiguous `uint8[N]`, `offsets` is `uint64[B + 1]`, sample IDs
are `uint8[B, 16]` (or the equivalent flat buffer), and optional flags are
`uint32[B]`. NumPy arrays and other compatible Python buffer exporters work
without a NumPy ABI dependency. Object arrays and lists of per-record arrays are
deliberately unsupported.

`write_batch`, rather than a per-record call, amortizes PyO3 and GIL transitions.
Python memory is copied once into one Rust-owned arena; records are zero-copy
`Bytes` slices of that arena until the packer copies them into the final block.
The call releases the GIL while waiting for bounded channel space, and workers
never retain borrowed Python memory. `queue_batches`, `max_batch_bytes`, and
`max_batch_records` bound ingestion memory; the active block adds a small amount
beyond it.

## Public configuration

The native writer should separate semantic options from resource limits:

```rust
struct PythonWriterOptions {
    snapshot_id: SnapshotId,
    store_id: String,

    target_shard_bytes: u64,
    max_shard_bytes: Option<u64>,
    target_block_bytes: usize,
    block_codec: BlockCodec,
    zstd_level: i32,

    encryption: Option<WriterEncryption>,
    limits: PythonWriterLimits,
}

struct WriterEncryption {
    key_id: String,
    key: EncryptionKey,
}

struct PythonWriterLimits {
    queue_batches: usize,
    max_batch_records: usize,
    max_batch_bytes: usize,
}
```

The fields above describe the current public surface. Future upload pipelining
will add a completed-shard byte budget, upload concurrency and a total staging
budget together so concurrency cannot exhaust local disk.

`max_shard_bytes` is optional. When set, publication rejects a completed
multi-record shard above the hard ceiling; a single-record jumbo shard remains
legal. The Cloudflare/B2 profile uses a 384 MiB soft target and 480 MiB ceiling.

The layout persists the encryption algorithm and `key_id`; key bytes remain
process-local and are zeroized. AES-256-GCM is currently the only writer
algorithm, so the Python constructor does not expose a redundant algorithm
selector. Adding another algorithm later must be an explicit format capability,
not an environment-dependent automatic choice.

The Python surface should mirror these fields and require encryption values as a
group:

```python
DatasetWriter(..., encryption_key_id="kms://training/key-7", encryption_key=key_bytes)
```

## Threads and processes

Multiple Python threads may call `write_batch`; an admission lock preserves call
order and prevents `finish()` from racing an accepted batch. Exactly one native
writer owns the worker and final publication. In practice batching matters more
than Python thread count when preprocessing is light.

An active writer is process-affine and must reject use after `fork`. Tokio
runtimes, OpenDAL clients, channels, staging descriptors, and multipart state are
never shared between processes. The MVP supports one writer process. Future
multi-process materialization should give each process a deterministic input
partition and independent shard ownership; workers publish fragment manifests and
only a coordinator validates and publishes the final layout.

The buffer API requires CPython 3.11 because PyO3 exposes the stable-ABI buffer
interface from that version onward. Wheels therefore target `abi3-py311`.

## Commit and failure behavior

The packer writes a local temporary shard, computes its exact `ShardId`, then
conditionally publishes the content-addressed final key and verifies streamed or
pre-existing objects before packing the next shard. Input-channel limits provide
memory backpressure; callers must currently provision staging space for one
maximum-size shard. The planned upload pipeline must add an explicit aggregate
staging budget before allowing multiple completed shards in flight.

`finish()` closes admission, joins the worker, validates aggregate
record count and content fingerprint, then publishes the immutable layout. Any
worker failure closes admission and is returned consistently by later writes and
`finish()`. A layout is never published from a partial build; abandoned local or
remote staging objects remain eligible for orphan cleanup.

Direct streaming to the final S3 key is impossible with a content-addressed name
because the full-byte BLAKE3 is unknown until EOF. A later diskless mode may upload
to a build-scoped temporary key, server-side copy to the final key, and delete the
temporary object after validation.
