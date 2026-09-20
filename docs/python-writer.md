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
bounded completed-shard queue
        |
        +----> uploader 0
        +----> uploader 1
        +----> uploader N
                    |
                    v
          single layout publisher
```

One packer is intentional. The measured plain and encrypted writers already
produce roughly 0.95 and 0.84 GiB/s on the development host, normally faster than
a Python producer or one S3 destination. It also makes record order, shard
boundaries, block nonces, BLAKE3 identity, and reproducibility independent of task
scheduling. Uploading completed shards overlaps network I/O with construction of
the next shard and is the first useful concurrency boundary.

Every queue is bounded by both records and bytes. `write_batch`, rather than a
per-record call, amortizes PyO3 and GIL transitions. Once Rust owns the batch, the
call releases the GIL while waiting for channel capacity. Payloads become
Rust-owned `Bytes` before a worker can access them; workers never retain borrowed
Python memory.

## Public configuration

The native writer should separate semantic options from resource limits:

```rust
struct PythonWriterOptions {
    snapshot_id: SnapshotId,
    store_id: String,

    target_shard_bytes: u64,
    target_block_bytes: usize,
    block_codec: BlockCodec,
    zstd_level: i32,

    encryption: Option<WriterEncryption>,
    limits: PythonWriterLimits,
}

struct WriterEncryption {
    algorithm: EncryptionAlgorithm,
    key_id: String,
    key: EncryptionKey,
}

struct PythonWriterLimits {
    ingest_max_records: usize,
    ingest_max_bytes: usize,
    completed_shard_queue_bytes: u64,
    upload_concurrency: usize,
    staging_max_bytes: u64,
}
```

`algorithm` is explicit even while AES-256-GCM is the only accepted value. The
layout persists `algorithm` and `key_id`; key bytes remain process-local and are
zeroized. Adding another algorithm later must be an explicit format capability,
not an environment-dependent automatic choice.

The Python surface should mirror these fields and require encryption values as a
group:

```python
DatasetWriter(
    ...,
    encryption_algorithm="aes-256-gcm",
    encryption_key_id="kms://training/key-7",
    encryption_key=key_bytes,
)
```

## Threads and processes

Multiple Python threads may enqueue batches through cloneable producer handles.
A monotonically assigned sequence preserves call admission order, and exactly one
handle owns `finish()`. In practice batching matters more than Python thread count
when preprocessing is light.

An active writer is process-affine and must reject use after `fork`. Tokio
runtimes, OpenDAL clients, channels, staging descriptors, and multipart state are
never shared between processes. The MVP supports one writer process. Future
multi-process materialization should give each process a deterministic input
partition and independent shard ownership; workers publish fragment manifests and
only a coordinator validates and publishes the final layout.

## Commit and failure behavior

The packer writes a local temporary shard, computes its exact `ShardId`, and then
places it on the completed-shard queue. Upload workers conditionally publish the
content-addressed final key and verify streamed or pre-existing objects. The local
staging budget applies backpressure before disk exhaustion.

`finish()` closes admission, drains the packer and uploaders, validates aggregate
record count and content fingerprint, then publishes the immutable layout. Any
worker failure closes admission and is returned consistently by later writes and
`finish()`. A layout is never published from a partial build; abandoned local or
remote staging objects remain eligible for orphan cleanup.

Direct streaming to the final S3 key is impossible with a content-addressed name
because the full-byte BLAKE3 is unknown until EOF. A later diskless mode may upload
to a build-scoped temporary key, server-side copy to the final key, and delete the
temporary object after validation.
